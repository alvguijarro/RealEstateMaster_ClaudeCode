import sys
import os

# Add current directory to sys.path for embedded Python compatibility
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import glob
import json
import threading
import pandas as pd
from flask import Flask, render_template, jsonify, request, send_from_directory
from io import StringIO
import analysis  # Import the analysis module
import webbrowser
import time
import google.generativeai as genai
from pathlib import Path

app = Flask(__name__, static_folder='static', template_folder='templates')

# Allow embedding in iframes and add CORS headers for polling
@app.after_request
def after_request(response):
    response.headers.pop('X-Frame-Options', None)
    # Add CORS headers for cross-origin polling from dashboard
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Methods'] = 'GET, POST, OPTIONS'
    response.headers['Access-Control-Allow-Headers'] = 'Content-Type'
    return response

@app.route('/health', methods=['GET', 'OPTIONS'])
def health_check():
    """Simple health check endpoint for service readiness polling."""
    return jsonify({'status': 'ok'})

# Global state
ANALYSIS_STATUS = 'idle' # idle, running, done, error
LOG_BUFFER = []
ANALYSIS_THREAD = None
LAST_ERROR = None
CURRENT_OUTPUT_FILE = 'Salidas/analisis_resultado.xlsx'

# =============================================================================
# SYSTEM PROMPT FOR GEMINI LLM REPORTS
# =============================================================================
SYSTEM_PROMPT = """
Eres un analista experto en inversiones inmobiliarias. Tu objetivo es crear informes de ALTO IMPACTO VISUAL para inversores, sobre un DISTRITO específico.

ESTILO REQUERIDO: "THE VISUAL ANALYST"
- Prioridad absoluta: LEGIBILIDAD y SÍNTESIS.
- Usa EMOJIS como iconos para cada sección y punto clave.
- Usa TABLAS Markdown para datos.
- Usa BLOCKQUOTES (>) para el resumen/perfil.
- No uses parrafadas largas. Ve al grano.

ESTRUCTURA EXACTA DE SALIDA:

# 🇪🇸 **Informe de Inversión: [NOMBRE DISTRITO]** (Ciudad/Municipio)

### 📊 **Indicadores Clave**
| Métrica | Valor (Estimado) | Evaluación (🟢/🟡/🔴) |
| :--- | :--- | :--- |
| **Rentabilidad Bruta** | [X.X]% - [Y.Y]% | [Emoji semáforo] [Texto breve] |
| **Riesgo Vacancia** | [Bajo/Medio/Alto] | [Emoji semáforo] [Texto breve] |
| **Tendencia Precios** | [Alcista/Estable/Bajista] | [Emoji semáforo] [Texto breve] |

### 🏘️ **Perfil del Distrito**
> **"[Frase gancho o apodo del barrio]"**: [Resumen de 2-3 líneas sobre la "vibra" del barrio, gentrificación, perfil de población y por qué es interesante].

#### 🎯 **Oportunidades Destacadas**
*   **[Nombre Zona/Barrio A]**: [Breve descripción de por qué interesa: precios bajos, alta demanda, etc.].
*   **[Nombre Zona/Barrio B]**: [Breve descripción].
*   **[Nombre Zona/Barrio C]**: [Breve descripción].

### ⚠️ **Riesgos y Consideraciones**
*   🔹 **[Riesgo A]**: [Explicación].
*   🔹 **[Riesgo B]**: [Explicación].
*   🔹 **[Riesgo C]**: [Explicación].

### 💡 **Veredicto Final**
[Conclusión de 1 frase sobre si COMPRAR o ESPERAR, y para qué perfil de inversor: Cashflow vs Revalorización].

---
*Nota: Datos estimados basados en conocimiento de mercado general. Verificar fuentes locales.*
"""

# Custom stdout capturer
class StreamCapture:
    def __init__(self):
        self.terminal = sys.stdout

    def write(self, message):
        self.terminal.write(message)
        # Append to global log buffer
        # Only append non-empty lines to keep UI clean? No, whitespace matters.
        if message:
            LOG_BUFFER.append(message)

    def flush(self):
        self.terminal.flush()

# Redirect stdout globally (careful in production, okay for local single-user)
# We will only swap it during the thread execution context or permanently?
# Permanently is easier.
sys.stdout = StreamCapture()


@app.route('/')
def index():
    return render_template('index.html', cache_bust=int(time.time()))


@app.route('/list-files')
def list_files():
    # Find xlsx files in the scraper's salidas output directory
    salidas_dir = Path(__file__).parent.parent / "scraper" / "salidas"
    files = []
    if salidas_dir.exists():
        for f in salidas_dir.glob("*.xlsx"):
            # simple heuristic for type
            f_lower = f.name.lower()
            ftype = 'unknown'
            if 'venta' in f_lower or 'sale' in f_lower: ftype = 'venta'
            elif 'alquiler' in f_lower or 'rent' in f_lower: ftype = 'alquiler'
            
            files.append({'filename': f.name, 'type': ftype})
    return jsonify(files)


@app.route('/analyze', methods=['POST'])
def start_analysis():
    global ANALYSIS_STATUS, LOG_BUFFER, ANALYSIS_THREAD, LAST_ERROR
    
    if ANALYSIS_STATUS == 'running':
        return jsonify({'error': 'Analysis already running'}), 400
    
    data = request.json
    venta_file_name = data.get('venta_file')
    alquiler_file_name = data.get('alquiler_file')
    filters_raw = data.get('filters', {})
    
    # Prepend salidas path to file names
    salidas_dir = Path(__file__).parent.parent / "scraper" / "salidas"
    venta_file = str(salidas_dir / venta_file_name) if venta_file_name else None
    alquiler_file = str(salidas_dir / alquiler_file_name) if alquiler_file_name else None
    
    # Reset state
    ANALYSIS_STATUS = 'running'
    LOG_BUFFER.clear()
    LAST_ERROR = None
    
    # Map filters
    # JS sends IDs, we map to strings analysis.py expects
    mapped_filters = {
        'active': True,
        'estado': [],
        'include_especial': filters_raw.get('include_especial', []),
        'ascensor': filters_raw.get('ascensor', []),
        'garaje': filters_raw.get('garaje', []),
        'terraza': filters_raw.get('terraza', []),
        'altura': [],
        'tipo': [],
        # New filters
        'habs': filters_raw.get('habs', []),
        'banos': filters_raw.get('banos', []),
        'price_min': filters_raw.get('price_min'),
        'price_max': filters_raw.get('price_max')
    }
    
    # Map Estado
    # 1: 'Obra nueva', 2: 'Segunda mano/buen estado', 3: 'A reformar'
    estado_map = {1: 'Obra nueva', 2: 'Segunda mano/buen estado', 3: 'A reformar'}
    for eid in filters_raw.get('estado', []):
        if int(eid) in estado_map:
            mapped_filters['estado'].append(estado_map[int(eid)])
            
    # Map Tipo
    # 1: 'Pisos', 2: 'Casas/Chalets'
    tipo_map = {1: 'Pisos', 2: 'Casas/Chalets'}
    for tid in filters_raw.get('tipo', []):
        if int(tid) in tipo_map:
            mapped_filters['tipo'].append(tipo_map[int(tid)])
            
    # Map Altura
    # 1: 'Bajos', 2: 'Intermedios', 3: 'Aticos'
    altura_map = {1: 'Bajos', 2: 'Intermedios', 3: 'Aticos'}
    for aid in filters_raw.get('altura', []):
        if int(aid) in altura_map:
            mapped_filters['altura'].append(altura_map[int(aid)])
            
    # Start thread
    def run_wrapper():
        global ANALYSIS_STATUS, LAST_ERROR
        try:
            # Build config
            config = analysis.DEFAULT_CONFIG.copy()
            config['venta_file'] = venta_file
            config['alquiler_file'] = alquiler_file
            config['filters'] = mapped_filters
            
            # Force cache clear for a fresh run since parameters changed?
            # Or trust cache logic? 
            # If user changes filters, we MUST rerun 'clean'. Analysis.py logic:
            # phase_clean checks checkpoint. Checkpoint doesn't key on filters.
            # So we MUST force cleaning.
            # We can use `resume_from='clean'` logic or just force clean.
            # Let's forcefully clear cache if we want to ensure filters apply.
            # Or better calls: checking code... phase_clean loads checkpoint if use_cache=True.
            
            # We should probably force a re-run of phase 2.
            # But the 'force' arg in run_pipeline reruns EVERYTHING.
            # Let's use force=True to be safe.
            
            # --- DYNAMIC FILENAME GENERATION ---
            global CURRENT_OUTPUT_FILE
            import os
            
            # 1. City extraction
            city = "Desconocido"
            if venta_file:
                # Expecting 'idealista_<City>_venta.xlsx'
                base = os.path.basename(venta_file)
                parts = base.split('_')
                if len(parts) >= 2:
                    city = parts[1]
            
            # 2. Habs string
            habs_list = mapped_filters.get('habs', [])
            if not habs_list:
                habs_str = "todos-"
            else:
                habs_str = "-".join(map(str, habs_list))
                
            # 3. Banos string
            banos_list = mapped_filters.get('banos', [])
            if not banos_list:
                banos_str = "todos-"
            else:
                banos_str = "-".join(map(str, banos_list))
                
            # 4. Price string
            p_min = mapped_filters.get('price_min')
            p_max = mapped_filters.get('price_max')
            p_min_str = str(int(float(p_min))) if p_min else "0"
            p_max_str = str(int(float(p_max))) if p_max else "max"
            
            # Construct filename
            # resultado_<Ciudad>_<#habs>habs_<#baños>banos_<min-precio>_<max-precio>.xlsx
            new_filename = f"resultado_{city}_{habs_str}habs_{banos_str}banos_{p_min_str}_{p_max_str}.xlsx"
            
            print(f"Directory for output: Salidas/{new_filename}")
            config['output_file'] = f"Salidas/{new_filename}"
            CURRENT_OUTPUT_FILE = f"Salidas/{new_filename}"
            
            # Redirect stdout handled by global capture
            analysis.run_pipeline(config, force=True)
            
            ANALYSIS_STATUS = 'done'
            print("\n>>> ANALYSIS COMPLETED SUCCESSFULLY via Web UI <<<")
            
        except Exception as e:
            ANALYSIS_STATUS = 'error'
            LAST_ERROR = str(e)
            print(f"\nFATAL ERROR: {e}")
            import traceback
            traceback.print_exc()

    ANALYSIS_THREAD = threading.Thread(target=run_wrapper)
    ANALYSIS_THREAD.start()
    
    return jsonify({'status': 'started'})


@app.route('/stream')
def stream_logs():
    since = int(request.args.get('since', 0))
    new_logs = LOG_BUFFER[since:]
    
    return jsonify({
        'logs': new_logs,
        'status': ANALYSIS_STATUS,
        'error': LAST_ERROR
    })


@app.route('/api/results')
def get_results():
    """Return JSON results if available"""
    # Look for the latest JSON output file in Salidas folder
    json_files = sorted(glob.glob('Salidas/resultado_*.json'), key=os.path.getmtime, reverse=True)
    if not json_files:
        return jsonify({'error': 'No hay resultados disponibles. Ejecuta el análisis primero.'})
        
    latest_file = json_files[0]
    try:
        with open(latest_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return jsonify({'data': data, 'file': latest_file})
    except Exception as e:
        return jsonify({'error': str(e)})

@app.route('/api/generate-report', methods=['POST'])
def generate_report():
    """Generate LLM report for a district"""
    data = request.json
    user_prompt = data.get('prompt')
    
    if not user_prompt:
        return jsonify({'error': 'Prompt is required'}), 400
        
    # Hardcoded API Key as requested
    api_key = "AIzaSyC7IGitg94xGP_ojTEbcnW9sHa24C1tFNM"
    if not api_key:
        return jsonify({'error': 'GOOGLE_API_KEY not found'}), 500
        
    try:
        genai.configure(api_key=api_key)
        # Trying gemini-2.0-flash
        model = genai.GenerativeModel(
            model_name='gemini-2.0-flash',
            system_instruction=SYSTEM_PROMPT
        )
        
        response = model.generate_content(user_prompt)
        return jsonify({'report': response.text})
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/deep-research', methods=['POST'])
def deep_research():
    """Execute Deep Research for a district using Google CSE + Gemini"""
    data = request.json
    distrito = data.get('distrito')
    
    if not distrito:
        return jsonify({'error': 'distrito is required'}), 400
    
    # Optional: metrics from the analysis
    metrics = data.get('metrics', {})
    
    try:
        from deep_research import deep_research_distrito
        
        report = deep_research_distrito(distrito, metrics=metrics)
        return jsonify({
            'distrito': distrito,
            'report': report,
            'status': 'success'
        })
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


def get_results_from_excel():
    """Fallback: read from Excel if JSON not available"""
    try:
        global CURRENT_OUTPUT_FILE
        filename = CURRENT_OUTPUT_FILE
        
        if not os.path.exists(filename):
            if os.path.exists('Salidas/analisis_resultado.xlsx'):
                filename = 'Salidas/analisis_resultado.xlsx'
        
        df = pd.read_excel(filename, sheet_name='oportunidades')
        records = df.to_dict(orient='records')
        return jsonify(records)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/download-results')
def download_results():
    global CURRENT_OUTPUT_FILE
    filename = CURRENT_OUTPUT_FILE
    if not os.path.exists(filename):
        if os.path.exists('Salidas/analisis_resultado.xlsx'):
            filename = 'Salidas/analisis_resultado.xlsx'
        else:
            return "No results found", 404
    
    # Extract directory and filename for send_from_directory
    directory = os.path.dirname(filename) or '.'
    basename = os.path.basename(filename)
    return send_from_directory(directory, basename, as_attachment=True)


@app.route('/heartbeat', methods=['POST'])
def heartbeat():
    global LAST_HEARTBEAT
    LAST_HEARTBEAT = time.time()
    return jsonify({'status': 'alive'})

# Heartbeat monitor
import time
LAST_HEARTBEAT = 0
SHUTDOWN_TIMEOUT = 300  # Shutdown after 300 seconds of no activity

def monitor_heartbeat():
    global LAST_HEARTBEAT
    print("Heartbeat monitor started...")
    
    # Wait for first heartbeat to avoid immediate shutdown
    while LAST_HEARTBEAT == 0:
        time.sleep(1)
        
    while True:
        if time.time() - LAST_HEARTBEAT > SHUTDOWN_TIMEOUT:
            print(f"No heartbeat for {SHUTDOWN_TIMEOUT}s. Shutting down server...")
            os._exit(0)
        time.sleep(1)

# Start monitor thread
monitor_thread = threading.Thread(target=monitor_heartbeat, daemon=True)
monitor_thread.start()

if __name__ == '__main__':
    port = 5001
    url = f"http://127.0.0.1:{port}"
    print(f"Starting server on {url}")
    
    # Auto-open browser
    if not os.environ.get('NO_BROWSER_OPEN'):
        try:
            import webbrowser
            from threading import Timer
            def open_browser():
                webbrowser.open_new(url)
            Timer(1.5, open_browser).start()
        except:
            pass
        
    app.run(port=port, debug=False, use_reloader=False)
