"""
Deep Research Module - Agentic Research for Real Estate Districts

Uses Google Custom Search + Gemini to generate comprehensive investment reports
for each district based on 21 predefined queries.
"""
import os
import time
import requests
from typing import List, Dict, Optional

# Rate limiting for Google CSE
_last_search_time = 0
SEARCH_RATE_LIMIT = 1.1  # seconds between requests

# Google CSE Configuration
GOOGLE_CSE_ID = os.getenv('GOOGLE_CSE_ID', '043339718c8054129')
GOOGLE_API_KEY = os.getenv('GOOGLE_API_KEY', 'AIzaSyC7IGitg94xGP_ojTEbcnW9sHa24C1tFNM')

# 21 Fixed Query Templates (user-defined)
RESEARCH_QUERIES = [
    "precio vivienda €/m² {zona}",
    "precio alquiler €/m² {zona} idealista",
    "rentabilidad alquiler bruta {zona} (venta vs alquiler)",
    "evolución precio vivienda {zona} últimos 4 años",
    "evolución precio alquiler {zona} últimos 4 años",
    "demanda alquiler {zona} escasez oferta vivienda",
    "okupación {zona} tasa / noticias / mapas",
    "seguridad delincuencia {zona} estadísticas / mapa",
    "plan urbanístico {zona} transporte metro cercanías bus nuevas estaciones",
    "zona tensionada vivienda {zona} declaración oficial alquiler",
    "evolución tasa paro {zona}",
    "evolución tasa de paro {zona}",
    "evolución población empadronada {zona}",
    "renta media hogares {zona} atlas renta INE",
    "hogares unipersonales tamaño medio hogar {zona} INE",
    "edad media {zona} pirámide población",
    "licencias de obra nueva {zona} ayuntamiento",
    "planeamiento urbanístico {zona} modificación PGOU / PGO",
    "inversiones públicas {zona} presupuesto municipal obras",
    "mapa ruido {zona} ayuntamiento",
    "mapa inundabilidad {zona} SNCZI",
]


def google_search(query: str, num_results: int = 5) -> List[Dict]:
    """
    Execute a Google Custom Search query.
    
    Returns list of results with title, link, and snippet.
    Rate-limited to 1 request per second.
    """
    global _last_search_time
    
    # Rate limiting
    elapsed = time.time() - _last_search_time
    if elapsed < SEARCH_RATE_LIMIT:
        time.sleep(SEARCH_RATE_LIMIT - elapsed)
    
    url = "https://www.googleapis.com/customsearch/v1"
    params = {
        'key': GOOGLE_API_KEY,
        'cx': GOOGLE_CSE_ID,
        'q': query,
        'num': min(num_results, 10),  # Max 10 per request
        'lr': 'lang_es',  # Spanish results
        'gl': 'es',       # Spain geo
    }
    
    try:
        resp = requests.get(url, params=params, timeout=15)
        _last_search_time = time.time()
        
        if resp.status_code == 200:
            data = resp.json()
            items = data.get('items', [])
            return [
                {
                    'title': item.get('title', ''),
                    'link': item.get('link', ''),
                    'snippet': item.get('snippet', '')
                }
                for item in items
            ]
        else:
            print(f"[WARN] Google Search error {resp.status_code}: {resp.text[:200]}")
            return []
            
    except Exception as e:
        print(f"[WARN] Google Search failed: {e}")
        return []


def execute_research_queries(zona: str, progress_callback=None) -> Dict[str, List[Dict]]:
    """
    Execute all 21 research queries for a given zone/district.
    
    Args:
        zona: District name
        progress_callback: Optional callback(query_num, total, query_text)
        
    Returns:
        Dictionary mapping query template -> list of search results
    """
    results = {}
    total = len(RESEARCH_QUERIES)
    
    for i, template in enumerate(RESEARCH_QUERIES, 1):
        query = template.format(zona=zona)
        
        if progress_callback:
            progress_callback(i, total, query)
        
        search_results = google_search(query)
        results[template] = search_results
        
        print(f"  [{i}/{total}] {query[:50]}... -> {len(search_results)} results")
    
    return results


def synthesize_report(zona: str, search_results: Dict[str, List[Dict]], 
                      metrics: Optional[Dict] = None) -> str:
    """
    Use Gemini to synthesize a comprehensive investment report.
    
    Args:
        zona: District name
        search_results: Output from execute_research_queries()
        metrics: Optional dict with calculated metrics (yield, price vs market, etc.)
        
    Returns:
        Markdown-formatted investment report in Spanish
    """
    try:
        import google.generativeai as genai
    except ImportError:
        return f"Error: google-generativeai no instalado. Ejecutar: pip install google-generativeai"
    
    # Configure Gemini
    genai.configure(api_key=GOOGLE_API_KEY)
    model = genai.GenerativeModel('gemini-2.0-flash')
    
    # Build context from search results
    context_parts = []
    for template, results in search_results.items():
        if results:
            snippets = "\n".join([f"- {r['snippet']}" for r in results[:3]])
            context_parts.append(f"**{template.format(zona=zona)}**:\n{snippets}")
    
    search_context = "\n\n".join(context_parts)
    
    # Build metrics context if available
    metrics_context = ""
    if metrics:
        metrics_lines = []
        if 'yield_bruto' in metrics:
            metrics_lines.append(f"- Rentabilidad bruta: {metrics['yield_bruto']:.1%}")
        if 'yield_neto' in metrics:
            metrics_lines.append(f"- Rentabilidad neta: {metrics['yield_neto']:.1%}")
        if 'descuento_vs_mercado_pct' in metrics:
            metrics_lines.append(f"- Descuento vs mercado: {metrics['descuento_vs_mercado_pct']:.1f}%")
        if 'n_oportunidades' in metrics:
            metrics_lines.append(f"- Nº oportunidades detectadas: {metrics['n_oportunidades']}")
        metrics_context = "\n".join(metrics_lines)
    
    # Build prompt
    prompt = f"""Eres un analista de inversión inmobiliaria experto y riguroso.

A continuación tienes datos recogidos de búsquedas web sobre el distrito "{zona}".
Sintetiza un informe de inversión EN ESPAÑOL con el siguiente formato.

**REGLAS CRÍTICAS ANTI-ALUCINACIÓN (MUY IMPORTANTE):**
1. **USA SOLO LOS DATOS PROPORCIONADOS ABAJO** en la sección "DATOS DE BÚSQUEDA WEB".
2. **NO INVENTES DATOS**. Si los resultados de búsqueda no contienen información específica sobre precios, delitos o planes urbanísticos, DEBES escribir: "No hay información suficiente en los resultados de búsqueda".
3. NO SUPONGAS tendencias si no hay datos históricos mostrados explícitamente en los fragmentos.
4. Si la información es escasa, admítelo honestamente. Es mejor decir "No hay datos" que dar un dato falso o inventado.
5. NO uses conocimiento general externo para rellenar datos numéricos (precios, rentabilidades, estadísticas).

---

## {zona} - Análisis de Inversión

### 📊 Resumen Ejecutivo
[2-3 líneas sobre si es INVERTIR / ESPERAR / EVITAR y por qué, basándote SOLO en lo encontrado]

### 💰 Precios y Rentabilidad
[Datos sobre precios venta/alquiler y rentabilidad bruta encontrados en los resultados]

### 📈 Tendencias
[Evolución de precios últimos años, proyección - SOLO SI HAY DATOS]

### 🚇 Infraestructura y Transporte
[Planes urbanísticos, nuevas líneas, accesibilidad - SOLO SI HAY DATOS]

### 👥 Demografía y Demanda
[Población, renta media, demanda alquiler - SOLO SI HAY DATOS]

### ⚠️ Riesgos
[Okupación, zona tensionada, ruido, inundabilidad - SOLO SI HAY DATOS]

### ✅ Conclusión
[1-2 frases finales con recomendación clara basada estrictamente en la evidencia encontrada]

---

**DATOS DE BÚSQUEDA WEB:**
{search_context}

**MÉTRICAS CALCULADAS:**
{metrics_context if metrics_context else "No disponibles"}

Genera el informe siguiendo ESTRICTAMENTE las reglas anti-alucinación.
Usa emojis apropiados para cada sección. Sé conciso pero informativo.
"""
    
    try:
        response = model.generate_content(prompt)
        return response.text
    except Exception as e:
        return f"Error generando informe: {e}"


def deep_research_distrito(zona: str, metrics: Optional[Dict] = None, 
                           progress_callback=None) -> str:
    """
    Main entry point: Execute full deep research for a district.
    
    Args:
        zona: District name
        metrics: Optional metrics dict from analysis
        progress_callback: Optional callback for progress updates
        
    Returns:
        Complete investment report in markdown format
    """
    print(f"\n{'='*60}")
    print(f"DEEP RESEARCH: {zona}")
    print(f"{'='*60}")
    
    # Phase 1: Execute all 21 queries
    print("\n[1/2] Ejecutando búsquedas...")
    search_results = execute_research_queries(zona, progress_callback)
    
    # Count total results
    total_results = sum(len(r) for r in search_results.values())
    print(f"\n  Total resultados: {total_results}")
    
    # Phase 2: Synthesize with Gemini
    print("\n[2/2] Sintetizando informe con Gemini...")
    report = synthesize_report(zona, search_results, metrics)
    
    print(f"\n{'='*60}")
    print("DEEP RESEARCH COMPLETADO")
    print(f"{'='*60}")
    
    return report


# Test function
if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python deep_research.py <distrito>")
        print("Example: python deep_research.py Arroyomolinos")
        sys.exit(1)
    
    distrito = " ".join(sys.argv[1:])
    report = deep_research_distrito(distrito)
    
    print("\n" + "="*60)
    print("INFORME GENERADO:")
    print("="*60)
    print(report)
