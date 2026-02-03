"""
Deep Research Module - Agentic Research for Real Estate Districts

Uses Gemini Grounding (Search Tool) via the new `google-genai` library to generate 
comprehensive investment reports for each district based on predefined research topics.
"""
import os
from typing import Dict, Optional
# Import the new client library for Gemini API
from google import genai
from google.genai import types

# Load Google API Key
GOOGLE_API_KEY = os.getenv('GOOGLE_API_KEY')

# 21 Fixed Research Topics
RESEARCH_TOPICS = [
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


def deep_research_distrito(zona: str, metrics: Optional[Dict] = None, 
                           progress_callback=None) -> str:
    """
    Main entry point: Execute full deep research for a district using Gemini Grounding.
    
    Args:
        zona: District name
        metrics: Optional metrics dict from analysis
        progress_callback: Optional callback (compatibility mode, not fully used with single-call)
        
    Returns:
        Complete investment report in markdown format
    """
    if not GOOGLE_API_KEY:
        return "Error: GOOGLE_API_KEY no configurada en el entorno."

    print(f"\n{'='*60}")
    print(f"DEEP RESEARCH (GEMINI GROUNDING): {zona}")
    print(f"{'='*60}")
    
    # Initialize New Client
    client = genai.Client(api_key=GOOGLE_API_KEY)
    
    # Build metrics context if available
    metrics_context = ""
    if metrics:
        metrics_lines = []
        if 'yield_bruto' in metrics:
            metrics_lines.append(f"- Rentabilidad bruta calculada: {metrics['yield_bruto']:.1%}")
        if 'yield_neto' in metrics:
            metrics_lines.append(f"- Rentabilidad neta calculada: {metrics['yield_neto']:.1%}")
        if 'descuento_vs_mercado_pct' in metrics:
            metrics_lines.append(f"- Descuento vs mercado: {metrics['descuento_vs_mercado_pct']:.1f}%")
        if 'n_oportunidades' in metrics:
            metrics_lines.append(f"- Nº oportunidades detectadas en nuestra base: {metrics['n_oportunidades']}")
        metrics_context = "\n".join(metrics_lines)
    
    # Prepare Prompt
    topics_list = "\n".join([f"- {t.format(zona=zona)}" for t in RESEARCH_TOPICS])
    
    prompt = f"""Eres un analista de inversión inmobiliaria experto y riguroso.
Estás encargado de realizar un informe de "Deep Research" sobre el distrito o zona: "{zona}".

Tu tarea es investigar a fondo utilizando tus herramientas de búsqueda y sintetizar un informe detallado.
Debes buscar información específica sobre los siguientes temas clave:

{topics_list}

---

**MÉTRICAS INTERNAS DE NUESTRA BASE DE DATOS (NO BUSCAR, USAR COMO REFERENCIA):**
{metrics_context if metrics_context else "No disponibles"}

---

Genera un informe FINAL EN ESPAÑOL con el siguiente formato Markdown.

**REGLAS:**
1. **FUNDAMENTA TODO**: Usa la búsqueda de Google para encontrar datos reales recientes (precios, noticias, planes urbanísticos).
2. **CITA FUENTES**: El sistema añadirá citas automáticamente, pero asegúrate de basar tus afirmaciones en los resultados de búsqueda.
3. **SÉ CRÍTICO**: Si hay datos contradictorios (ej: bajada de precios en una fuente, subida en otra), menciónalo.
4. **NO INVENTES**: Si no encuentras datos sobre algo específico (ej: inundabilidad), indícalo claramente ("No se hallaron datos específicos").

---
FORMATO DEL INFORME:

## {zona} - Análisis de Inversión

### 📊 Resumen Ejecutivo
[Recomendación clara: INVERTIR / ESPERAR / EVITAR y por qué]

### 💰 Precios y Mercado
[Análisis de precios venta/alquiler, tendencias recientes y rentabilidades de mercado vs nuestras métricas]

### 🚇 Infraestructura y Urbanismo
[Transporte, obras públicas, planeamiento y proyectos futuros]

### 👥 Demografía y Social
[Perfil del habitante, seguridad, ocupación, demanda de alquiler]

### ⚠️ Riesgos y Oportunidades
[Riesgos específicos (okupación, zonas tensionadas) vs Catalizadores de revalorización]

### ✅ Conclusión Final
[Veredicto inversor fundamentado]
"""

    print("\n[1/1] Ejecutando investigación y síntesis con Gemini Grounding...")
    
    if progress_callback:
        progress_callback(50, 100, "Investigando y redactando con Gemini...")

    try:
        # Reverted to gemini-2.0-flash as deep-research-pro requires Interactions API
        # Use new generate_content call structure with google_search tool
        response = client.models.generate_content(
            model='gemini-2.0-flash',
            contents=prompt,
            config=types.GenerateContentConfig(
                tools=[types.Tool(
                    google_search=types.GoogleSearchRetrieval
                )]
            )
        )
        
        # Check if response has valid text
        if response.text:
            print("\n  -> Informe generado correctamente.")
            return response.text
        else:
            return "Error: Gemini no devolvió texto (posible bloqueo de seguridad)."
            
    except Exception as e:
        print(f"\n[ERROR] Fallo en Deep Research: {e}")
        return f"Error generando informe: {e}"


# Test function
if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python deep_research.py <distrito>")
        sys.exit(1)
    
    distrito = " ".join(sys.argv[1:])
    report = deep_research_distrito(distrito)
    
    print("\n" + "="*60)
    print("INFORME GENERADO:")
    print("="*60)
    print(report)
