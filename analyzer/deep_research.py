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
                           progress_callback=None, api_key: Optional[str] = None) -> str:
    """
    Main entry point: Execute full deep research for a district using Gemini Grounding.
    
    Args:
        zona: District name
        metrics: Optional metrics dict from analysis
        progress_callback: Optional callback (compatibility mode, not fully used with single-call)
        api_key: Optional API Key override
        
    Returns:
        Complete investment report in markdown format
    """
    # Use passed key, or env var, or global var
    key_to_use = api_key or os.getenv('GOOGLE_API_KEY') or GOOGLE_API_KEY
    
    if not key_to_use:
        return "Error: GOOGLE_API_KEY no configurada en el entorno."

    print(f"\n{'='*60}")
    print(f"DEEP RESEARCH (GEMINI GROUNDING): {zona}")
    print(f"{'='*60}")
    
    # Initialize New Client
    client = genai.Client(api_key=key_to_use)
    
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
    
    prompt = f"""Eres un analista experto en inversiones inmobiliarias. Tu objetivo es crear informes de ALTO IMPACTO VISUAL para inversores, sobre un DISTRITO específico.
    
    ESTILO REQUERIDO: "THE VISUAL ANALYST"
    - Prioridad absoluta: LEGIBILIDAD y SÍNTESIS.
    - Usa EMOJIS como iconos para cada sección y punto clave.
    - Usa TABLAS Markdown para datos.
    - Usa BLOCKQUOTES (>) para el resumen/perfil.
    - No uses parrafadas largas. Ve al grano.
    
    Tu tarea es investigar a fondo el distrito "{zona}" utilizando tus herramientas de búsqueda y sintetizar un informe detallado.
    Debes buscar información específica y actualizada sobre los siguientes temas clave:
    
    {topics_list}
    
    ---
    
    **MÉTRICAS INTERNAS DE NUESTRA BASE DE DATOS (NO BUSCAR, USAR COMO REFERENCIA):**
    {metrics_context if metrics_context else "No disponibles"}
    
    ---
    
    Genera un informe FINAL EN ESPAÑOL con el siguiente formato Markdown.
    
    **REGLAS CRÍTICAS DE CONTENIDO:**
    1. **PROHIBIDO HACER RECOMENDACIONES DE INVERSIÓN**: NUNCA digas "Comprar", "Esperar", "Vender" o "Recomiendo invertir". Tu trabajo es SÓLO exponer los HECHOS y DATOS. Deja que el inversor decida.
    2. **CITA FUENTES SIEMPRE**: Cada dato numérico o afirmación debe tener su fuente enlazada. El sistema añadirá citas automáticamente.
    3. **FUNDAMENTA TODO**: Usa la búsqueda de Google para encontrar datos reales recientes (precios, noticias, planes urbanísticos).
    4. **SÉ CRÍTICO**: Si hay datos contradictorios, menciónalo.
    5. **NO INVENTES**: Si no hay datos, indícalo.
    
    ---
    ESTRUCTURA EXACTA DE SALIDA:
    
    # 🇪🇸 **Informe de Inversión: {zona}**
    
    ### 📊 **Indicadores Clave**
    | Métrica | Valor (Estimado) | Evaluación (🟢/🟡/🔴) |
    | :--- | :--- | :--- |
    | **Precio Venta** | [€/m²] | [Emoji] [Ref. mercado] |
    | **Rentabilidad Bruta** | [X.X]% | [Emoji] [Ref. mercado] |
    | **Riesgo Vacancia** | [Bajo/Medio/Alto] | [Emoji] [Motivo] |
    | **Tasa de Paro** | [X.X]% | [Emoji] [Tendencia] |
    
    ### 📝 **Resumen Ejecutivo**
    [Párrafo de síntesis sobre el estado del distrito. NO hacer recomendaciones de inversión. Solo describir la situación actual basándose en los datos encontrados: precios al alza/baja, demanda, proyectos, etc.]
    
    ### 💰 **Precios y Mercado**
    *   **Precio vivienda**: [Datos precio m2, evolución anual, comparación].
    *   **Precio alquiler**: [Datos precio m2, evolución, demanda].
    *   **Rentabilidad**: [Estimación yield bruto/neto basado en datos de mercado].
    *   **Evolución precios**: [Tendencia últimos años].
    
    ### 🚇 **Infraestructura y Urbanismo**
    *   **Transporte**: [Metro, cercanías, accesos].
    *   **Inversiones Públicas**: [Presupuestos, obras en marcha].
    *   **Planeamiento**: [PGOU, nuevos desarrollos].
    *   **Mapas de Ruido/Inundabilidad**: [Datos si existen].
    
    ### 👥 **Demografía y Social**
    *   **Demografía**: [Población, edad media, crecimiento].
    *   **Tasa de Paro**: [Datos empleo/desempleo].
    *   **Renta Media**: [Nivel socioeconómico].
    *   **Seguridad y Okupación**: [Datos reales o percepción, citando fuentes].
    
    ### ⚠️ **Riesgos y Oportunidades**
    *   🔹 **Riesgos**: [Listado de riesgos objetivos: paro, regulación, falta de oferta, etc.].
    *   🔹 **Oportunidades**: [Factores positivos: revalorización, alta demanda alquiler, mejoras urbanas].
    
    ### ✅ **Conclusión Final**
    [Síntesis de los factores principales encontrados. RECUERDA: NO dar recomendación de inversión explícita. Solo resumir si los fundamentales son sólidos o débiles.]
    """

    print("\n[1/1] Ejecutando investigación y síntesis con Gemini Grounding...")
    
    if progress_callback:
        progress_callback(50, 100, "Investigando y redactando con Gemini...")

    try:
        # Use new generate_content call structure with google_search tool
        # Updated to gemini-3-flash-preview as requested
        response = client.models.generate_content(
            model='gemini-3-flash-preview',
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
