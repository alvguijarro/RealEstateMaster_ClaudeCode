# Análisis de Inversión Inmobiliaria

Análisis automatizado para detectar oportunidades de inversión comparando precios de VENTA vs ALQUILER.

## Inicio Rápido

### Opción 1: Doble-click (Recomendado)

1. Coloca tus archivos Excel en esta carpeta:
   - Un archivo con `venta` en el nombre (ej: `datos_venta.xlsx`)
   - Un archivo con `alquiler` en el nombre (ej: `datos_alquiler.xlsx`)

2. **Doble-click en `run_analysis.bat`**

3. El resultado se abrirá automáticamente: `analisis_resultado.xlsx`

> **Nota:** Si no tienes Python instalado, el script te guiará para instalarlo.

### Opción 2: Línea de comandos

```bash
python analysis.py
python analysis.py --venta mi_venta.xlsx --alquiler mi_alquiler.xlsx
python analysis.py --resume-from market   # Reanudar desde una fase
python analysis.py --force                # Ignorar caché
python analysis.py --enrich --api-key "XYZ" # Análisis cualitativo con Gemini
```

### Análisis Cualitativo con Gemini (LLM)

Puedes enriquecer el análisis con la IA de Google Gemini para los distritos más prometedores:
1.  Obtén una [API Key de Google AI Studio](https://aistudio.google.com/).
2.  Configúrala como variable de entorno `GOOGLE_API_KEY` o pásala con `--api-key`.
3.  Ejecuta con la opción `--enrich`.

Esto añadirá una columna "Analisis IA" en la pestaña `distritos_resumen` del Excel, con una valoración cualitativa sobre la demanda, perfil de inquilino y riesgos.

## Requisitos

- **Python 3.9+** (se instala una vez, gratis desde python.org)
- Las dependencias se instalan automáticamente al ejecutar `run_analysis.bat`

## Archivos de Entrada

El script busca automáticamente archivos con estas palabras:
- **VENTA:** `venta`, `sale`
- **ALQUILER:** `alquiler`, `rent`

## Salida

El archivo `analisis_resultado.xlsx` contiene:
- `zonas_resumen` - Estadísticas por distrito
- `oportunidades` - Lista de propiedades bajo mercado
- `supuestos_y_parametros` - Configuración utilizada
- `log_calidad_datos` - Log de datos procesados

## Configuración

Edita `DEFAULT_CONFIG` en `analysis.py` para cambiar parámetros financieros y pesos de scoring.
