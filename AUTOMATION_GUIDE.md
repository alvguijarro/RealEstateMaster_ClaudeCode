# Guía de Automatización: Ciclo Programado

Este sistema permite ejecutar un raspado completo de todas las provincias (Alquiler y Venta) de forma secuencial y automática cada 14 días.

## Funcionamiento
El orquestador (`scripts/automated_cycle.py`):
1.  **Carga las URLs**: Lee automáticamente `idealista_urls_alquiler.md` y `idealista_urls_venta.md`.
2.  **Gestiona el orden**: Primero realiza todas las provincias en Alquiler, y luego todas en Venta.
3.  **Persistencia**: Si se interrumpe (cierre de ventana, reinicio de PC), al reiniciar el script continuará exactamente por donde iba gracias a `cycle_state.json`.
4.  **Autonomía**: Enciende el servidor de scraping si es necesario y gestiona las rotaciones de identidad y VPN automáticamente.
5.  **Reporte de Eficacia**: Al finalizar cada provincia o detenerse, el log mostrará un resumen detallado de cuántas propiedades ha extraído cada perfil de navegador (Firefox, WebKit, etc.) y su porcentaje de éxito respecto al total.

## Instrucciones para Programarlo en Windows

Para que el proceso sea 100% autónomo, debes añadirlo al **Programador de Tareas** de Windows:

1.  Presiona `Win + R`, escribe `taskschd.msc` y pulsa Enter.
2.  En el panel derecho, haz clic en **"Crear tarea básica..."**.
3.  **Nombre**: `RealEstateMaster_AutoCycle`.
4.  **Desencadenador**: Selecciona **"Diariamente"** (el propio script decidirá si le toca Ejecutar o Esperar según los últimos 14 días).
5.  **Acción**: Selecciona **"Iniciar un programa"**.
6.  **Programa o script**: Haz clic en "Examinar" y busca el archivo `LAUNCH_AUTO_CYCLE.bat` en la carpeta raíz del proyecto.
7.  **Iniciar en (opcional)**: Introduce la ruta completa de la carpeta raíz del proyecto (ej: `C:\Users\tu_usuario\RealEstateMaster`).
8.  **Finalizar**: Marca la casilla "Abrir el diálogo Propiedades..." y en la pestaña **"Configuración"**, asegúrate de que esté marcado "Si la tarea no se detiene, terminarla en: 3 días" (para evitar solapamientos raros).

## Control Manual
*   Puedes lanzar el ciclo en cualquier momento ejecutando el archivo `LAUNCH_AUTO_CYCLE.bat`.
*   Para ver el estado actual o resetear un ciclo, puedes editar `scraper/cycle_state.json`.
