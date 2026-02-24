# Changelog

## [2.9.3] - 2026-02-23
### Added
- **Sincronización de Archivos VENTA/ALQUILER**: Implementada lógica bidireccional en Analytics Pro para emparejar automáticamente los archivos de venta y alquiler que comparten el mismo prefijo geográfico.
- Deep Research Hiperlocal: Ahora el dropdown de distritos muestra la jerarquía completa (`Distrito, Ciudad, Zona (Provincia)`).
- Corrección de "Okupados": Se ha refinado la lógica de detección para ignorar enlaces de navegación global (como "Seguro Anti Okupas"), eliminando falsos positivos masivos.
- Reparación de datos: Se ha generado una versión reparada del dataset de Almería (`idealista_Almería_venta_REPARADO.xlsx`).

### Fixed
- **Localización incorrecta en Deep Research**: Corregido bug crítico donde el informe de Deep Research mostraba la zona y ciudad incorrectas (ej. "Baix, Baix" en lugar de "Pilar de la Horadada, Alicante"). El frontend ahora obtiene la ciudad y provincia directamente de los datos del análisis (`Ciudad`, `Provincia`) en lugar de intentar adivinarlas a partir del nombre del archivo Excel.
- **Detección de provincia mejorada**: La variable `currentAnalysisProvince` ahora se extrae prioritariamente de los datos del análisis en lugar del nombre del resultado, asegurando consistencia en la integración con la calculadora de rentabilidad.
- **Bug precio x10 en Market Metrics**: Corregido error crítico en `dashboard/app.py` donde la función `clean_currency()` convertía valores float a string (ej. `129235.0` → `"129235.0"`) y luego `.replace('.','')` eliminaba el punto decimal, produciendo `"1292350"` (10 veces el valor real). Se ha reescrito el parsing para usar `float()` directamente en valores numéricos, solo aplicando el manejo de formato español como fallback para strings.
- **Sincronización de ordenación en tablas**: Unificado el comportamiento de ordenación entre las tablas "Principales oportunidades" y "Top 100 viviendas" en Analytics Pro. Ahora ambas tablas muestran resaltado naranja y flechas de dirección consistentes, siguiendo la misma lógica de orden inicial (descendente para métricas, ascendente para texto).
- **Automatización de Market Trends**: Añadido soporte para modo `--headless` en `trends_tracker.py` y creado un script wrapper (`run_tracker.bat`) para facilitar la programación de tareas automáticas en segundo plano.
- **Robustez en Market Trends**: Implementado un sistema de detección de bloqueos más avanzado, aumentado los tiempos de espera para la extracción de datos (15s) y añadido capturas de pantalla automáticas para depurar errores de detección de propiedades ("0 viviendas"). Se ha refinado la lógica para aceptar resultados de 0 viviendas cuando se confirma la presencia del mensaje "No hay anuncios", evitando reintentos innecesarios.
- **Fix verificación inicial de Idealista**: Implementada espera adaptativa de hasta 30 segundos (3 intentos de 10s) que detecta específicamente la pantalla de "Verificación del dispositivo". Esto evita que el proceso automático de Idealista sea detectado falsamente como un bloqueo en ambos scrapers (Market Trends y Provincias).
- **Fix auto-stop por 300 skips**: Corregido bug donde el flag `scraping_finished` no se verificaba en el bucle exterior de scraping, impidiendo que el auto-stop funcionara tras 300 propiedades consecutivas saltadas.
- **Fix cleanup timeout**: Aumentado el timeout del comando PowerShell de limpieza de procesos zombi de 10s a 30s para evitar errores de timeout frecuentes.
- **Double-Check Phase en Market Trends**: Añadida una fase final de doble comprobación automática para URLs que devuelven 0 propiedades sin confirmación ("No hay anuncios"). Estos casos se re-escanean con esperas más conservadoras antes de cerrar el navegador para descartar falsos positivos y actualizar los datos si es necesario.

## [Unreleased] - Market Trends Enhancements
- `scraper_wrapper.py`: **Modo Deep Scrape (NUEVO)**. Automatizada la transición en búsquedas de gran volumen. Al alcanzar el límite impuesto por Idealista (página 60), automáticamente itera inyectando 4 variaciones de ordenación para sobrepasar la barrera de las 1800 viviendas mostradas y llegar hasta las 6000 o 9000 propiedades totales de una ciudad de forma autónoma con un mismo "click".
- `scraper_wrapper.py`: **Auto-Stop por Saltos Consecutivos (NUEVO)**. Integrado un sistema de protección para el Deep Scrape. Si el scraper detecta 300 propiedades consecutivas que ya han sido scrapeadas (aprox. 10 páginas redundantes), detiene automáticamente el proceso para ahorrar ancho de banda y evitar bloqueos innecesarios.
- `scraper_wrapper.py` & `update_urls.py`: **Corrección de Bucle Infinito por CAPTCHA**. Modificado el manejador de errores de navegación (`_goto_with_retry`) que tragaba silenciosamente la excepción de timeout del CAPTCHA. Ahora se propaga correctamente como un bloqueo duro, rotando el proxy y previniendo que la siguiente fase asuma erróneamente que la propiedad "está activa, pero no en búsqueda". Además, se modificó el regex `id:` y el orden de ejecución para priorizar bloqueos absolutos ("uso indebido") frente a los iFrames sobreinyectados de DataDome.
- `server.py`: **Segmentación Inteligente de Zonas (NUEVO)**. Integrado un chequeo SQLite previo al lanzamiento de un batch. Si una provincia rebasa las 2000 propiedades registradas de base, el servidor intercepta la URL madre y la expande sobre las rutas específicas de todas sus subzonas activas para raspar con total granularidad y maximizar datos locales, evadiendo la censura volumétrica superficial del portal inmobiliario.
- `utils.py`: **DataDome Coords Solver**. Refactorizada la función `solve_datadome_2captcha` que usaba Tokens estáticos + Proxie externo (desincronizado con IP local, levantando bandera roja del WAF) a favor de un algoritmo en base a coordenadas físicas. Ahora captura el slider original dentro del iframe, extrae el bounding_box del contenedor embebido y utiliza 2Captcha Coordinates para arrastrar físicamente el elemento evitando saltos IP.
- `trends_tracker.py`: Automatizada la exportación y guardado local del histórico en CSV al finalizar el escaneo (para backup físico sin intervención del usuario).
- **Exportación Diferenciada (NUEVO)**: `trends_tracker.py` ahora genera dos archivos CSV separados (`Venta` y `Alquiler`) con formato Largo para facilitar uso directo en software estadístico.
- **Selector Jerárquico de Zonas (NUEVO)**: `index.html` (Market Trends) actualizado para tener la misma UX premium que el Scraper, permitiendo colapsar sub-zonas, búsquedas eficientes y des/seleccionar "Todas" con un solo click.
- `scraper_wrapper.py`: Añadida orden `continue` para enganchar correctamente la rotación de perfiles en caso de error crítico aislando la inicialización de Playwright.
- `trends_tracker.py`: Arreglo de bug crítico en la rotación de perfiles tras un bloqueo CAPTCHA (NameError: browser).
- `trends/app.py`: Añadido `Flask-SocketIO` y transmisión en tiempo real de logs del subproceso (stdout).
- `index.html`: Añadido un nuevo bloque de 'Log Terminal' para visibilizar progresos y errores sin tener abierta la consola.
- `index.html`: Arreglado el renderizado oculto del Log Terminal forzando `flex-shrink: 0` y retirando `height: 100%` colindante que lo colapsaba.
- `index.html`: Arreglado fallo donde el gráfico desaparecía si no había selecciones almacenadas, ahora auto-selecciona la primera provincia disponible.

## [2.9.1] - 2026-02-22
### Fixed
- **Disfunción del Botón Detener**: Solucionado un problema estructural en `scraper_wrapper.py` donde la inicialización directa de `asyncio.Event` en el hilo principal de Flask rompía las callbacks del hilo asíncrono en segundo plano (`RuntimeError cross-loop`) al presionar "Detener". Ahora el scraper responde y se detiene invariablemente.
- **Terminación de Scraping en Batch**: Corregido error crítico donde el scraper seguía extrayendo URLs después de pulsar "Detener" en modo Batch. Ahora el endpoint `/api/batch/stop` detiene explícitamente el `scraper_controller` antes de terminar el proceso orquestador, previniendo procesos huérfanos.

## [2.9.0] - 2026-02-22
### Added
- **Auto-Retry en Tiempo Real**: Implementado bucle de auto-reinicio directamente en `scraper_wrapper.py` para bloqueos de CAPTCHA detectados a mitad de sesión. Ahora el scraper guarda estado, espera 15 minutos (sin spam de logs) y reintenta automáticamente sin intervención del usuario.

### Fixed
- **Sincronización de UI y Seguridad**: 
    - Los botones "Pausar" y "Detener" ahora se desactivan instantáneamente si se pierde la conexión con el servidor de sockets.
    - Añadida función `syncStatus()` al conectarse, que recupera el estado del scraper y restaura la interfaz si hay una sesión activa tras un refresco de página o reconexión.
    - Corregido error que impedía detectar bloqueos por discrepancia de mayúsculas en el mensaje "CAPTCHA".
- **Limpieza de Logs**: Eliminado el log minuto a minuto durante las esperas de bloqueo en `scraper_wrapper.py` y `run_batch.py` para mantener la consola limpia.
- **Refactorización**: Centralizada la lógica de actualización de progreso en `app.js` para asegurar consistencia entre actualizaciones en tiempo real y sincronización inicial.

## [2.8.1] - 2026-02-21
### Fixed
- **Bug #1 — Parámetro `captchaUrl` incorrecto (causa raíz del fallo)**: La librería `2captcha-python` define el método como `datadome(self, captcha_url, ...)` en snake_case. El código llamaba `captchaUrl=...` (camelCase), que era ignorado como kwarg desconocido, enviando la tarea sin la URL del CAPTCHA y obteniendo `ERROR_BAD_PARAMETERS`. Corregido a `captcha_url=captcha_url` en `utils.py`.
- **Bug #2 — Proxy en formato incorrecto**: La librería espera `proxy={'type': 'HTTP', 'uri': '...'}` (dict), pero el código pasaba `proxy=string, proxytype=string` (dos kwargs separados), haciendo que el proxy no se adjuntara a la tarea. Corregido para pasar el dict completo.
- **Bug #3 — Cookie con scope incorrecto**: La cookie `datadome` se inyectaba con `url="https://www.idealista.com"` (host exacto). Cambiado a `domain=".idealista.com"` para que aplique a todas las subpages del dominio, igual que el comportamiento nativo de DataDome.

## [2.8.0] - 2026-02-21
### Performance
- **Inicio del scraper ~60% más rápido**: Eliminados tres cuellos de botella en `scraper_wrapper.py` que sumaban más de 50 segundos de overhead por cada inicio de provincia.
  - **`_clear_profile_locks`**: Se sustituye `os.walk` (recorría miles de archivos de caché) por búsqueda directa en carpetas conocidas (`root`, `Default`, `WebsiteData`). Reducción estimada: **25s → <0.1s**.
  - **`_cleanup_zombie_browsers`**: Se reemplaza el comando PowerShell que hacía una consulta WMI por cada proceso por una única consulta `WHERE CommandLine LIKE '%stealth_profile%'`. Reducción estimada: **25s → <2s**.
  - **Llamadas redundantes eliminadas**: `_clear_profile_locks` se llamaba dos veces consecutivas y `_cleanup_zombie_browsers` se llamaba adicionalmente desde `_kill_browser_by_channel`. Ahora cada función se ejecuta una sola vez.
- **Detección precoz de DataDome**: El loop de extracción del H1 (4 intentos × 4s = hasta 16s de espera) ahora detecta el iframe de DataDome en el primer intento y llama al solver inmediatamente en lugar de esperar el timeout completo. Reducción estimada: **16s → <1s** cuando hay CAPTCHA activo.

## [2.7.9] - 2026-02-21
### Fixed
- **Bug #1 — Solver DataDome (TypeError Silencioso)**: La librería `2captcha-python` puede devolver el token como `str` o como `dict`. El código anterior lanzaba `TypeError: string indices must be integers` al comprobar `'code' in result` sobre un string, marcando el CAPTCHA como irresoluble. Ahora se normaliza el tipo de respuesta antes de extraer el token, afectando a `solve_datadome_2captcha` y `solve_geetest_2captcha` en `utils.py`.
- **Bug #3 — Firefox hang → muerte del proceso**: Cuando `launch_persistent_context` de Firefox agotaba sus reintentos, el bloque `except` llamaba `_stop_evt.set()` + `break`, terminando todo el batch. Ahora en lugar de abortar, el perfil se marca como bloqueado (`mark_current_profile_blocked()`), se rota a la siguiente identidad disponible (`rotate_identity()`), y el bucle de recuperación externo continúa con `continue`.
- **Bug #4 — Firefox elegido en reintentos**: Añadida blacklist de sesión mediante contador de fallos consecutivos (`_launch_fail_counts`) por perfil de browser. Tras `LAUNCH_FAIL_BLACKLIST_THRESHOLD` (3) fallos, el perfil se bloquea permanentemente para esa sesión, evitando que el mismo engine inestable sea re-seleccionado.

### Improved
- **Reorden del pool de browsers** (`config.py`): WebKit (Safari) pasa al primer puesto (más estable y efectivo contra CAPTCHAs de Idealista), Firefox al último puesto como último recurso, dado su comportamiento inestable bajo Playwright Windows Juggler.
- **Batch runner (`run_batch.py`)**: 
  - `RETRY_LIMIT_PER_URL` aumentado de 2 a 3 para dar más oportunidades tras un fallo de Firefox.
  - Espera entre reintentos (`RETRY_WAIT_BASE`) aumentada de 10s a 30s para dar tiempo al servidor a resetear el estado del browser.
  - Añadido cooldown extendido de 90-150s entre provincias cuando la anterior falló todos los reintentos (señal de bloqueo IP a nivel de red).
  - Mejorada la extracción del nombre de provincia con la función `extract_province_name()` que parsea el slug de la URL de Idealista correctamente para todas las provincias.

## [2.7.8] - 2026-02-21
### Fixed
- **Estabilización de UI y JavaScript**: 
    - Corregido fallo crítico en `app.js` causado por la falta del ID `tableHeader` en `index.html`.
    - Implementada arquitectura robusta para la manipulación del DOM mediante el helper `getEl`, evitando cierres inesperados al faltar elementos.
    - Asegurada la inicialización de socket, listeners y carga de datos mediante bloques try-catch y promesas concurrentes.
    - Eliminada declaración duplicada de `restartServerBtn`.
- **Error "Service Unavailable"**: Eliminada duplicidad de rutas en `server.py` que causaba un `AssertionError`.
- **Botones de Control**: Reparada la lógica de habilitación/deshabilitación de botones "Pausar" y "Detener" sincronizada con el estado del servidor.


## [2.7.7] - 2026-02-20
### Added
- **Integración con BigQuery**: Rediseñada la pestaña "API & Database" (ahora "BigQuery Upload") para permitir la subida directa de archivos Excel desde la carpeta `scraper/salidas` a Google BigQuery. Soporta selección múltiple y carga secuencial.
- **Lógica de Upsert Inteligente**: Implementada estrategia de actualización o inserción (Delete + Insert) basada en la URL para evitar duplicados y optimizar el almacenamiento. Se añade automáticamente una marca de tiempo (`upload_timestamp`) a cada fila.
- **Utilidad de Carga Masiva**: Implementado endpoint en el servidor para procesar y subir lotes de archivos con validación de datos numéricos.

### Improved
- **Omisión de Anuncios Desactivados**: El scraper ahora detecta y salta automáticamente las propiedades ya marcadas como desactivadas ("Anuncio activo" = "No") en todos los modos (Full Scrape, Update URLs, Smart Enrichment). Esto optimiza drásticamente la velocidad de procesamiento al evitar visitas innecesarias a páginas que ya no existen.
- **Carga de Metadatos**: Optimizada la carga de archivos Excel existentes para capturar tanto fechas de actualización como estado de actividad de forma global.

### Removed
- **Legacy Idealista API**: Eliminada toda la funcionalidad de descarga vía API de Idealista (obsoleta). 
- **Limpieza de Código**: Eliminados scripts antiguos (`batch_api_scan.py`, `api_client.py`, etc.) y rutas de servidor no utilizadas para mejorar la mantenibilidad.

## [2.7.6] - 2026-02-20
### Fixed
- **Sincronización de Distritos**: Implementada normalización robusta (minúsculas, sin acentos, sin espacios) para garantizar el cruce de datos Venta/Alquiler.
- **Filtros Booleanos**: Corregido bug que ignoraba filtros de Ascensor/Garaje/Terraza por diferencias de acentuación ('Sí' vs 'Si').
- **Estabilidad de Exportación**: Solucionado crash fatal por variables no definidas y saneamiento de NaNs en el JSON de salida.
- **Visualización Top 100**: Corregido error que limitaba la tabla 'Top 100' a 63 elementos; ahora muestra los 100 resultados completos.
- **Recuperación de Propiedades**: Verificado que las viviendas en el rango de 159.000€ (como las de Baix Segura) ahora se procesan y muestran correctamente.

## [2026-02-20] - URL Update Speed & Data Integrity
### Added
- **Speed Optimization (URL Updates)**:
    - **Fast Mode by Default**: The "Actualizar estado de URLs" process now defaults to **FAST mode** (4-6s per property), matching the speed of the province scraper.
    - **Increased Session Limits**: Raised the property limit before mandatory rests from 50 to **150-200**, reducing total session downtime.
    - **Reduced Rest Duration**: Shortened mandatory anti-bot rests to **5-10 minutes** (from 15m) to maintain security with less impact on total execution time.
    - **Stealth Tuning**: Adjusted stealth presets to use "Standard Stealth" instead of "Extra Stealth" for URL updates, tripling the speed while keeping profile rotation safe.

### Fixed
- **Excel Data Loss Fix**: Resolved a critical bug where selecting specific worksheets (districts) for an update would cause all other worksheets to be deleted from the output file. The script now preserves the entire Excel structure, updating only the selected data.
- **Journal Restoration**: Fixed data restoration from `journal_update.jsonl` to ensure all properties are correctly accounted for when resuming partial updates.
- **Table Synchronization (Analytics Pro)**:
    - Unified data formatting using a centralized `format_dataframe_for_ui` helper in `analysis.py`.
    - Standardized property titles (full description), scores, and currency formatting across both "Principales oportunidades" and "Top 100 viviendas" tables.
    - **Functional Reference Links**: Enabled functional "Ver Refs" links for the Top 100 table, using a new URL-based lookup in `script.js` that works correctly even after sorting.
- **District Matching Hardening**:
    - Implemented `.strip()` on both Venta and Alquiler district strings during the cleaning phase to prevent invisible whitespace from excluding properties.
    - Hardened district matching in the comparable search loop for improved rent estimation reliability.
- **Critical Bugfix (NameError)**: Restored the accidentally removed `safe_col` helper function in `analysis.py`, resolving the fatal error during the export phase.
- **WebKit Connection Robustness**: Fixed case-sensitivity in error detection and added automatic browser restarts for "Failed sending data to peer" errors.

## [2026-02-19] - Browser Robustness & Infinite Rotation
### Added
- **Infinite Identity Rotation**: Overhauled `rotate_identity` logic to ensure the scraper never stops. It now searches the entire pool for available profiles and, if all are blocked, waits for the one with the shortest remaining cooldown before continuing.
- **DataDome Token Integration**: Implemented a specialized 2Captcha solver for DataDome challenges. It extracts the `captcha-delivery.com` URL, retrieves a valid token via 2Captcha API, and injects the `datadome` cookie directly into the browser context for a reliable bypass.
- **CAPTCHA-First Block Logic**: Integrated `solve_captcha_advanced` into the "0 properties found" detection. The scraper now attempts to solve CAPTCHAs (prioritizing DataDome tokens) before triggering a profile rotation on potential blocks.

### Fixed
- **System Lag Removal**: Replaced expensive PowerShell `Get-CimInstance` with a faster `Get-Process` filter in `_cleanup_zombie_browsers`. This resolves the reported mouse/audio stutter during scraper startup.
- **CAPTCHA Solver Integration**: Fixed a logic bug where the CAPTCHA screen was misidentified as a fatal block ("Uso indebido"), causing premature rotation. The scraper now correctly triggers the 2Captcha solver before deciding to rotate.
- **Improved Slider Detection**: Expanded handle selectors in `utils.py` and implemented **DPI-aware coordinate scaling** using `devicePixelRatio`. This ensures accurate clicks on high-DPI (4K/Retina) displays.
- **Firefox Launch Stability**: Increased launch timeout to 120s and added `--no-remote` along with stability environment variables (`MOZ_REMOTE_SETTINGS_DEVTOOLS`, `MOZ_PROXY_ALLOW_BYPASS_FROM_SETTINGS`) to resolve Windows Juggler timeout issues.
- **Persistent Continuity**: Updated `BlockedException` handling to ensure the scraper continues by rotating identity instead of performing a "Hard Stop".

## [2026-02-18] - Log Control & CAPTCHA Statistics
### Fixed
- **Log Panel (Pause Button)**: Fixed the "Pause" button functionality in the live log panel. It now correctly stops auto-scrolling when paused, allowing users to inspect past events. Added a visual indicator (orange border and background) when the log is in a paused state.
- **Auto-scroll Logic**: Unified `autoScrollEnabled` state across all log-adding functions to ensure consistent behavior.

### Added
- **CAPTCHA Statistics**: Implemented session-level tracking for detected vs automatically solved CAPTCHAs.
- **Session Summary**: Added a final summary log at the end of each scraping session (both standard and URL status update) in the format: `📊 CAPTCHAs solved/found: X/Y`.

## [2026-02-18] - CAPTCHA Solver Refinement & Dependency Fix
### Fixed
- **CAPTCHA Solver (2Captcha)**:
  - **Missing Dependency**: Fixed a critical issue where `2captcha-python` was missing from the embedded environment, preventing any cloud-based solving.
  - **Refined Selection**: Added more robust selectors for Datadome/Idealista slider handles (`.arrow-right`, `[aria-label*='Slide']`, etc.).
  - **Improved organic movement**: Implemented a more natural ease-in-out drag motion with random micro-jitter and a pre-drag "wiggle".
  - **Automatic Verification**: The solver now explicitly checks if the CAPTCHA container has disappeared after a drag attempt before reporting success.

### Changed
- Updated `start.py` to ensure `2captcha-python` is installed on startup.

## [2026-02-18] - BigQuery Restoration & UI Regression Fixes
### Fixed
- Fixed `IndentationError` in `update_urls.py` reported by user.
- Repaired UI scorecards showing 0/0 by standardizing layout and unifying socket listeners.
- Restored missing "Pause Log" button in the Live Logs panel.
- Fixed real-time Results table updates for "Actualizar estado de URLs" mode.

### Changed
- Restored BigQuery integration via a dedicated `DatabaseManager` (Supabase remains removed).
- Adjusted Content Grid layout to give more space to the Results table (60% width).
- Updated `database_manager.py` to auto-upload to BigQuery if credentials are found.

## [2026-02-18] - Scraper Port Fix & Analytics Pro UI Polish
### Fixed
- **Scraper Module - UI & JS Robustness Fixes:**
    - Resolved `AssertionError` at server startup by removing duplicate route in `server.py`.
    - Added missing `tableHeader` element to `index.html` to prevent JavaScript `TypeError`.
    - Consolidated global DOM element constants in `app.js` with safety guards (`getEl`).
    - Wrapped all event listener attachments and UI updates in safety checks to prevent script crashes when elements are missing.
    - Optimized initialization sequence using `Promise.all` for concurrent data loading (provinces, Excel files, BigQuery).
    - Fixed duplicate `restartServerBtn` declaration.
- **Scraper Module (Service Availability)**:
  - Resolved "Service unavailable" error by correctly routing the Scraper server to **port 5003**.
  - Synchronized `server.py` and `shared/config.py` to ensure consistent port allocation.
- **Supabase Integration Cleanup**:
  - **TOTAL REMOVAL**: Deleted all residual Supabase-related files (`database_manager.py`, `import_historical_data.py`, etc.).
  - Removed all Supabase buttons ("Subir a Supabase", "Borrar Todo") and the "Base de Datos" section from the Scraper UI.
  - Purged all Supabase API endpoints from the backend server.
- **Scraper UI Layout**:
  - Refined the dashboard layout to ensure the right-hand panels ("Actualizar provincias completas" and "Actualizar estado de URLs") share vertical space (50-50 split), providing a more balanced visual experience.
- **Analytics Pro (Result Visibility)**:
  - **Hidden by Default**: Both the primary results table (including headers) and the "Top 100" accordion are now completely hidden until the profitability analysis is finished.
  - **Repaired HTML Structure**: Fixed a nesting bug in `index.html` where result cards were outside the hidden results container.
  - **JS Logic Update**: Ensured the results area is explicitly shown using `style.display = 'block'` upon analysis completion, even if only the terminal was previously visible.

## [2026-02-18] - Analytics Pro & Scraper UI Fixes
### Fixed
- **Analytics Pro (Table Formatting)**:
  - Tables ("Principales oportunidades" and "Top 100 viviendas") now start EMPTY on initialization for a cleaner UI.
  - Corrected "Puntuación" formatting: values are now rounded to the nearest integer (e.g., 83 instead of 82.5).
  - Restored and enhanced sorting arrows (↑/↓) for both main and Top 100 tables.
  - Ensured "Top 100" table uses the same color-coding for Precision and Score badges as the main table.
  - Added missing `/heartbeat` endpoint to `app.py` to eliminate 404 errors.
  - Fixed a critical `ReferenceError: calcBtn is not defined` that was preventing table rendering.
- **Scraper Tool (URL Status Update)**:
  - **Auto-Initialization**: Fixed bug where "Archivo Excel" dropdown remained stuck on "Cargando...". It now populates automatically on load with file names, property counts, and modification dates (formatted as `[DD/MM HH:MM]`).
  - **UI Restoration**: Restored the missing "Hojas de Excel a actualizar" checklist in `index.html`, allowing users to select specific worksheets for URL status updates.
  - **Standardized Connectivity**: Forced Scraper server to port 5000 and added `allow_unsafe_werkzeug=True` for reliable development startup.


- **2Captcha Support**: Integrated 2Captcha API to handle complex CAPTCHAs (GeeTest and others) when simple slider-drag simulation fails.
- **Advanced Solver Pipeline**: Created `solve_captcha_advanced` in `utils.py` that first attempts a human-like slider solve (free) and falls back to 2Captcha (paid) if needed.
- **GeeTest & Slider Support**: Implemented `solve_slider_2captcha` using the **Coordinates** method to handle the "Desliza hacia la derecha" bar seen in latest Idealista blocks.
- **Improved Advanced Solver**: `solve_captcha_advanced` now intelligently detects GeeTest vs standard sliders and uses the appropriate 2Captcha method (Token-based vs Coordinate-based).
- **Cross-Profile Integration**: Ensured all browser profiles (Stealth, Fast, etc.) across all scripts utilize the unified advanced solver.
- **NordVPN Removal**: Completely removed NordVPN integration, including automated IP rotation logic, frontend status badges, and manual rotation controls to simplify the codebase.
- **Improved Captcha Orchestration**: The `solve_captcha_advanced` function now seamlessly prioritizes local solving and falls back to 2Captcha (Coordinate or GeeTest modes) without needing VPN rotation.
- **Improved Scraper Safety**: Captcha screenshots are now saved to the system's temporary directory to avoid project clutter.


## [2026-02-18] - CAPTCHA Debugging & Refinement
### Fixed
- **CAPTCHA Solver (2Captcha)**:
  - **Identified Root Cause**: Diagnosed that recent solver failures were due to an exhausted 2Captcha account balance ($0.00).
  - **Refined Solver Logic**: hardened `solve_slider_2captcha` in `utils.py` to be more robust for future use:
    - **Expanded Selectors**: Added support for new Idealista/Datadome container types and handle buttons.
    - **Organic Humanization**: Implemented cubic ease-in-out mouse acceleration and vertical jitter to better mimic human drag behavior.
    - **Targeting Precision**: Improved screenshot area validation to ensure the solver receives the correct challenge image.

## [2026-02-18]

### Improved
- **Log Noise Reduction (Core Scraper)**: Streamlined startup and execution logs to reduce terminal clutter.
  - **Lock Cleanup**: Consolidated multiple "Removed stale lock file" messages into a single summary log.
  - **Smart Enrichment**: Merged three separate startup logs (Enrichment Mode, Province/Operation, Target File) into one concise line.
- **Log Noise Reduction (Batch & Periodic Scripts)**:
  - **run_batch.py**: Removed redundant Identity and Mode logs. Combined progress and processing information into a single line.
  - **batch_api_scan.py**: Refactored the progress indicator to consolidate multi-line statistics (Progress, Remaining, ETA) into one clear status line.
  - **run_periodic_low_cost.py**: Removed unnecessary vertical whitespace in per-province logs.
- **UI Refinement**:
  - **Terminal Placeholder**: Removed the initial "Esperando inicio del scraping..." entry from the logs panel to keep the interface clean on startup.
  - **Connection Logs**: Suppressed the initial "Conectado al servidor" message in the UI; it now only appears during reconnection events.


## [2026-02-17]

### Fixed
- **Firefox Launch Stability**: Removed invalid Chromium-specific flags (`--disable-gpu`, `--disable-dev-shm-usage`) from Firefox launch arguments that caused repeated `Timeout 120000ms exceeded` errors and "unrecognized command line flag" warnings. Added `MOZ_REMOTE_SETTINGS_DEVTOOLS` env var to suppress noisy log warnings.
- **Stop Button During Missing Property Checks**: Fixed bug where `StopException` raised by `_interruptible_sleep` and `_goto_with_retry` was caught and swallowed by a generic `except Exception` handler, causing the scraper to continue checking URLs even after the user pressed "Stop". The stop signal now correctly breaks the loop and saves progress.
- **Browser Safety Safeguards**: Rewrote process termination logic to explicitly prevent killing user sessions (Chrome, Firefox, Edge). Scraper now only targets processes with `stealth_profile` in their command line.
- **Falkon Blacklist**: Added a hard-coded block in the launch loop to prevent the unstable Falkon browser from being used, even if present in configuration.
- **Santa Cruz de Tenerife Batch Fix**: Unified "Santa Cruz de Tenerife" province scraping. Removed sub-zone splitting (El Hierro, etc.) to ensure the scraper targets the full province URL as intended.
- **Cleanup Refactor**: Removed duplicate and dangerous `_kill_browser_by_channel` method that was causing redundant process kills.
- **Batch URL Logic Fix**: Disabled automatic expansion of Province URLs into zones. Full province selections now prioritizing the single verified Seed URL (e.g., `/venta-viviendas/alicante/`) over iterating all individual zones, reducing request count and improving stability. Sub-zone expansion is now only performed if explicitly requested via partial selection.
- **Robust Zombie Process Cleanup**: Enhanced `_cleanup_zombie_browsers` to use PowerShell for aggressively finding and terminating hung Firefox instances that block profile directories, specifically targeting processes with `stealth_profile` or residing in `ms-playwright`, while strictly avoiding user's personal browsers.
- **Proactive Lock Cleanup**: Added explicit profile lock file removal (`parent.lock`, etc.) before every browser launch attempt to prevent "Timeout exceeded" startup hangs.
- **Analytics Pro Fix**: Resolved JSON serialization errors: fixed `WindowsPath` object error AND implemented recursive `NaN` cleaning to prevent frontend parsing failures.
- **Merger Tool UI Fix**: Fixed dropdown stacking context issue by applying `z-index` to the parent card container, ensuring options render above the action button.

- **Analytics Pro Refinement**: Improved data quality by filtering out comparables with invalid data (price/m² <= 0) and excluding properties from "Opportunities" if their estimated rent or yield cannot be calculated, resolving the "0€ - 0€" display issue.
- **Comparables UI Fix**: Added missing `precio_m2` field to the JSON output for reference properties, fixing the issue where the "€/m²" column showed "-" despite valid price/size data.
- **UI Enhancement**: Added an info icon (i) with a tooltip explaining the criteria for "Top Opportunities" (Undervalued + Valid Data) in the Analytics Pro results header.

## [2026-02-16]

### Added
- **Split Province Scraper UI**: Refactored the "Actualizar provincias completas" panel into distinct "Venta" and "Alquiler" columns for clearer operation separation.
- **Price Limit Filters**: Added optional "Precio Máx" filters (default 300k€ for Venta, 2k€ for Alquiler) to batch scraping.
  - Automatically appends `/con-precio-hasta_X/` to generated URLs.
  - Ensures filters are applied efficiently across all sub-zones of a province.
- **Improved URL Expansion**: Frontend now strictly prioritizes sub-zone expansion for whole-province selections to guarantee 100% pricing filter coverage.
- **Independent Start Buttons**: Replaced the global start button with separate "Iniciar Venta" and "Iniciar Alquiler" actions to prevent mode confusion.

## [2026-02-14]

### Fixed
- **UI Timer Restoration**: Restored the `startTimer` function in `app.js` and ensured it is called correctly across all scraping modes (Manual, Batch, Update). This resolves the "startTimer is not defined" error.
- **Improved Stop Responsiveness**: Strategically integrated `check_signals()` and `_wait_for_pause()` calls within the scraper's core loops (URL processing and property processing) to ensure the application responds to stop commands almost instantly.
- **Standardized Signal Handling**: Synchronized the stop mechanism across `scraper_wrapper.py`, `batch_api_scan.py`, and `run_periodic_low_cost.py` using a unified filesystem flag and event-based approach.
- **Timer Persistence**: Fixed an issue where the UI timer would reset unexpectedly during batch transitions by prioritizing the batch start time when active.

### Added
- **Alternative Browser Rotation Pool**: Expanded the identity rotation system to include Brave, Opera, and Vivaldi. 
- **Automatic Executable Discovery**: Implemented a robust Windows-based detection logic to automatically find and use these browsers if installed.
- **Fail-safe Browser Switching**: The scraper now intelligently skips missing browsers and rotates to the next available identity in the pool (1-8), preventing session crashes.
- **Firefox Stability Fix**: Resolved 120s timeout hangs by enabling multi-process (e10s) and strengthening recursive lock-file cleanup for Firefox profiles.

## [2.7.5] - 2026-02-20
### Fixed
- Corregido error en la sincronización de distritos (Normalización robusta: minúsculas, sin acentos, sin espacios).
- Solucionado bug en filtros booleanos (Ascensor, Garaje, Terraza) que fallaban por acentos ('Sí' vs 'Si').
- Corregido crash en la exportación JSON por variables no definidas.
- Corregido error de visualización en la tabla 'Top 100 viviendas' (ahora muestra los 100 resultados correctamente).
- Resuelto el problema de propiedades desaparecidas (viviendas de 159.000€ ahora visibles tras corregir filtros y sincronización).
- Saneamiento de NaNs en la salida JSON para evitar errores de sintaxis en el frontend.
- Unificación de formateo de datos para tablas de Oportunidades y Top 100.
- **Hierarchical Geo-Locality**: Rewrote similarity logic to prioritize Barrio > Distrito > Ciudad, ensuring local market dynamics dominate rent estimation.
- **Weighted Valuation Averaging**: Replaced simple means with quality-weighted averages of adjusted prices for higher statistical reliability.
- **Unified Portable Package**: Created a zero-install portable version of all tools in the `python_portable` directory.
- **Smart Launcher**: Updated `START_PORTABLE.bat` with automatic setup detection and configuration.
- **Enhanced Setup Engine**: `SETUP.bat` now installs all tool dependencies from a central `requirements_master.txt` and includes Firefox for Playwright.

## [2026-02-13]

### Added
- **Advanced Hedonic Valuation Engine**: Implemented a professional property valuation algorithm that normalizes comparable prices (Habitaciones, m², Garaje, Estado, Planta) to match target property features.
- **Hierarchical Geo-Locality**: Rewrote similarity logic to prioritize Barrio > Distrito > Ciudad, ensuring local market dynamics dominate rent estimation.
- **Weighted Valuation Averaging**: Replaced simple means with quality-weighted averages of adjusted prices for higher statistical reliability.
- **Unified Portable Package**: Created a zero-install portable version of all tools in the `python_portable` directory.
- **Smart Launcher**: Updated `START_PORTABLE.bat` with automatic setup detection and configuration.
- **Enhanced Setup Engine**: `SETUP.bat` now installs all tool dependencies from a central `requirements_master.txt` and includes Firefox for Playwright.
- **Master Dependency Tracking**: Simplified tool management by consolidating all requirements into `requirements_master.txt`.

### Fixed
- **Browser Identity Robustness**: Improved Firefox launch reliability by disabling background updates/telemetry, implementing aggressive profile lock cleanup, and increasing launch timeouts to 120s.
- **Improved Block Detection**: Reduced false "Uso indebido" detections on short pages by verifying the presence of property elements before flagging a block.
- **Redundant Navigation Prevention**: Optimized session resumption to skip redundant re-navigating to the current listing page if it was already opened during the initialization phase.
- **Advanced Zombie Cleanup**: Enhanced process cleanup on Windows to specifically target orphaned Firefox/Chrome/Edge processes tied to scraper profiles.
- **Resume State Synchronization**: Fixed a bug where browser rotation could overwrite resume progress with "Page 1" if a block occurred immediately after restart. Progress is now restored before the first navigation.
- **Zone mapping to Parent Province**: Fixed a bug where zone-specific URLs (e.g., Albacete Centro) would create new Excel files instead of saving data into the parent province's file.
- **Scraper Stop Responsiveness**: Refined `ScraperController` logic to prevent accidental pausing during stop sequences and synchronized stop flags across all task types for immediate halting.
- **UI Timer Stagnation**: Ensured the elapsed timer starts correctly in all scraping modes (Batch, Update, Enrichment) and persists across province transitions by preventing intermediate sub-task completions from resetting the UI state.
- **Scraper Rollover Resilience**: Fixed a critical bug where the scraper would prematurely terminate during identity rotation due to a rogue stop event trigger.
- **Improved Cooldown Handling**: Wrapped rollover wait periods in exception handlers to prevent thread failure logs when stopping during cooldown.
- **Optimized Resume Logic**: Clarified logs when restoring processed URLs from session files to avoid confusion during restarts.
- **Scraper Log Optimization**: Reduced console log verbosity by removing internal timing, simulation, and debug messages while preserving critical progress and error alerts.
- **Improved Navigation Logs**: Consolidated navigation attempts and progress indicators for cleaner terminal output.
- **Batch Process Lock**: Fixed a logic deadlock where the batch runner would wait indefinitely for a "completed" status that was being masked by its own active process.
- **Log Noise Reduction**: Removed frequent "Human interaction timed out" warning messages from the console.
- **Scraper Mode Switching**: Mode switching is now allowed during execution (hot-swap).
- **UI Button Synchronization**: Fixed an issue where "Pausar" and "Detener" buttons were disabled during identity rotation (`blocked`) and rest periods (`resting`), ensuring continuous user control.
- **Improved City Detection**: Implemented a robust "bottom-up" hierarchical extraction logic for the "Ciudad" field. It now automatically strips noise phrases like "Próximo a" and "Alrededores de", validates candidates against known provinces, and handles diverse formats like "Córdoba, Córdoba".
- **Browser Launch Resilience**: Fixed recurring `Timeout 90000ms exceeded` errors by increasing launch retries to 4 and implementing a progressive randomized backoff.
- **Firefox Flag Fix**: Resolved the "unrecognized command line flag -foreground" error on Windows by explicitly ignoring it in the launch context.
- **Silenced Browser Warnings**: Added environment overrides to suppress Firefox remote settings warnings that were cluttering the logs.
- **Automatic Profile Cleanup**: Blocked profiles are now immediately deleted instead of being archived as `stealth_profile_BLOCKED_*`, ensuring the directory stays clean and disk space is preserved.
- **Startup Maintenance**: Added a routine to automatically purge any existing leftover `BLOCKED` folders on scraper startup.
- **Administrative Fallbacks**: Enhanced district inference using neighborhood dictionaries (BARRIO_TO_DISTRITO) and added smarter fallbacks for small towns where the city itself acts as the administrative district.
- **Firefox Navigation Guard**: Implemented specialized handling for `NS_ERROR_ABORT` in Firefox. The scraper now performs an immediate, zero-delay retry with a 'commit' wait strategy when an abort is detected, significantly speeding up navigation through Idealista's security redirects.
- **Unlimited Deactivation Checks**: Removed the hardcoded limit of 15 properties for missing-from-search verification. The scraper now processes the entire list of missing URLs from the Excel file to ensure complete synchronization and deactivation tracking in a single session.
- **Improved Status Reporting**: Added internal status tracking and a profile efficacy report showing properties scraped per browser profile.



### Improved
- **Block Detection Accuracy**: Refined `_check_for_blocks` and `extract_detail_fields` to correctly identify hard blocks ("Uso indebido", "El acceso se ha bloqueado") and distinguish them from resolvable CAPTCHAs.
- **CAPTCHA Log Fix**: Corrected a false positive where the scraper would log "CAPTCHA solved!" if a block page title matched generic idealista patterns.
- **Browser Stealth Reinforcement**: Added additional anti-detection arguments for Chromium (Chromium, Google Chrome, Edge) and injected more randomized hardware fingerprints (concurrency, memory, GPU) to reduce block rates on non-WebKit profiles.

### Added
- **Smart Efficiency Optimization**: Implemented a "Smart Skip" mechanism that identifies properties already present and enriched in the Excel file directly from the search results, skipping detail page navigation for confirmed active ads.
- **Deactivation Tracking**: Added logic to track properties in Excel that are missing from current search results, performing a targeted status-only check to mark them as inactive with their deactivation date.
- **Detailed Efficiency Summary**: Added a granular summary line at the end of each scraping run that reports the number of new listings, deactivated properties, and "smart-skipped" active ads.
- **Excel Data Standardization**: Batch-renamed 40+ legacy API result files to the standard `idealista_<Province>_venta.xlsx` format to ensure compatibility with enrichment and analysis tools.
- **Improved Update Scraper**: Optimized `update_urls.py` to skip properties already updated during the current day, further reducing redundant scraping.
- **Architectural Deep Dive**: Completed a comprehensive analysis of the project's multi-service architecture (Scraper, Analyzer, Merger, Dashboard).
- **Onboarding Documentation**: Created a summarized [walkthrough.md](file:///C:/Users/alvgu/.gemini/antigravity/brain/9d461782-381f-4e54-be73-85b1a6ef310f/walkthrough.md) documenting service orchestration, anti-detection mechanisms, and analytical pipelines.

## [2026-02-12]

### Added
- **Graceful Scraper Termination**: Implemented `StopException` to handle manual stops and interruptions reliably across all scraping loops and sleep cycles.
- **Thread-Safe Scraper Controls**: Modified `pause`, `resume`, and `stop` methods in `ScraperWrapper` to use thread-safe event manipulation, ensuring responsiveness even during browser-blocking operations.
- **Persistent State on Interruption**: The scraper now automatically saves its progress (`current_page`, `processed_urls`) to `resume_state.json` when paused or stopped by the user, enabling seamless resumption.
- **Robust Control API**: Updated server endpoints (`/api/pause`, `/api/resume`, `/api/stop`) to synchronize control signals across manual, batch, and periodic scraping tasks using filesystem flags.
- **Non-blocking Identity Rotation**: Refactored the browser identity rotation logic to be asynchronous, preventing event loop blocks during profile cooldown periods.
- **Automatic Price Filtering**: Implemented automatic price limit filters for provincial searches (2.000€ for rent, 300.000€ for sale).
- **Persistent Blocking Recovery**: Enhanced the identity rotation system to handle recovery from persistent blocks by ensuring `_processed` URLs are preserved across session restarts.

### Fixed
- **UI Button Synchronization**: Ensured the "Stop" and "Pause" buttons in the web interface are always enabled while any scraping task is active, and standardized labels to "Detener". Fixed a regression where buttons re-enabled prematurely during batch mode errors.
- **Excel Targeting Precision**: Fixed an issue where the user-selected destination file was being overridden by automatic city-detection logic.
- **Server Control Clarity**: Renamed the server stop button from "Parar" to "Detener Serv." and ensured consistency in the UI.
- **Status Reporting**: Enhanced the `/api/status` endpoint to accurately report the combined state of manual scrapers and background batch processes, preventing UI resets during batch retries.
- **Checkpoint Resilience**: Resolved a critical `'NoneType' object has no attribute 'empty'` error during periodic saves.
- **Critical Regressions**: Fixed a `NameError: name 'pd' is not defined` caused by a missing import in `scraper_wrapper.py`.
- **Identity Rotation Crash**: Fixed a `TypeError` when unpacking `rotate_identity()` results during CAPTCHA recovery loops.
- **Forced Filename Consistency**: Ensured that user-selected Excel files ("Baleares", etc.) take precedence over automatic city-detection ("Palma"), preventing filename mismatches in logs and checkpoints.
- **Resume Logic Integrity**: Fixed a bug where processed URLs were not correctly restored from `resume_state.json` during identity rotation, causing the scraper to restart from page 1 instead of continuing.
- **Heartbeat False Alarms**: Modified long sleep cycles (cooldowns) to update activity timestamps, preventing the heartbeat monitor from triggering false "potential hang" alarms.

## [2026-02-11]

### Added
- **Advanced Anti-Detection Tactics**: Implemented comprehensive fingerprint noise and masking in `scraper_wrapper.py`:
    - **Canvas & Audio Fingerprinting**: Added subtle randomized noise to Canvas and Audio contexts to break generic bot fingerprints.
    - **WebRTC Protection**: Added masking to prevent local IP leakage via WebRTC.
    - **Font List Obfuscation**: Masks the list of installed fonts to return only standard system fonts.
    - **Modern Client Hints**: Spoofs `navigator.userAgentData` (brands, platform, architecture) to match modern Chrome versions (v132+).
- **Organic Mouse Movements**: Upgraded the background mouse jitter logic from linear steps to organic **Cubic Bézier curves**. This produces sinusoidal, human-like trajectories that bypass advanced behavioral analysis.

### Fixed
- **Targeted Browser Cleanup**: Refined the browser cleanup logic (`_cleanup_zombie_browsers`) to use a targeted PowerShell command (on Windows) and filtered `pkill` (on Linux). This ensures that only scraper-specific browser processes (matching `stealth_profile`) are closed, leaving the user's personal browser sessions (Chrome/Firefox/Edge) completely untouched.
- **Firefox Launch Resilience**: Enhanced `_clear_profile_locks` to handle Firefox-specific `.parentlock` and `lock` files. Added automated zombie process cleanup before launch to prevent "Profile in use" errors and 90s timeouts.
- **Workspace Optimization**: Performed a deep cleanup of the project directory, removing hundreds of temporary files (`.png`, `.html`, `.log`) and orphaned profile directories to free up disk space.
- **Robust Block Detection**: Broadened hard block keywords to include "uso indebido", "bloqueado", and variations. Improved text extraction using `document.documentElement.innerText` and whitespace normalization to ensure detection of messages across the whole page.
- **Deep Block Verification**: Added a fallback check in the listing loop that performs a thorough text scan if no properties are found, preventing "silent blocks".
- **Captcha False Positive Mitigation**: Relaxed validation in `missing_fields` to only require `URL`, `price`, and `Titulo`. This prevents legitimate property pages from triggering CAPTCHA alarms when optional fields like `Provincia` or `Ubicacion` are missing or misformatted.

## [2026-02-10]

### Fixed
- **Excel Lock Resilience**: Fixed an infinite loop/hang when an Excel output file was open. Added a retry limit (5) and a stop signal check to the export logic.
- **Stop Button Reliability**: Improved the "Stop" command's ability to unblock hanging operations by ensuring the browser closure is triggered correctly within the scraper's event loop.
- **Unlogged Pauses**: Identified and mitigated silent hangs during navigation by adding a 120s global guard/timeout to Playwright operations.
- **Persistent Hangs**: Wrapped `simulate_human_interaction` in a strict 5-second timeout to prevent deadlocks after page load.
- **Resume Functionality**: Scraper now correctly resumes from the specific page (e.g., page 5) saved in the resume state, rather than restarting from page 1.
- **Update Efficiency**: "Update Provinces" now skips URLs already marked as enriched in columns `enriched` or `__enriched__` (values like VERDADERO, TRUE, SI), saving significant time and resources.

### Added
- **Heartbeat Monitor**: Implemented a background "heartbeat" task that logs status and warns if the scraper has been silent for more than 5 minutes.
- **Enhanced Navigation Logs**: Added explicit "Starting navigation" logs to improve tracing and pinpoint the exact moment of potential hangs.
- **Async Stack Dump**: Added automatic stack trace dumping when the heartbeat alarm triggers (5+ min freeze) to identify the exact line of code causing a hang.

## [2026-02-09]

### Fixed
- **Batch Pause Logic**: Fixed a critical bug where pausing the batch scraper failed to signal the active scraper process.
- **Corrección de Pausa en Provincias**: Corregido el botón "Pausar" durante el scraping por provincias/zonas, asegurando que se comunique correctamente con el backend.

### Added
- **Batch Scraper Improvements**:
    - **Premium Destination Selector**: Replaced standard `<select>` with a dark-themed, searchable custom dropdown for Excel files in the Batch panel.
    - **Intelligent Auto-Selection**: implemented automated matching between selected provinces and existing Excel files in `scraper/salidas`.
    - **Accent & Robust Matching**: Enhanced `autoSelectBatchFile` with normalization to handle Spanish accents (e.g., "A Coruña", "Álava") and various filename formats.
    - **Interactive Help**: Added a detailed explanation of the "Update Provinces" process via an information toggle in the UI.
    - **Selection Persistence**: Added manual override protection to prevent auto-selection from overwriting user-chosen files.
    - **Firefox Stability**: Resolved a critical timeout (`Timeout 60000ms exceeded`) in Province Updates.
- **Depuración de Pausas**: Añadido registro granular de tiempos (`DEBUG_TIMING`) para navegación, sleeps y simulaciones de interacción humana para identificar causas de pausas largas.
- **Visibilidad Anti-bot**: Elevado el nivel de log a `WARN` para descansos de sesión (7-15 min) y pausas de café para que sean claramente visibles en la interfaz.
- **Merger Tool Enrichment**: Updated `merger/app.py` to automatically mark merged properties as "enriched".
    - When a URL exists in both files, the resulting row now receives `__enriched__ = True` and `Fecha Enriquecimiento = <TODAY>`.
    - This ensures continuity with the "Enricher Tool" logic and allows these properties to be skipped in future enrichment passes.

## [2026-02-08]

### Added

- **Granular Logging**: Added `DEBUG_TIMING` logs to `scraper_wrapper.py` for coffee breaks, session rests, and long sleeps to diagnose unexplained pauses.
- **UI Safety**: Disabled the "Iniciar scraping de provincias" button in `app.js` whenever any scraping process is active to prevent accidental double-starts.

### Fixed
- **Scraper Tool UI Redesign**: Major interface overhaul for improved efficiency:
    - **50/50 Split Layout**: The main view is now divided into two equal columns, providing a more balanced and readable workspace.
    - **Module Transformation**: The right column is now split into two panels:
        - **"Actualizar provincias completas"**: Houses the multi-province selectors (Venta/Alquiler) and a new **"Iniciar scraping de provincias"** button for dedicated batch scraping.
        - **"Actualizar estado de URLs"**: Separated panel for updating existing Excel files to detect deactivated listings.
    - **Global Action Bar**: Consolidated essential controls (Start, Resume, Pause, Stop, Dual Mode, Server controls) into a unified row at the bottom.
    - **Responsive Grid**: Dynamic grid that collapses to single column on smaller screens.
- **Batch Runner Timeout**: Increased timeout for `/api/stop` calls in `run_batch.py` from 5s to 30s to mitigate read timeouts during browser cleanup.
- **Decoupled Province Selection**: Selecting provinces no longer auto-fills the manual URL input. The URL field is now exclusively for user-entered searches.
- **Dedicated Batch Button**: The new "Iniciar scraping de provincias" button triggers Fast mode scraping for all selected provinces, keeping the manual URL workflow completely separate.
- **Unified State Management**: Refactored `app.js` with a central `updateScraperState` function to ensure Start/Pause/Stop buttons are consistently synchronized across all scraping modes (Single URL, Batch, URL Update).
- **Global Status Emission**: Implemented real-time `status_change` events across the entire backend stack (`server.py`, `scraper.py`, `cli.py`, `update_urls.py`), ensuring the UI correctly reflects process completion, errors, or user interruptions.
- **Province-Zone Mapping Documentation**: Generated `province_urls_mapping.md` with verified Idealista URLs for all 52 Spanish provinces (Venta <300k and Alquiler).
- **Latency Monitoring Instrumentation**: Added granular `DEBUG_TIMING` logs to `scraper_wrapper.py` in critical path (navigation, sleeps, breaks, simulations) to pinpoint unexplained latency bottlenecks.
### Added
- **Persistent Scraper (Infinite Retry)**: Scraper logic updated to never abandon a URL due to blocks/captchas. It now rotates browsers, waits for cooldowns, and retries indefinitely until completion.
- **Smart Enrichment Mode**: Implemented intelligent property enrichment for batch province updates:
    - **Standardized Filenaming**: Outputs to `idealista_{Province}_{Type}_MERGED.xlsx`.
    - **Skip Enriched**: Automatically detects and skips properties already enriched in previous runs.
    - **Auto-Enrich New**: Marks newly scraped properties with `__enriched__ = TRUE` and timestamp.
    - **Province-File Mapping**: Added `province_file_mapping.json` for consistent file targeting.

### Fixed
- **Firefox Launch Timeout**: Reduced browser launch timeout to 60s to prevent hang-ups, and fixed infinite retry loop on failed engines.
- **Engine Rotation Delay**: Removed hardcoded 15-minute wait when switching engines; now rotates immediately (5-15s) while still respecting long cooldowns for blocks.
- **Startup Crash**: Fixed `ImportError` in `scraper_wrapper.py` when running from main dashboard.
- **Scraper Tool UI Redesign**: Major interface overhaul for improved efficiency:
    - **Province-to-File Mapping**: Created `province_file_mapping.json` mapping all 52 Spanish provinces to standardized output files (`idealista_{Province}_{venta|alquiler}_MERGED.xlsx`).
    - **Enrichment Tracking**: New `__enriched__` and `Fecha Enriquecimiento` columns mark properties that have been scraped.
    - **Skip Already Enriched**: Properties with `__enriched__ = TRUE` and a valid `Fecha Enriquecimiento` are automatically skipped to avoid redundant scraping.
    - **New Properties Detection**: New properties not present in the Excel file are automatically added and marked as enriched.
    - **Province Mapping Module**: Created `province_mapping.py` utility module with functions to detect provinces/operations from URLs, load enriched URLs, and manage enrichment markers.
    - **End-to-End Integration**: Smart enrichment flows through `app.js` → `server.py` → `run_batch.py` → `scraper_wrapper.py`.

### Fixed
- **Province Dropdown Conflict**: Resolved a "double-toggle" bug where dropdowns would immediately close after opening.
- **Dropdown Generation Logic**: Fixed a string concatenation syntax error in the JavaScript code.
- **Province Batch URL Mapping**: Resolved a critical naming inconsistency in `app.js` (`url_venta` vs `venta_url`) that caused batch scraping to fail with "No valid URLs".
- **Dynamic Slug Generation**: Added automatic slug extraction from province URLs in `app.js` to ensure robust identification even if slugs are missing from the configuration file.
- **Button Validation Logic**: Fixed a regression where the "Start Scraping" button remained disabled due to conflicting validation logic between the Province Selector and Excel Enrichment panels.
- **Encoding & Log Mojibake**: Resolved `ðŸ”„` and "Error en monitor" by enforcing `utf-8` encoding in `server.py` subprocess calls.
- **Unwanted URL Expansion**: Fixed a logic error where selecting a whole province (e.g., Almería) resulted in scraping its sub-zones (Alpujarras) instead of the main province page. Added `expand: false` override in `app.js` and `server.py`.
- **UI Script Initialization**: Fixed an unresponsive province selector caused by a duplicate `startBatchFromProvinces` definition and problematic `cloneNode` event listeners in `app.js`.
- **Batch Stop Mechanism**: Modified `run_batch.py` to send an explicit `/api/stop` command to the server on interruption, ensuring active scrapers and browser processes are terminated cleanly along with the runner.
- **Scraper Stability (UnboundLocalError)**: Fixed a crash in the main scraping loop caused by a local `import time` statement shadowing the global module during instrumentation.

## [2026-02-07]

### Added
- **Multi-Browser Rotation with Profile Cooldowns**: Implemented a sophisticated browser engine rotation system to evade bot detection:
    - **Dual Engine Support**: Scraper now supports both Chromium and Firefox (via Playwright).
    - **Sequential Rotation**: Batch scraping alternates between engines to diversify browser fingerprints.
    - **Profile Cooldown System**: When a browser profile is blocked (CAPTCHA/Uso Indebido), it enters a 15-minute cooldown.
    - **Automatic Engine Selection**: The system selects the next available engine that's not in cooldown.
    - **Per-Engine Profiles**: Separate persistent profiles for each browser (`stealth_profile_chromium`, `stealth_profile_firefox`).
    - **API Extension**: `/api/start` now accepts `browser_engine` parameter (`chromium` or `firefox`).
- **Smart Rotation Logic**: 
    - **Immediate Relaunch**: If a profile is blocked and another engine is available, the scraper now switches and restarts immediately (5-15s delay) instead of waiting for the blocked profile to cool down.
    - **Dynamic Batch Waits**: Removed fixed 15-minute waits in batch mode; the system now only checks for available engines and waits only if *all* are blocked.
- **Provincial URL Expansion (Batch Search)**: Implemented a new engine that automatically desegregates provincial URLs into smaller, sequential sub-zone batches.
    - Added `province_zones.json` mapping for 25 high-volume Spanish provinces (Madrid, Barcelona, Valencia, etc.).
    - When starting a batch scrape for a province, the backend automatically expands it into its constituent districts/towns (e.g., Madrid -> Madrid Capital, Corredor del Henares, etc.).
    - This bypasses Idealista's 2,000-property display limit, ensuring 100% data coverage for entire provinces.
- **Dynamic Batch UI**: The frontend log now displays the expanded URL count total (e.g., "Batch iniciado, URLs totales: 12").
- **Advanced Anti-Bot Evasion (Phases 1 & 2)**: Comprehensive countermeasures against Idealista's bot detection:
    - **Deep Fingerprint Spoofing**: Injected JavaScript that masks Chrome DevTools Protocol (CDP) signatures, spoofs WebGL vendor/renderer to match real NVIDIA GPU, adds realistic navigator.plugins, patches Permissions API, and randomizes timing functions.
    - **Randomized GPU Fingerprints**: Each session now uses a randomly selected GPU from a pool (NVIDIA RTX 3060, GTX 1660 Ti, AMD RX 6700 XT, Intel UHD 630, etc.) instead of static values.
    - **Enhanced Chromium Launch Args**: Added 12 new anti-automation arguments including `--disable-blink-features=AutomationControlled`, `--disable-features=IsolateOrigins`, `--force-color-profile=srgb`, etc.
    - **Playwright-Stealth Plugin**: Integrated for additional 30+ automation indicator patches.
    - **Continuous Mouse Jitter**: Background task that subtly moves the mouse at random intervals during page loads to maintain "human presence".
    - **Realistic HTTP Headers**: Added Accept-Language (es-ES), DNT, and Upgrade-Insecure-Requests headers to match real browser behavior.
    - **Google Warmup Removed**: Eliminated the pre-scrape Google navigation (added delay without evading detection).
    - **NordVPN Periodic Rotation Removed**: VPN now only rotates IP when a block is detected, not proactively every N provinces.

### Fixed
- **Province URL Patterns**: Resolved 404 errors for special-case provinces that do not use the `-provincia` suffix in Idealista's routing.
    - Corrected slugs for: Álava, Asturias, Baleares, Cantabria, Guipúzcoa, La Rioja, Las Palmas, Navarra, Santa Cruz de Tenerife, and Vizcaya.
    - Fixed Ceuta and Melilla patterns to use the specific `[city]-[city]` format (e.g., `ceuta-ceuta`).
- **Data Integrity**: Province expansion ensures more granular results, reducing the likelihood of hitting "soft blocks" by spreading the load across smaller search segments.

## [2026-02-07]

### Added
- **Merger Tool Overhaul**: Complete refactor of the Excel Merger utility (`merger/app.py`).
    - **Advanced Sorting**: Added functionality to sort the file list by Name (A-Z), Modification Date (Newest/Oldest), and File Size.
    - **Detailed Statistics**: The merge result screen now provides a comprehensive breakdown:
        - Unique properties and districts for *File 1*.
        - Unique properties and districts for *File 2*.
        - Final unique count and total **duplicates removed** in the merged file.
    - **Flexible Validation**: Updated backend logic to accept both English ("rent", "sale") and Spanish ("alquiler", "venta") keywords in filenames.
    - **Output Naming**: Standardized output filenames to end in `_MERGED.xlsx` (previously `_MERGE.xlsx`).
    - **Smart De-duplication Update**: Refined merging logic to prioritize *File 1* row structure but intelligently update specific fields (`exterior`, `Ciudad`, `Fecha Scraping`) from *File 2* if the URL exists in both.
- **Batch Data Consolidation**: Implemented and executed a one-off batch script (`batch_merger.py`) to consolidate all historical scattered files in `scraper/salidas`.
    - Merged multiple "updated" batches into unified files.
    - Merged API-only batches with Idealista-scraped files for A Coruña, Alicante, Álava, and Toledo.
    - Cleaned up the workspace by moving 30+ intermediate/original files to `scraper/salidas/old`.

## [2026-02-06]

### Added
- **NordVPN Automatic IP Rotation**: Integrated NordVPN CLI into the scraper for automatic IP rotation.
    - A new toggle "Rotar IP con NordVPN al detectar bloqueo" in the UI (disabled by default).
    - If enabled, when a CAPTCHA or block is detected, the scraper closes the browser, rotates the IP, and relaunches automatically.
    - **API Endpoints**: `/api/nordvpn/status` and `/api/nordvpn/rotate` for real-time monitoring and manual rotation.
    - **VPN Status Badge**: Header badge displays current NordVPN connection status (Connected/Disconnected).
- **Infinite Batch Retries**: Updated `run_batch.py` to remove the retry limit. The scraper now persists indefinitely, waiting 15 minutes between attempts if a CAPTCHA block is detected.
- **Robust CAPTCHA Recovery**: Implemented a 30-second wait-and-check loop in `scraper_wrapper.py` before aborting. If the block persists, it performs a clean shutdown with a saved checkpoint, signaling the batch runner for a full restart.
- **Scraper Thread Safety**: Added comprehensive exception handling to the scraper's background thread in `server.py`, ensuring status updates (e.g., `blocked`) reach the UI even on fatal errors.
- **UI UX Polish**: Updated the first scorecard label in the Scraper Tool from "Provincias" to "**Páginas**" to correctly reflect the scraping progress per page.

### Fixed
- **Critical Bugfix**: Resolved an `IndentationError` in `scraper_wrapper.py` that was preventing the scraper service from starting and causing "Service timeout" errors in the dashboard.
- **Batch Scraping UI Fixes**: Batch Scraping now correctly updates Start/Pause/Stop interaction button states upon launch.
    - Added specific API endpoints `/api/batch/stop`, `/api/batch/pause`, `/api/batch/resume` to control the background batch process via flags.
    - Frontend now detects batch mode and routes pause/stop actions to the correct batch endpoints.
- **VPN Control Refactor**: Disabled automatic NordVPN status polling on page load. Now requires explicit user activation via the checkbox toggle.


## [2026-02-05]

### Added
- **API Pagination Bypass**: Removed `totalPages` check in `api_client.py`. The scraper now ignores API hints and continues fetching until the results list is empty.
- **Extended Scanning**: Increased default scanning limits in `.bat` files from 300 pages to 2,000 pages (up to 80,000 properties per province).
- **Enricher Auto-Restart**: Implemented a 15-minute recursive recovery loop for CAPTCHA/Soft Ban blocks.
- **UI Sync Fixes**: Corrected port mismatch (5000 -> 5003) and event name alignment for real-time progress updates.
- **Repair Scripts**: Added `RE-RUN_TRUNCATED_ALQUILER.bat` and `RE-RUN_TRUNCATED_VENTA.bat` to specifically re-scan provinces that hit the previous 2,000 item limit.
- **CAPTCHA Detection Improvements**: Enhanced `detect_captcha` in `update_urls.py` to check HTML source (`page.content()`) for:
    - "recibiendo muchas peticiones tuyas" (Soft Block)
    - "el acceso se ha bloqueado" (Hard Block)
    - "desliza hacia la derecha" (Slider CAPTCHA)
- **Immediate Block Response**: Scraper now triggers recovery mode **immediately** upon detecting block messages, without waiting for consecutive failures.
- **Periodic Scraper Preparation (< 300k)**: Added `low_cost_provinces.json` with URLs for all 52 Spanish provinces for low-cost housing searches.
- **Periodic Scraper Evolution (Dedicated Tab)**: Refactored the monthly low-cost scraper into a standalone module:
    - **Dedicated Tab**: Moved to a new "Escaneo Mensual <300k" tab for better workspace organization.
    - **Real-time Monitoring**: Implemented real-time log streaming from the background script to the UI via SocketIO.
    - **Full Controls**: Added Start, Stop, Pause, and Resume controls with backend signal handling (flag-based).
    - **Dynamic Progress Table**: Added a results table that tracks the status and progress of all 52 provinces in real-time.
    - **Backend Streaming**: Refactored `server.py` to use a monitor thread for capturing and emitting `subprocess` output.

## [2026-02-03]

### Added
- **Gemini Deep Research (Grounding)**: Migrated "Deep Research" from Google Custom Search to native **Gemini Grounding**.
    - Removed dependency on Custom Search Engine ID (CSE) and rate limits.
    - Simplified `analyzer/deep_research.py` to use `google.generativeai` with `tools='google_search_retrieval'`.
    - Gemini now autonomously performs the searches based on the 21 research topics.
- **Dynamic Province Detection**: Fixed hardcoded "Madrid" in frontend logic.
    - `analyzer/static/script.js` now extracts City and Province from filenames (e.g., `API_BATCH_Melilla...` -> Melilla).
    - Ensures "Generar informe con IA" and "Deep Research" use the correct geographical context.
- **Library Upgrade**: Upgraded from `google-generativeai` to `google-genai` (v1.61.0) for better tool support.
- **Model Upgrade**: Switched Deep Research model to `gemini-3-flash-preview` for latest capabilities.
- **Feature Unification**: Merged "Generar informe con IA" and "Deep Research".
    - The "Generar informe" button now triggers the Deep Research engine directly for the top district.
    - Prompts unified to combine "Visual Analyst" styling with Deep Research data depth.

### Changed
- **Frontend Fix**: Fixed "BATCH" appearing in district names by correctly parsing `API_BATCH_` filenames (index 2 for City).

## [2026-02-02]

### Added
- **Centralized Configuration Module**: Created `shared/config.py` to unify ports, paths, and API keys across all services.
    - Ports (SCRAPER_PORT=5003, ANALYZER_PORT=5001, DASHBOARD_PORT=5000, METRICS_PORT=5004) now imported from a single source.
    - Google API Key for LLM reports now loaded from environment variable `GOOGLE_API_KEY` with fallback to default.
    - RapidAPI credentials centralized with environment variable support.
- **API Price Filter (300k Max)**: Added `API_MAX_PRICE=300000` configuration to limit property downloads to ≤300,000€.
    - The `fetch_api_page` function in `api_client.py` now includes `&maxPrice=300000` in all API requests by default.
    - This reduces database load and API calls by filtering out high-priced properties at the source.
- **Background Enrichment Worker** (`scripts/enrich_worker.py`): Automatic hybrid API+Scraper approach.
    - Reads URLs from API-downloaded Excel files and visits each to extract missing fields.
    - **18 additional fields**: m2 útiles, orientación, año construcción, certificación energética, gastos comunidad, okupado, copropiedad, con inquilino, nuda propiedad, cesión remate, etc.
    - Price filter: Only enriches properties ≤300,000€.
    - Resume capability: Tracks enriched URLs in `.enrich_state.json`.
    - Rate limiting: Conservative delays (8-20s between pages, 2-5min between batches).
    - CAPTCHA detection with manual resolution support.
    - Run with: `scraper/RUN_ENRICH_WORKER.bat` or `python scripts/enrich_worker.py --input "scraper/salidas/API_BATCH_*.xlsx"`
- **Unicode Safety & Windows Compatibility**: Fixed a `UnicodeEncodeError` in the `log` function by implementing ASCII fallbacks for special characters (`≤`, `→`, `€`), ensuring stability on Windows consoles.
- **Concise User Logs**: Streamlined the enrichment worker's terminal output to be less technical and more informative ("Procesando X inmuebles..." instead of internal filtering steps).
- **Fast File Loading (Limit 100)**: Added a result limit to the enrichment file selector and optimized the `/api/salidas-files` endpoint, ensuring instantaneous UI response even with thousands of files.
- **Improved API Dashboard UI**:
    - Reverted province selector to a visible multi-select list for faster navigation.
    - Simplified the "API & Database" panel by removing redundant nested "Configuración" boxes.
    - Integrated a live search/filter input to quickly find and select provinces in the batch scan list.
    - Added "SEL. FILTRADAS" and "LIMPIAR" shortcuts for efficient batch configuration.
- **Premium UI Aesthetic**: Introduced high-fidelity CSS components for selects and section headers, adopting the "Market Metrics" premium theme project-wide.
- **Robust Scraper Lifecycle**: Improved stop button logic and terminal status handling (`completed`, `stopped`, `error`).
    - UI now consistently resets state (`resetUIState`) on all terminal statuses.
    - Added programatic stop triggering when "Uso Indebido" is detected, ensuring immediate UI feedback.
    - Explicit confirmation of browser closure logged across all exit paths: "✅ Browser closed successfully."
- **Fixed "Precio por m2" Precision**: Replaced direct text extraction with a robust programatic calculation (`price / m2_construidos`).
    - Handled Spanish decimal commas (`,`) in fallback extraction to prevent incorrect 100x scaling.
    - **Smart Formatting**: Applied 2 decimal places for rental prices (values < 100) and integer rounding for sales (values ≥ 100) in Excel exports.
- **Improved Background Enrichment**:
    - Replaced early returns with loop breaks in `enrich_worker.py` to ensure final status updates and cleanup.
    - Harmonized error handling with the main scraper for consistent stealth and block detection.

### Changed
- **main.py**: Imports ports from `shared.config` instead of hardcoding.
- **analyzer/app.py**: Uses `GOOGLE_API_KEY` from shared config for LLM report generation.
- **dashboard/app.py**: Added project root to `sys.path` for shared module access.
- **scraper/idealista_scraper/api_client.py**: Refactored to use shared config; added `max_price` parameter to `fetch_api_page`.

## [2026-02-01]

### Added
- **Optimized Database Schema**: Unified `listings` table for maximum efficiency with "Analyzer Pro".
- **Logical Separation**: Added `operation` ('VENTA'/'ALQUILER') and `province` columns with auto-population logic.
- **High-Speed Indexing**: Created `idx_analytics_fast` for instant queries by Province/Operation.
- **Robust Ingestion**: Updated input processing to handle multi-sheet Excel files and sanitize integer parsing.
- **Strict filtering**: Ingestion now strictly filters files by 'venta'/'alquiler' keywords to ensure data integrity.
- **Full Data Load**: Successfully reloaded all historical data from Excel archives, ensuring 100% sheet coverage.

## [2026-01-31]

### Added
- **Enhanced Mortgage & Yield Calculator**: Full replication of `Calculadora.xlsx` advanced logic.
    - **Amortization Tab**: Added a dedicated "Hipoteca" view with a full amortization table (360+ rows), calculating monthly Interest, Principal Repayment, and Remaining Balance.
    - **Dual Scenario Analysis**: Implemented "Prudente" and "Optimista" calculation modes for Net Yield, Annual Cashflow, and ROCE (Return on Capital Employed).
    - **Tax Optimization**: Updated IRPF reduction from 60% to **50%** to match the new Housing Law (Ley 12/2023) and the definitive Excel formulas, resolving the 416€ vs 333€ discrepancy.
    - **Regional Accuracy**: Verified and updated ITP rates for all 19 Spanish Autonomous Communities as of 2025 (e.g., Andalucía 7%, Galicia 8%, Navarra 6%).
    - **Advanced Deductions**: Integrated mortgage interest from the first year and the 3% property depreciation into the net benefit calculation.
    - **Real-time Synchronicity**: Seamless bidirectional synchronization between the "Análisis" and "Hipoteca" tabs.
    - **Dynamic Inputs**: Added missing fields for Agency Commission, Notary, Registry, Gestoría, Tasación, and split insurance fields (Hogar, Vida, Impago).
- **Multi-Tab Interface**: Refactored the calculator into a single-page app with internal navigation (Análisis | Hipoteca) for instant data sharing.
- **Main Sidebar**: Added "Calculadora" as a top-level tool for quick access.
- **Deep Table Integration**:
    - **Smart Mapping**: Implemented an automated district-to-region mapper that pre-selects the correct Comunidad Autónoma and ITP rate based on property location.
    - **UI Polish**: Reordered columns in "Principales oportunidades" and added the "Calcular rent. neta" action button.
    - **Cross-Origin Bridge**: Implemented a secure `postMessage` protocol to allow the Analyzer (port 5001) to communicate with the Launcher (port 5000) and open the calculator with pre-loaded data.

### Modified
- **Cross-Module Navigation**: Implemented `window.openCalculator` in the launcher to allow direct communication between the Analyzer and the Calculator.
- **Data Flow Integration**: The "Calcular" button in `Analytics Pro` now triggers a deep-sync that pre-fills purchase price and rental estimates into the new tabbed calculator.

## [2026-01-30]

### Added
- **Smart Rent Calculation**: Overhauled the rent estimation logic in `Analytics Pro`. The new algorithm enforces strict Property Type matching (Pisos vs Chalets) to prevent skewed comparisons.
- **Weighted Precision Scoring**: Implemented a detailed, user-defined weighting system for precision scoring:
    - **20%** Surface Area (m²)
    - **20%** Bedrooms
    - **15%** Property Type (Piso/Casa)
    - **15%** District
    - **15%** Bathrooms
    - **15%** Extras (Garage, Terrace, Elevator)
- **API Integration (Idealista7)**: Implemented `scripts/sync_market_data.py` for massive market data synchronization via API.
    - **Snapshot Strategy**: Fetches thousands of active listings (Rent/Sale) to build a robust historical database.
    - **Data Normalization**: Automatically formats API data to match existing scraper schema (Booleans, Ordinals, Districts).
    - **District Splitting**: Exports data into district-specific worksheets for seamless Analyzer integration.

### Fixed
- **Comparables Reliability**: Comparisons for "For Sale" properties now strictly avoid mixing distinct property types (e.g., comparing a flat with a detached house) to ensure realistic rent estimates.

## [2026-01-28]

### Added
- **Automated CAPTCHA Solver**: Implemented an intelligent slider solver that mimics human-like dragging behavior (variable speed, slight jitters, and overshoot) to automatically bypass "Slide to Right" challenges.
- **Advanced Stealth**: Implemented "Uso Indebido" detection with hard-stop logic. The scraper now immediately halts and archives the compromised profile if a block is detected.
- **Human Emulation**: Added randomized Bezier-curve mouse movements and realistic micro-pauses to mimic human browsing behavior across all scraper tools.
- **Flag Removal**: Resolved the "No se admite el indicador de línea de comandos" warning by explicitly ignoring the `--no-sandbox` flag and removing the detected `--disable-blink-features=AutomationControlled` flag in all browser launches.
- **Profile Auto-Cleaning**: Blocked profiles are automatically renamed (e.g., `stealth_profile_BLOCKED_...`), ensuring the next session starts with a fresh, unflagged identity.
- **Journaling for URL Updates**: Replaced potential data-loss checkpointing with a robust line-by-line JSONL journal. Resuming now reloads actual scraped data, preventing information loss on resume.
- **Auto-Resume for Main Scraper**: The regular scraper now automatically detects when a CAPTCHA is solved and resumes operation without requiring manual confirmation. The "Continuar" button has been removed from the UI.
- **Resumable URL Updates**: Implemented checkpointing for the "Actualizar estado de URLs" feature. Users can now resume an interrupted update session from the exact property where it stopped.
- **Hot-switching for URL Updates**: Users can now dynamically switch between Fast and Stealth modes during the "Actualizar estado de URLs" process, instantly adjusting request delays.
- **Update Start/Resume UI**: Added a dedicated "Reanudar Update" button in the UI that appears automatically when an unfinished update session is detected for the selected Excel file.

### Fixed
- **Startup Window**: Removed the "Press any key to continue" pause from the main startup script, allowing the window to close automatically when the application exits.
- **Resume Data Loss**: Fixed a critical issue where resuming a URL update skipped previously processed items without restoring their data. The new journaling system ensures zero data loss.
- **Update Pause/Resume UI Fix**: Resolved an issue where the "Pause" button in the URL update process did not update the UI, leaving the "Resume" button inaccessible. The backend now explicitly communicates status changes viaized `[STATUS]` messages.
- **Inactive Property Logic**: Modified `update_urls.py` to check for inactive/expired property status *before* field validation, preventing false CAPTCHA detections on removed ads.
- **Update Script Syntax Error**: Fixed a critical `SyntaxError` in `update_urls.py` caused by a duplicate `except` block.
- **URL Injection in Updates**: Fixed infinite loops in the URL update process by ensuring the `URL` is correctly injected into the data dictionary before validation.

## [2026-01-27]

### Added
- **Skip Deactivated Listings**: Implemented logic in `scraper_wrapper.py` to detect and skip deactivated properties ("anuncio ya no está publicado") instead of treating them as CAPTCHAs or errors.
- **Enhanced Stop Confirmation**: Replaced generic "Closing browser..." log with a clearer "Scraper completely stopped. Browser closed." to confirm the process has fully terminated.
- **Manual Stop Resume Support**: The scraper now preserves the `resume_state.json` file when stopped manually by the user, allowing for session resumption at a later time.
- **Automated Resume Button**: The frontend now automatically refreshes and enables the "Reanudar sesión" button when a manual stop is detected.
- **Stealth Wait Skip**: Switching from Stealth to Fast mode now immediately interrupts long waits (coffee breaks, session rests), allowing for instant speed-up.

### Fixed
- **Dashboard Metrics Inconsistency**: Fixed boolean filters for Garaje, Terraza, and Trastero in `dashboard/static/script.js` to correctly display property percentages.
- **Resume Button State**: Resolved issue where the Resume button would disappear or fail to enable after a manual stop.
- **Git Hygiene**: Added `scraper/app/resume_state.json` to `.gitignore` to prevent tracking temporary session states.
- **Block Detection in Loops**: Fixed a logic gap where "uso indebido" blocks encountered during property extraction were misidentified as CAPTCHA, causing the scraper to hang instead of restarting.
- **UI Clarification**: Removed the "Arrancar" server button from the UI, as the web interface cannot start the server once it is stopped (manual restart is required).
- **Update URLs Loop**: Fixed infinite loop where valid properties were flagged as CAPTCHAs due to missing URL injection in `update_urls.py`.
- **Update URLs Resumption**: Fixed bug where solving a CAPTCHA during an update did not resume the process correctly.

## [Previous Sessions]

### Added
- **Auto-Recovery for Blocks**: Enhanced `BlockedException` handling in `scraper_wrapper.py` with automated session restarts, clearer logging ("Reiniciando sesión automáticamente..."), and explicit browser closure to avoid resource leaks.
- **Resume Tooltips**: Added Seed URL and Page Number to the "Reanudar sesión" button tooltip in the UI for better context.
- **Stop Logs**: Added explicit termination confirmation to all exit paths in the scraper controller.

### Fixed
- **Scraper Startup Hang**: Resolved a `NameError` in `scraper_wrapper.py` that prevented the scraper from initializing.
- **Resume Functionality (JS)**: Fixed a `ReferenceError` in `app.js` caused by a missing reference to `outputDirInput`.
- **Portable Environment Bug**: Fixed a critical issue with the portable Python distribution where the `browsers` folder had a trailing space in its name, causing Playwright to fail.
- **Dashboard Room Distribution**: Fixed a sorting/logic issue in property distribution charts within `dashboard/app.py`.

### Cleanup
- **Workspace Optimization**: Removed large, unnecessary directories (`scraper/stealth_profile`, `scraper/python`) and updated `.gitignore` rules.
- **Git Tracking**: Cleaned up internal cache and large binary files from the repository history.
