# Changelog - RealEstateMaster

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
    - `analyzer/static/script.js` now extracts City and Province from filenames (e.g., `API_BATCH_Melilla...` -> Melilla).
    - Ensures "Generar informe con IA" and "Deep Research" use the correct geographical context.
- **Library Upgrade**: Upgraded from `google-generativeai` to `google-genai` (v1.61.0) for better tool support.
    - `analyzer/static/script.js` now extracts City and Province from filenames (e.g., `API_BATCH_Melilla...` -> Melilla).
    - Ensures "Generar informe con IA" and "Deep Research" use the correct geographical context.
- **Library Upgrade**: Upgraded from `google-generativeai` to `google-genai` (v1.61.0) for better tool support.
- **Model Upgrade**: Switched Deep Research model to `gemini-3-flash-preview` for latest capabilities.

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
