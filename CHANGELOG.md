# Changelog - RealEstateMaster

## [Latest] - 2026-01-30

### Added
- **Smart Rent Calculation**: Overhauled the rent estimation logic in `Analytics Pro`. The new algorithm enforces strict Property Type matching (Pisos vs Chalets) to prevent skewed comparisons.
- **Weighted Precision Scoring**: Implemented a detailed, user-defined weighting system for precision scoring:
    - **20%** Surface Area (m²)
    - **20%** Bedrooms
    - **15%** Property Type (Piso/Casa)
    - **15%** District
    - **15%** Bathrooms
    - **15%** Extras (Garage, Terrace, Elevator)

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
