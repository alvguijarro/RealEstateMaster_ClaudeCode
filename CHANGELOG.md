# Changelog - RealEstateMaster

## [Latest] - 2026-01-27

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
