# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

## [2026-02-05] - Graceful Exit & Partial Naming
- **Feature:** Implemented `_updated_partial.xlsx` suffix for intermediate and incomplete enrichment saves.
- **Feature:** Automated promotion of `_partial` files to `_updated.xlsx` upon 100% completion.
- **Core:** Implemented `update_stop.flag` for graceful shutdown, ensuring the scraper saves progress before exiting when stopped via UI.
- **Git:** Optimized `.gitignore` to exclude `stealth_profile/` and temporary journal files, preventing repository bloat and protecting session privacy.
- **Maintenance:** Confirmed persistence of `enriched_history.json` for multi-session caching.

## [2026-02-05] - Enrichment UI Refinements
- **UI:** Removed emojis from logs for cleaner, Mojibake-free output.
- **UI:** Enhanced batch progress display with detailed file, district, and property counts.
- **UI:** Added real-time green highlighting for new fields in the Results table.
- **UI:** Added elapsed time timer for batch processes.
- **UI:** Renamed "Páginas" scorecard to "Provincias" and "Scrapes History" to "Enrichment History".
- **UI:** Added hover effect to Pause button and Trash icon for batch logs.
- **Core:** Improved skipped item handling to reflect pre-enriched progress immediately.

## [2026-02-04] - Advanced Human Emulation for Enrichment
- **Core:** Ported 'Coffee Break' logic to Enrichment module.
- **Core:** Ported 'Session Rest' (Long Pause) logic to Enrichment module.
- **Core:** Implemented 'Variable Scroll' with pauses and back-scrolls for Enrichment.
- **Config:** Centralized stealth timing constants in `config.py` for shared use.
- **UI:** Added detailed status logs for stealth actions (e.g., "Coffee break: 45s").

## [2026-02-03] - Batch Processing & Optimization
- **Feature:** Added Batch Enrichment Tool to process multiple Excel files sequentially.
- **Backend:** Implemented `BatchManager` in `server.py` for queue management.
- **UI:** Added Batch interface with file selection, progress tracking, and global scorecards.
- **Core:** Optimized `update_urls.py` to stream logs via WebSocket.
- **Performance:** Used `os.scandir` for faster file listing in API.

## [2026-02-02] - Price Calculation Fix
- **Fix:** Corrected "precio por m2" calculation in enrichment to ensure valid numeric output.

## [2026-02-01] - BigQuery Migration
- **Feature:** Added BigQuery migration script with partitioning and clustering.
- **Tool:** Updated Analytics Pro to query BigQuery directly.

## [2026-01-30] - Calculator & UI Tweaks
- **UI:** Refined Calculator input fields for maintenance and insurance costs.
- **UI:** Fixed "Delete All" functionality in Transaction Manager.
- **UI:** Fixed Market Metrics visualization bug for asset percentages.
- **Feature:** Implemented Excel Merger Tool for consolidating files.
