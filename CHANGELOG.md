# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

## [2026-02-05] - Enrichment Overhaul & Data Integrity Fixes
- **Core:** Implemented "Mother File" detection to automatically detect and use the original Excel source when selecting partial results, ensuring 100% data preservation and correct progress tracking.
- **Core:** Developed "Safe Merge Save" logic to prevent worksheet loss and ensure all original data is preserved during enrichment saves.
- **Core:** Implemented `update_stop.flag` for graceful shutdown and robust progress storage.
- **UI:** Split file management into "Pendientes" and "Completados" columns with real-time status highlighting (yellow for partials, green for finished).
- **Core:** Implemented **Auto-Restart & Recovery Mode** for CAPTCHA/Soft Bans:
  1. Detects "Uso Indebido" or CAPTCHA blocks.
  2. Safeguards progress with a checkpoint save.
  3. Wipes the browser profile to ensure a fresh identity (new `user-data-dir`).
  4. Enters a **15-minute cool-down loop**.
  5. Retries the failed URL indefinitely until successful, effectively bypassing long-duration IP/Session blocks without human intervention.
- **UI:** Connected missing `progress` and `property_scraped` WebSocket listeners to revive real-time updates for:
    - **Scorecards:** Correct live count for "Provincias" and "Propiedades".
    - **Results Table:** Live row insertion with highlighted new fields.
    - **Status Bar:** Detailed "Progreso del lote" text showing current file and sheet info.
- **UI:** Refined "Enrichment History" to filter out noise, showing only completed `_updated.xlsx` results.
- **Fix:** Resolved critical bug preventing "Iniciar Lote" button from activating.
- **Fix:** Removed emojis from backend logs to solve Windows encoding/Mojibake issues.
- **Git:** Hardened `.gitignore` to exclude stealth browser profiles and temporary journal files.

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
