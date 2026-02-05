# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

## [2026-02-05] - Enrichment Overhaul (UI, Consistency & Stealth)
- **Core:** Implemented "Mother File" detection to automatically use original source Excel when a partial result is selected, ensuring 100% data preservation.
- **Core:** Integrated robust "Safe Merge Save" logic to prevent data loss or worksheet dropping in intermediate saves.
- **Core:** Implemented `update_stop.flag` for graceful shutdown and progress preservation.
- **UI:** Split file list into "Pendientes" and "Completados" with visual status cues (yellow/green).
- **UI:** Fixed real-time synchronization of counters (Provincias/Propiedades) and Results table via `property_scraped` WebSocket listener.
- **UI:** Corrected "Enrichment History" to filter for relevant processed files (`_updated.xlsx`).
- **Fix:** Resolved "Start Batch" button activation bug.
- **Fix:** Removed emojis from backend logs to prevent encoding/Mojibake issues in Windows/UTF-8.
- **Git:** Optimized `.gitignore` for stealth profiles and temporary journal files.

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
