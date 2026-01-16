# Idealista Scraper - Web Interface

A standalone, portable web application for scraping Idealista property listings.

## Quick Start

1. Double-click `start.py` or run:
   ```
   python start.py
   ```

2. On first run, it will:
   - Download embedded Python (~15MB)
   - Install dependencies (~50MB)
   - Install Playwright browsers (~150MB)
   - This takes 2-3 minutes with a good connection

3. Your browser will open automatically to `http://localhost:5000`

## Usage

1. Enter an Idealista search URL (e.g., `https://www.idealista.com/alquiler-viviendas/madrid/`)
2. Select mode:
   - **Stealth**: Slower, human-like delays (recommended)
   - **Fast**: Minimal delays, higher detection risk
3. Click **Start** to begin scraping
4. Use **Pause/Resume** to temporarily stop
5. Click **Stop** to end and export data
6. Download the Excel file when complete

## Features

- 🎯 Real-time scraping progress
- 📊 Live results table with 40+ fields
- 📋 Color-coded logs
- ⏸️ Pause/Resume functionality
- 📥 Excel export with automatic naming
- 🔄 Smart deduplication (skips unchanged properties)

## Requirements

- Windows 10/11
- Internet connection (for first run setup and scraping)
- ~300MB disk space

## Portability

Copy the entire folder to any Windows PC and run `start.py`. No Python installation required.
