"""Centralized Configuration for RealEstateMaster.

This module provides shared constants and configuration across all services.
"""
import os
from pathlib import Path

# =============================================================================
# PROJECT PATHS
# =============================================================================
PROJECT_ROOT = Path(__file__).parent.parent.resolve()
SCRAPER_DIR = PROJECT_ROOT / "scraper"
ANALYZER_DIR = PROJECT_ROOT / "analyzer"
DASHBOARD_DIR = PROJECT_ROOT / "dashboard"
MERGER_DIR = PROJECT_ROOT / "merger"
OUTPUT_DIR = SCRAPER_DIR / "salidas"

# =============================================================================
# SERVICE PORTS
# =============================================================================
DASHBOARD_PORT = 5000  # Main launcher/unified dashboard
ANALYZER_PORT = 5001   # Analyzer Pro service
MERGER_PORT = 5002     # Merger Tool service
SCRAPER_PORT = 5003    # Idealista Scraper service
METRICS_PORT = 5004    # Market Metrics Dashboard

# =============================================================================
# API CONFIGURATION
# =============================================================================
RAPIDAPI_HOST = "idealista7.p.rapidapi.com"
# API Key loaded from environment variable with fallback
RAPIDAPI_KEY = os.environ.get(
    "RAPIDAPI_KEY",
    "0f45666904mshbae4e59c6a93975p1c04c7jsn7c1c3240e93d"  # Default fallback
)

# Google Gemini API Key
GOOGLE_API_KEY = os.environ.get("GOOGLE_API_KEY", "AIzaSyC7IGitg94xGP_ojTEbcnW9sHa24C1tFNM")

# =============================================================================
# API FILTERS (Default constraints)
# =============================================================================
# Maximum price for property downloads (in EUR). Set to None for no limit.
API_MAX_PRICE = 300000  # User request: limit to 300k EUR

# =============================================================================
# SCRAPER CONFIGURATION
# =============================================================================
DEFAULT_OUTPUT_DIR = str(OUTPUT_DIR)
