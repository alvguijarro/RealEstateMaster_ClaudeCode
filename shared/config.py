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
TRENDS_PORT = 5005     # Market Trends Tracker

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
GOOGLE_API_KEY = os.environ.get("GOOGLE_API_KEY", "AIzaSyB5g2kv8fP4HEnPdhc8megfWQM4TFIp8Oc")

# CapSolver API Key (DataDome solver; empty = disabled)
CAPSOLVER_API_KEY = os.environ.get("CAPSOLVER_API_KEY", "CAP-80466E39600EB27CBE3C64207EF3702BEE5F7662B71FCF0323FD4045AA753463")

# =============================================================================
# API FILTERS (Default constraints)
# =============================================================================
# Maximum price for property downloads (in EUR). Set to None for no limit.
API_MAX_PRICE = 300000  # User request: limit to 300k EUR

# =============================================================================
# SCRAPER CONFIGURATION
# =============================================================================
DEFAULT_OUTPUT_DIR = str(OUTPUT_DIR)

# =============================================================================
# NOTIFICACIONES EMAIL (Trends Tracker)
# =============================================================================
SMTP_HOST     = os.environ.get("SMTP_HOST", "")
SMTP_PORT     = int(os.environ.get("SMTP_PORT", "587"))
SMTP_USER     = os.environ.get("SMTP_USER", "")
SMTP_PASSWORD = os.environ.get("SMTP_PASSWORD", "")
SMTP_FROM     = os.environ.get("SMTP_FROM", os.environ.get("SMTP_USER", ""))
SMTP_TO       = os.environ.get("SMTP_TO", "")   # Puede ser lista separada por comas
SMTP_ENABLED  = bool(SMTP_HOST and SMTP_USER and SMTP_TO)
