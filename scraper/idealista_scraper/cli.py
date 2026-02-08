"""Command-line interface for the Idealista scraper.

This module provides the main entry point for running the scraper from the command line.
It handles argument parsing and session initialization.
"""
from __future__ import annotations

import argparse
import asyncio
import sys

from .utils import log, same_domain
from .scraper import ScraperSession


def parse_args():
    """Parse command-line arguments.
    
    Returns:
        argparse.Namespace: Parsed arguments with out, sheet, debug_items, and seed fields
    """
        p = argparse.ArgumentParser(description="Idealista scraper (press 'q' then ENTER to stop)")
    p.add_argument(
        "--out", 
        default="idealista.xlsx", 
        help="Output Excel file path (default: idealista.xlsx, auto-prefixed with province if detected)"
    )
    p.add_argument(
        "--sheet", 
        default="idealista", 
        help="Default sheet name, used only if listing type (alquiler/venta) not auto-detected (default: idealista)"
    )
    p.add_argument(
        "--debug-items", 
        action="store_true", 
        help="Print raw feature chips and flag values per property page for debugging"
    )
    p.add_argument(
        "--seed", 
        default="", 
        help="Seed Idealista search URL (if omitted, you will be prompted interactively)"
    )
    return p.parse_args()


def main():
    """Main entry point for the CLI scraper.
    
    Handles:
    1. Argument parsing
    2. URL validation
    3. Scraper session initialization and execution
    """
    args = parse_args()

    try:
        # Get seed URL (from argument or prompt user)
        seed = args.seed or input("Enter your seed page on idealista.com: ").strip()
        
        # Validate URL
        if not seed or not same_domain(seed):
            log("ERR", "Please provide a valid idealista.com URL.")
            log("ERR", "Example: https://www.idealista.com/alquiler-viviendas/madrid-madrid/")
            sys.exit(1)

        log("INFO", "Starting Idealista scraper...")
        log("INFO", "A browser window will open. Press 'q' then ENTER to stop and save.")
        
        # Initialize and run scraper session
        session = ScraperSession(
            cdp_endpoint="",  # Not used anymore
            out_xlsx=args.out,
            sheet_name=args.sheet,
            debug_items=args.debug_items,
            seed_url=seed,
        )
        asyncio.run(session.listen_and_collect())
        
    except KeyboardInterrupt:
        print()
        log("INFO", "Stopped by user (Ctrl+C).")
        print("[STATUS] stopped")
    except Exception as e:
        log("ERR", f"Unexpected error: {e}")
        print(f"[STATUS] error: {str(e)}")
        raise
