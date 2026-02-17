"""Batch API Scan for Multiple Provinces/Locations.

This script iterates through a list of locations (ID and Name), fetches data via the API,
and exports each to a separate Excel file. It can be run periodically.
"""
import sys
import os
import time
import argparse
from datetime import datetime
from pathlib import Path

# Add project root to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from scraper.idealista_scraper.api_client import fetch_data_generator
from scraper.idealista_scraper.excel_writer import export_split_by_distrito
from scraper.idealista_scraper.utils import sanitize_filename_part, log
from scraper.idealista_scraper.nordvpn import rotate_ip
from scraper.app.server import add_history_entry, DEFAULT_OUTPUT_DIR
import pandas as pd

# Define your locations here or load from a file
# Format: {"id": "LOCATION_ID", "name": "Location Name"}
PROVINCES_TO_SCAN = [
    # Full list of Spanish Provinces (INE Codes)
    {"id": "0-EU-ES-01", "name": "Alava"},
    {"id": "0-EU-ES-02", "name": "Albacete"},
    {"id": "0-EU-ES-03", "name": "Alicante"},
    {"id": "0-EU-ES-04", "name": "Almeria"},
    {"id": "0-EU-ES-05", "name": "Avila"},
    {"id": "0-EU-ES-06", "name": "Badajoz"},
    {"id": "0-EU-ES-07", "name": "Baleares"},
    {"id": "0-EU-ES-08", "name": "Barcelona"},
    {"id": "0-EU-ES-09", "name": "Burgos"},
    {"id": "0-EU-ES-10", "name": "Caceres"},
    {"id": "0-EU-ES-11", "name": "Cadiz"},
    {"id": "0-EU-ES-12", "name": "Castellon"},
    {"id": "0-EU-ES-13", "name": "Ciudad Real"},
    {"id": "0-EU-ES-14", "name": "Cordoba"},
    {"id": "0-EU-ES-15", "name": "A Coruna"},
    {"id": "0-EU-ES-16", "name": "Cuenca"},
    {"id": "0-EU-ES-17", "name": "Girona"},
    {"id": "0-EU-ES-18", "name": "Granada"},
    {"id": "0-EU-ES-19", "name": "Guadalajara"},
    {"id": "0-EU-ES-20", "name": "Guipuzcoa"},
    {"id": "0-EU-ES-21", "name": "Huelva"},
    {"id": "0-EU-ES-22", "name": "Huesca"},
    {"id": "0-EU-ES-23", "name": "Jaen"},
    {"id": "0-EU-ES-24", "name": "Leon"},
    {"id": "0-EU-ES-25", "name": "Lleida"},
    {"id": "0-EU-ES-26", "name": "La Rioja"},
    {"id": "0-EU-ES-27", "name": "Lugo"},
    {"id": "0-EU-ES-28", "name": "Madrid"},
    {"id": "0-EU-ES-29", "name": "Malaga"},
    {"id": "0-EU-ES-30", "name": "Murcia"},
    {"id": "0-EU-ES-31", "name": "Navarra"},
    {"id": "0-EU-ES-32", "name": "Ourense"},
    {"id": "0-EU-ES-33", "name": "Asturias"},
    {"id": "0-EU-ES-34", "name": "Palencia"},
    {"id": "0-EU-ES-35", "name": "Las Palmas"},
    {"id": "0-EU-ES-36", "name": "Pontevedra"},
    {"id": "0-EU-ES-37", "name": "Salamanca"},
    {"id": "0-EU-ES-38", "name": "Santa Cruz de Tenerife"},
    {"id": "0-EU-ES-39", "name": "Cantabria"},
    {"id": "0-EU-ES-40", "name": "Segovia"},
    {"id": "0-EU-ES-41", "name": "Sevilla"},
    {"id": "0-EU-ES-42", "name": "Soria"},
    {"id": "0-EU-ES-43", "name": "Tarragona"},
    {"id": "0-EU-ES-44", "name": "Teruel"},
    {"id": "0-EU-ES-45", "name": "Toledo"},
    {"id": "0-EU-ES-46", "name": "Valencia"},
    {"id": "0-EU-ES-47", "name": "Valladolid"},
    {"id": "0-EU-ES-48", "name": "Vizcaya"},
    {"id": "0-EU-ES-49", "name": "Zamora"},
    {"id": "0-EU-ES-50", "name": "Zaragoza"},
    {"id": "0-EU-ES-51", "name": "Ceuta"},
    {"id": "0-EU-ES-52", "name": "Melilla"},
]

# Signal flags
BATCH_STOP_FLAG = Path(__file__).parent.parent / "scraper" / "BATCH_STOP.flag"

def check_signals():
    """Check for stop flag."""
    if BATCH_STOP_FLAG.exists():
        log("WARN", "🛑 Stop signal detected. Exiting batch scan...")
        try: BATCH_STOP_FLAG.unlink()
        except: pass
        sys.exit(0)

def run_batch_scan(operation="rent", max_pages=50, delay_between=10, resume=False, use_vpn=False, rotate_every=5):
    """Run scanning process for all defined locations."""
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    log_file = Path(DEFAULT_OUTPUT_DIR) / f"batch_scan_log_{timestamp}.txt"
    
    # Statistics
    stats = {
        "processed": 0,
        "total_provinces": len(PROVINCES_TO_SCAN),
        "total_items": 0,
        "errors": 0,
        "successful_provinces": []
    }
    
    def file_log(level, msg):
        # Console output with color-like formatting (simple prefixes)
        print(f"[{level}] {msg}")
        try:
            with open(log_file, "a", encoding="utf-8") as f:
                f.write(f"[{level}] {datetime.now().isoformat()} - {msg}\n")
        except:
            pass

    file_log("INFO", f"Starting Batch Scan for {len(PROVINCES_TO_SCAN)} locations. Operation: {operation.upper()}")
    if resume:
        file_log("INFO", "RESUME MODE: ON - Will attempt to append to existing files.")
    
    start_time = time.time()

    for i, loc in enumerate(PROVINCES_TO_SCAN):
        check_signals()
        # NOTE: Periodic VPN rotation removed (2026-02-07)
        # VPN rotation now only happens when a block is detected in scraper_wrapper.py

        loc_id = loc["id"]
        loc_name = loc["name"]
        loc_clean = sanitize_filename_part(loc_name)
        
        # Resume Logic
        start_page = 1
        existing_df = pd.DataFrame()
        out_path = os.path.join(DEFAULT_OUTPUT_DIR, f"API_BATCH_{loc_clean}_{operation}_{timestamp}.xlsx") # Default new
        
        if resume:
            # Find latest file for this province/op
            files = list(Path(DEFAULT_OUTPUT_DIR).glob(f"API_BATCH_{loc_clean}_{operation}_*.xlsx"))
            if files:
                # Sort by mod time descending
                latest_file = max(files, key=lambda f: f.stat().st_mtime)
                try:
                    # Read all sheets (Distritos) and combine
                    sheets_dict = pd.read_excel(latest_file, sheet_name=None)
                    if sheets_dict:
                        existing_df = pd.concat(sheets_dict.values(), ignore_index=True)
                        count = len(existing_df)
                        
                        # Heuristic: If we have items, start from next page
                        # Assumes 40 per page. 
                        # E.g. 2000 items -> 50 pages done -> start 51
                        # E.g. 2020 items -> 51 pages done? (20 part of 51) -> start 51 to refill? 
                        # Safer: floor div. 2000 // 40 = 50. Start 51.
                        # 2010 // 40 = 50. remainder 10. Start 51 (to get rest of 51).
                        start_page = (count // 40) + 1
                        
                        out_path = str(latest_file) # Target the existing file
                        file_log("INFO", f"Resuming {loc_name} from Page {start_page} (Found {count} items in {latest_file.name})")
                except Exception as e:
                    file_log("WARN", f"Could not read resume file {latest_file.name}: {e}")

        # If start_page > max_pages, skip
        if start_page > max_pages:
             file_log("INFO", f"Skipping {loc_name}: Already has {len(existing_df)} items (>= max_pages {max_pages} limit)")
             # Add to stats as success
             stats["processed"] += 1
             stats["total_items"] += len(existing_df) if not existing_df.empty else 0
             continue

        # calculate progress
        progress_pct = ((i) / stats["total_provinces"]) * 100
        remaining_provinces = stats["total_provinces"] - i
        elapsed_time = time.time() - start_time
        avg_time_per_prov = elapsed_time / (i + 1) if i > 0 else 0
        est_remaining_time = avg_time_per_prov * remaining_provinces
        
        # Time formatting
        est_min = int(est_remaining_time // 60)
        est_sec = int(est_remaining_time % 60)
        
        file_log("INFO", f"🚀 [{i}/{stats['total_provinces']}] {loc_name} ({loc_id}) | Progress: {progress_pct:.1f}% | Est. Remaining: {est_min}m {est_sec}s")
        
        try:
            # Modified generator to accept start_page? No, we have to loop manually or modify generator.
            # Generator loop is: `range(1, max_pages + 1)`.
            # We need to skip `range(start_page, ...)`
            # Simple hack: Just loop here manually instead of strictly following generator's range
            # Actually, `fetch_data_generator` starts at 1. We need to modify it or just burn calls? No burning is bad.
            # Let's modify the CALL to pass start_page if we can, OR just loop `fetch_api_page` manually here? 
            # Re-implementing the loop here is safer than editing api_client again.
            
            all_rows = []
            
            # Using manual loop instead of generator to support start_page
            for p in range(start_page, max_pages + 1):
                check_signals()
                # file_log("INFO" if p % 5 == 0 else "DEBUG", f"Fetching page {p}...")
                print(f"\r   > Fetching page {p}/{max_pages}... Found {len(all_rows)} new items", end="")
                
                json_data = None
                # Import here to avoid circular dep issues if any, though likely fine
                from scraper.idealista_scraper.api_client import fetch_api_page, map_item_to_row
                
                json_data = fetch_api_page(p, loc_id, operation, location_name=loc_name)
                
                if not json_data or "error" in json_data:
                    err = json_data.get('message') if json_data else "No Data"
                    file_log("ERR", f"Error on page {p}: {err}")
                    break
                
                items = json_data.get('elementList', [])
                if not items:
                    # End of results
                    break
                    
                for item in items:
                    row = map_item_to_row(item)
                    all_rows.append(row)
                
                # Check actual API limit
                total_pages_api = json_data.get('totalPages', max_pages)
                if p >= total_pages_api:
                    break
                
                time.sleep(0.5)
            
            print("") # Newline
            
            if not all_rows and start_page == 1:
                file_log("WARN", f"No data found for {loc_name}")
            elif not all_rows and start_page > 1:
                file_log("INFO", f"No *new* data found (Resume matched end of list).")
            else:
                # Export
                # If we have existing_df (from resume), pass it to handle deduplication
                
                export_split_by_distrito(
                    existing_df=existing_df,
                    additions=all_rows,
                    out_path=out_path,
                    carry_cols=set()
                )
                
                total_count = len(existing_df) + len(all_rows) # approx, dedupe might reduce
                
                add_history_entry(
                    seed_url=f"BATCH:{loc_id}",
                    properties_count=len(all_rows), # New items
                    category=f"{loc_clean}_{operation}",
                    output_file=out_path
                )
                
                stats["total_items"] += len(all_rows)
                stats["successful_provinces"].append(loc_name)
                file_log("OK", f"SUCCESS: Saved {len(all_rows)} new items. Total in file: {total_count}")
            
        except Exception as e:
            stats["errors"] += 1
            file_log("ERR", f"FAILED processing {loc_name}: {e}")
            import traceback
            traceback.print_exc()
        
        stats["processed"] += 1
        
        # Delay between locations
        if i < len(PROVINCES_TO_SCAN) - 1:
            # print(f"   Waiting {delay_between}s...")
            time.sleep(delay_between)

    # Final Summary
    total_time = time.time() - start_time
    total_min = int(total_time // 60)
    total_sec = int(total_time % 60)
    
    file_log("INFO", f"===================================================")
    file_log("INFO", f"BATCH SCAN COMPLETED in {total_min}m {total_sec}s")
    file_log("INFO", f"Total Provinces: {stats['total_provinces']}")
    file_log("INFO", f"Total Items Fetched ({operation.upper()}): {stats['total_items']}")
    file_log("INFO", f"Errors Encountered: {stats['errors']}")
    file_log("INFO", f"Results Directory: {DEFAULT_OUTPUT_DIR}")
    file_log("INFO", f"===================================================")
    
    # input("\nPress Enter to close this window...")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run batch API scan.")
    parser.add_argument("--operation", default="rent", choices=["rent", "sale"], help="Operation type")
    parser.add_argument("--max-pages", type=int, default=50, help="Max pages per location")
    parser.add_argument("--filter", type=str, help="Filter provinces by name or ID (substring)")
    parser.add_argument("--provinces", type=str, help="Comma-separated list of province IDs or Names (exact matchish)")
    parser.add_argument("--resume", action="store_true", help="Resume from latest file if exists")
    parser.add_argument("--nordvpn", action="store_true", help="Use NordVPN to rotate IP periodically")
    parser.add_argument("--rotate-every", type=int, default=5, help="Rotate IP every N provinces")
    
    args = parser.parse_args()
    
    # Filter list if requested
    if args.filter:
        original_count = len(PROVINCES_TO_SCAN)
        term = args.filter.lower()
        PROVINCES_TO_SCAN = [
            p for p in PROVINCES_TO_SCAN 
            if term in p['name'].lower() or term in p['id']
        ]
        print(f"Filtered provinces from {original_count} to {len(PROVINCES_TO_SCAN)} matching '{args.filter}'")
    
    if args.provinces:
        original_count = len(PROVINCES_TO_SCAN)
        target_list = [t.strip().lower() for t in args.provinces.split(',')]
        
        filtered = []
        for p in PROVINCES_TO_SCAN:
            p_name = p['name'].lower()
            p_id = p['id'].lower()
            # Match if any target is in name OR equals ID
            if any(t == p_name or t == p_id or t in p_name for t in target_list):
                 filtered.append(p)
        
        PROVINCES_TO_SCAN = filtered
        print(f"Selected {len(PROVINCES_TO_SCAN)} provinces from list: {args.provinces}")

    run_batch_scan(operation=args.operation, max_pages=args.max_pages, resume=args.resume, use_vpn=args.nordvpn, rotate_every=args.rotate_every)
