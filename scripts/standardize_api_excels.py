"""
Standardize API-downloaded Excel files to match the scraper's column structure.

Detection: A file is considered API-derived if:
  1. It has a column called "exterior"
  2. It does NOT have "__enriched__" column
  3. It does NOT have "Fecha Enriquecimiento" column

Actions:
  - Reorder columns to match ORDERED_BASE (which now includes "exterior")
  - Add empty "__enriched__" and "Fecha Enriquecimiento" columns
  - Create a backup before overwriting

Usage:
    python standardize_api_excels.py [--dry-run]
"""

import os
import sys
import shutil
import argparse
from pathlib import Path
from datetime import datetime

import pandas as pd

# Force UTF-8 for Windows console
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))
from scraper.idealista_scraper import ORDERED_BASE

SALIDAS_DIR = Path(__file__).parent.parent / "scraper" / "salidas"
BACKUP_DIR = SALIDAS_DIR / "old" / "pre_standardization"

# Enrichment columns that API files are missing
ENRICHMENT_COLS = ["Fecha Enriquecimiento", "__enriched__"]


def is_api_file(filepath: str) -> bool:
    """Detect if an Excel file has API structure (not scraper structure)."""
    try:
        # Read only headers (nrows=0) for speed
        df = pd.read_excel(filepath, nrows=0)
        cols = set(df.columns)

        has_exterior = "exterior" in cols
        missing_enriched = "__enriched__" not in cols
        missing_fecha_enrich = "Fecha Enriquecimiento" not in cols

        return has_exterior and missing_enriched and missing_fecha_enrich
    except Exception:
        return False


def standardize_file(filepath: str, dry_run: bool = False) -> bool:
    """Standardize a single API-structured Excel file.

    Returns True on success, False on error.
    """
    basename = os.path.basename(filepath)

    try:
        # Load ALL sheets
        xl = pd.ExcelFile(filepath)
        sheet_names = xl.sheet_names

        if dry_run:
            print(f"  [DRY-RUN] Would standardize: {basename} ({len(sheet_names)} sheets)")
            return True

        # Create backup
        BACKUP_DIR.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_name = f"{Path(basename).stem}_backup_{ts}.xlsx"
        backup_path = BACKUP_DIR / backup_name
        shutil.copy2(filepath, backup_path)
        print(f"  Backup created: {backup_name}")

        # Process each sheet
        all_dfs = {}
        for sheet in sheet_names:
            df = pd.read_excel(filepath, sheet_name=sheet)

            # Add enrichment columns if missing
            for col in ENRICHMENT_COLS:
                if col not in df.columns:
                    df[col] = None

            # Build final column order: ORDERED_BASE first, then any extras
            ordered = list(ORDERED_BASE)
            extra = [c for c in df.columns if c not in ordered]
            final_order = ordered + sorted(extra)

            # Add any ORDERED_BASE columns that are completely missing
            for col in final_order:
                if col not in df.columns:
                    df[col] = None

            # Reorder
            df = df[final_order]
            all_dfs[sheet] = df

        # Write back
        with pd.ExcelWriter(filepath, engine="openpyxl") as writer:
            for sheet, df in all_dfs.items():
                df.to_excel(writer, sheet_name=sheet, index=False)

        print(f"  ✅ Standardized: {basename} ({len(sheet_names)} sheets)")
        return True

    except PermissionError:
        print(f"  ❌ SKIPPED (file open?): {basename}")
        return False
    except Exception as e:
        print(f"  ❌ ERROR: {basename} -> {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description="Standardize API Excel files")
    parser.add_argument("--dry-run", action="store_true", help="Only list files, don't modify")
    args = parser.parse_args()

    if not SALIDAS_DIR.exists():
        print(f"ERROR: Directory not found: {SALIDAS_DIR}")
        sys.exit(1)

    # Scan for xlsx files
    xlsx_files = sorted(SALIDAS_DIR.glob("*.xlsx"))
    print(f"Scanning {len(xlsx_files)} Excel files in {SALIDAS_DIR}...")
    print(f"ORDERED_BASE has {len(ORDERED_BASE)} columns (includes 'exterior')")
    print()

    api_files = []
    scraper_files = []

    for f in xlsx_files:
        # Skip temp files
        if f.name.startswith("~$"):
            continue
        if is_api_file(str(f)):
            api_files.append(f)
        else:
            scraper_files.append(f)

    print(f"Found {len(api_files)} API-structured files to standardize:")
    for f in api_files:
        print(f"  > {f.name}")

    print(f"\nFound {len(scraper_files)} already-standard (scraper) files:")
    for f in scraper_files:
        print(f"  OK {f.name}")

    if not api_files:
        print("\nNo API files found. Nothing to do.")
        return

    print(f"\n{'=' * 60}")
    print(f"Processing {len(api_files)} files...")
    print(f"{'=' * 60}")

    success = 0
    failed = 0
    for f in api_files:
        result = standardize_file(str(f), dry_run=args.dry_run)
        if result:
            success += 1
        else:
            failed += 1

    print(f"\n{'=' * 60}")
    print(f"Done! Success: {success}, Failed: {failed}")
    if args.dry_run:
        print("(Dry run - no files were modified)")


if __name__ == "__main__":
    main()
