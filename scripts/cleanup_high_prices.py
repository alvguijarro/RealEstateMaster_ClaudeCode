import pandas as pd
import os
from pathlib import Path

def cleanup_high_prices(directory, price_limit=300000):
    output_dir = Path(directory)
    if not output_dir.exists():
        print(f"Directory not found: {directory}")
        return

    excel_files = list(output_dir.glob("*.xlsx"))
    print(f"Found {len(excel_files)} Excel files in {directory}")

    total_removed = 0
    files_modified = 0

    for file_path in excel_files:
        try:
            # Skip temporary or backup files
            if file_path.name.startswith("~$") or "backup" in file_path.name.lower():
                continue

            print(f"Processing {file_path.name}...")
            df = pd.read_excel(file_path)

            if "price" not in df.columns:
                print(f"  Warning: 'price' column not found in {file_path.name}. Skipping.")
                continue

            # Ensure price is numeric
            df['price'] = pd.to_numeric(df['price'], errors='coerce')
            
            # Identify rows to keep
            original_count = len(df)
            df_filtered = df[df['price'] <= price_limit].copy()
            new_count = len(df_filtered)
            removed = original_count - new_count

            if removed > 0:
                print(f"  Removed {removed} records with price > {price_limit}€")
                df_filtered.to_excel(file_path, index=False)
                total_removed += removed
                files_modified += 1
            else:
                print("  No records found exceeding the limit.")

        except Exception as e:
            print(f"  Error processing {file_path.name}: {e}")

    print("\n--- Summary ---")
    print(f"Files processed: {len(excel_files)}")
    print(f"Files modified: {files_modified}")
    print(f"Total records removed: {total_removed}")

if __name__ == "__main__":
    salidas_path = r"c:\Users\alvgu\.gemini\antigravity\playground\RealEstateMaster\scraper\salidas"
    cleanup_high_prices(salidas_path)
