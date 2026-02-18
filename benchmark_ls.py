
import os
import time
from pathlib import Path

# Path to scraper salidas (INPUTS)
SALIDAS_DIR = os.path.join("scraper", "salidas")

print(f"Benchmarking directory: \n{os.path.abspath(SALIDAS_DIR)}\n")

# --- Benchmark 1: os.listdir + stat ---
start = time.time()
try:
    files = os.listdir(SALIDAS_DIR)
    count = 0
    for f in files:
        if f.endswith(".xlsx"):
            full_path = os.path.join(SALIDAS_DIR, f)
            mtime = os.path.getmtime(full_path)
            count += 1
    end = time.time()
    print(f"1. os.listdir + getmtime: {count} files in {end - start:.4f} seconds")
except Exception as e:
    print(f"1. Error: {e}")

# --- Benchmark 2: os.scandir (current impl) ---
start = time.time()
try:
    count = 0
    with os.scandir(SALIDAS_DIR) as entries:
        for entry in entries:
            if entry.is_file() and entry.name.endswith(".xlsx"):
                mtime = entry.stat().st_mtime
                count += 1
    end = time.time()
    print(f"2. os.scandir + stat:     {count} files in {end - start:.4f} seconds")
except Exception as e:
    print(f"2. Error: {e}")

# --- Benchmark 3: glob.glob ---
start = time.time()
try:
    import glob
    files = glob.glob(os.path.join(SALIDAS_DIR, "*.xlsx"))
    count = 0
    for f in files:
        mtime = os.path.getmtime(f)
        count += 1
    end = time.time()
    print(f"3. glob.glob + getmtime:  {count} files in {end - start:.4f} seconds")
except Exception as e:
    print(f"3. Error: {e}")
