import sys
import os
from pathlib import Path
import json

# Setup path to import app.server
sys.path.append(os.path.join(os.getcwd(), 'scraper'))

from app.server import expand_batch_urls

def test():
    urls = [
        "https://www.idealista.com/venta-viviendas/a-coruna-provincia/",
        "https://www.idealista.com/alquiler-viviendas/albacete-provincia/"
    ]
    
    print("Testing expansion...")
    expanded = expand_batch_urls(urls)
    
    print(f"Original count: {len(urls)}")
    print(f"Expanded count: {len(expanded)}")
    
    if len(expanded) > len(urls):
        print("SUCCESS: Expansion worked!")
        print("Sample:", expanded[:3])
    else:
        print("FAILURE: No expansion occurred.")

if __name__ == "__main__":
    test()
