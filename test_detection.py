import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent / "scraper"))
from scraper.app.province_mapping import detect_province_and_operation

test_urls = [
    "https://www.idealista.com/alquiler-viviendas/albacete/centro/",
    "https://www.idealista.com/venta-viviendas/madrid/moncloa/",
    "https://www.idealista.com/alquiler-viviendas/alicante/centro/",
    "https://www.idealista.com/venta-viviendas/a-coruna/centro/",
]

for url in test_urls:
    prov, op = detect_province_and_operation(url)
    print(f"URL: {url} -> Province: {prov}, Op: {op}")
