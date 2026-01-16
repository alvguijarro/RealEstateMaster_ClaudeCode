
from urllib.parse import urlsplit, urlunsplit
import re

def build_paginated_url(seed_url: str, page_number: int) -> str:
    """Build paginated URL from seed URL."""
    parts = urlsplit(seed_url)
    if page_number <= 1:
        return seed_url
    path = parts.path
    is_areas = "/areas/" in path
    if is_areas:
        base_path = re.sub(r"/pagina-\d+/?$", "", path)
        if not base_path.endswith("/"):
            base_path += "/"
        new_path = f"{base_path}pagina-{page_number}"
        return urlunsplit((parts.scheme, parts.netloc, new_path, parts.query, parts.fragment))
    else:
        base_path = re.sub(r"/pagina-\d+\.htm$", "", path)
        if not base_path.endswith("/"):
            base_path += "/"
        new_path = f"{base_path}pagina-{page_number}.htm"
        return urlunsplit((parts.scheme, parts.netloc, new_path, "", parts.fragment))

def canonical_listing_url(u: str) -> str:
    m = re.search(r"(https?://[^/]+)/(?:[a-z]{2}/)?(inmueble[s]?/\d+/?)", u, flags=re.I)
    return f"{m.group(1)}/{m.group(2)}" if m else u

# Test Case from User Logs
# Note: User provided URL from logs:
# https://www.idealista.com/areas/venta-viviendas/pagina-72?shape=%28%28qv_uFpndWi_DejMkoFqjOnmAseWghOshPcjFgz%5ErvVzrBj%7BTnrn%40s%40fvk%40yzJrbG%29%29

seed_url_71 = "https://www.idealista.com/areas/venta-viviendas/pagina-71?shape=%28%28qv_uFpndWi_DejMkoFqjOnmAseWghOshPcjFgz%5ErvVzrBj%7BTnrn%40s%40fvk%40yzJrbG%29%29"
seed_url_72 = "https://www.idealista.com/areas/venta-viviendas/pagina-72?shape=%28%28qv_uFpndWi_DejMkoFqjOnmAseWghOshPcjFgz%5ErvVzrBj%7BTnrn%40s%40fvk%40yzJrbG%29%29"

print(f"Testing Page 72 -> 73:")
next_url = build_paginated_url(seed_url_72, 73)
print(f"Result: {next_url}")

expected = "https://www.idealista.com/areas/venta-viviendas/pagina-73?shape=%28%28qv_uFpndWi_DejMkoFqjOnmAseWghOshPcjFgz%5ErvVzrBj%7BTnrn%40s%40fvk%40yzJrbG%29%29"
if next_url == expected:
    print("SUCCESS: URL built correctly")
else:
    print(f"FAILURE: Expected \n{expected}\nGot \n{next_url}")

print("\nTesting Canonical URL:")
u1 = "https://www.idealista.com/inmueble/12345/"
u2 = "https://www.idealista.com/en/inmueble/12345"
u3 = "https://www.idealista.com/inmueble/12345?xtor=..."
print(f"U1: {canonical_listing_url(u1)}")
print(f"U2: {canonical_listing_url(u2)}")
print(f"U3: {canonical_listing_url(u3)}")
