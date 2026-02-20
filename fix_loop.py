
import os
import re

path = r'c:\Users\alvgu\.gemini\antigravity\playground\RealEstateMaster\scraper\update_urls.py'
with open(path, 'r', encoding='utf-8') as f:
    content = f.read()

# Target block to replace
target = """                                    # Check data integrity - if empty, we might be blocked or page failed to load
                                    if not d or (not d.get('Titulo') and not d.get('price')):
                                         # If page loaded but no title/price, it's likely a captcha we missed or a broken page
                                         # Let's check captcha one more time
                                         if await detect_captcha(page):
                                              raise BlockedException("Hidden CAPTCHA detected")
                                         else:
                                              # If really no data, maybe it's just a failure? 
                                              # But we shouldn't save it as "Active" with empty data.
                                              # If active=No (because extractor detected it, e.g. "anuncio desactivado"), that's fine.
                                              # But if simply empty, we check for "No encontramos lo que estás buscando"
                                              page_not_found_text = await page.evaluate("() => document.body ? document.body.innerText : ''")
                                              if "No encontramos lo que estás buscando" in page_not_found_text or "el anuncio ya no está en nuestra base de datos" in page_not_found_text:
                                                   # Explicitly not found = Inactive
                                                   if not d: d = {}
                                                   d['Anuncio activo'] = 'No'
                                                   d['Baja anuncio'] = 'desconocida'
                                              else:
                                                   # Raise exception to trigger retry or skip without saving bad data.
                                                 pass
                                              raise Exception("Extraction returned empty data (Title/Price missing)")"""

# New content (with correct indentation)
replacement = """                                    # Check data integrity - if empty, we might be blocked or page failed to load
                                    if not d or (not d.get('Titulo') and not d.get('price')):
                                         # Check if inactive or blocked
                                         emit_to_ui('INFO', f'Empty data for {url}. Checking for block...')
                                         await asyncio.sleep(2) 
                                         block_status = await detect_captcha(page)
                                         if block_status == "block": 
                                             raise BlockedException("Hard Block detected during extraction")
                                         if block_status == "captcha": 
                                             break 
                                         
                                         page_text = await page.evaluate("document.body.innerText")
                                         if "No encontramos lo que estás buscando" in page_text or "ya no está en nuestra base de datos" in page_text:
                                              if not d: d = {}
                                              d['Anuncio activo'] = 'No'
                                              d['Baja anuncio'] = 'desconocida'
                                              break
                                         raise Exception("Extraction empty (Title/Price missing)")"""

# Note: The target string in my script must exactly match the file content.
# Since I just did a replacement of line 890 with 'pass', I need to be careful.

fixed_content = content.replace(target, replacement)

if fixed_content == content:
    print("FAILED to replace. Content remains identical.")
    # Try a more fuzzy match or regex if needed
else:
    with open(path, 'w', encoding='utf-8') as f:
        f.write(fixed_content)
    print("SUCCESSFULLY replaced the extraction block.")
