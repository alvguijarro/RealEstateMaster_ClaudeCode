
import os
import re

path = r'c:\Users\alvgu\.gemini\antigravity\playground\RealEstateMaster\scraper\update_urls.py'
with open(path, 'r', encoding='utf-8') as f:
    content = f.read()

# I will find the extraction loop by distinct markers
start_marker = "# Extract"
end_marker = "# --- Data Merging & Logging ---"

# Let's use a more precise regex to find the problematic block
# We want to replace everything from "d = None" until the merging starts.

new_extraction_block = """                            # Extract
                            d = None
                            for attempt in range(3):
                                try:
                                    d = await extract_detail_fields(page, debug_items=False)
                                    if d and d.get('isBlocked'):
                                        raise BlockedException("Uso Indebido detected (via extractor)")
                                    
                                    # Check data integrity - if empty, we might be blocked or page failed to load
                                    if not d or (not d.get('Titulo') and not d.get('price')):
                                         # Check if inactive or blocked
                                         emit_to_ui('INFO', f'Empty data for {url}. Checking for block...')
                                         await asyncio.sleep(2) # Give it a moment to settle
                                         block_status = await detect_captcha(page)
                                         if block_status == "block": 
                                             raise BlockedException("Hard Block detected during extraction")
                                         if block_status == "captcha": 
                                             break # Deal with captcha later
                                         
                                         page_text = await page.evaluate("document.body.innerText")
                                         if "No encontramos lo que estás buscando" in page_text or "ya no está en nuestra base de datos" in page_text:
                                              if not d: d = {}
                                              d['Anuncio activo'] = 'No'
                                              d['Baja anuncio'] = 'desconocida'
                                              break
                                         
                                         # Look for explicit block strings in body
                                         if any(kw in page_text.lower() for kw in ["uso indebido", "bloqueado", "peticiones"]):
                                              raise BlockedException("Undetected block keywords found in page body")
                                              
                                         raise Exception("Extraction empty (Title/Price missing)")
                                    
                                    break
                                except BlockedException:
                                    raise 
                                except Exception as e:
                                    if "Execution context was destroyed" in str(e) and attempt < 2:
                                        await asyncio.sleep(1)
                                        continue
                                    raise e
                            
                            # Final block/captcha check
                            block_status = await detect_captcha(page)
                            if block_status == "block":
                                raise BlockedException("Hard Block detected")
                                
                            if block_status == "captcha":
                                captchas_found += 1
                                emit_to_ui('WARN', f'({i}/{len(urls)}) CAPTCHA detectado.')
                                emit_to_ui('INFO', 'Intentando resolver CAPTCHA automáticamente...')
                                if await solve_captcha_advanced(page):
                                     if await detect_captcha(page) is None:
                                          captchas_solved += 1
                                          emit_to_ui('OK', 'CAPTCHA resuelto automáticamente!')
                                          d = await extract_detail_fields(page, debug_items=False)
                                
                                if await detect_captcha(page) == "captcha":
                                    emit_to_ui('WARN', 'Resuelve el CAPTCHA manualmente en el navegador.')
                                    while await detect_captcha(page) == "captcha":
                                        play_captcha_alert()
                                        await asyncio.sleep(5)
                                    d = await extract_detail_fields(page, debug_items=False)
                            
"""

# We'll use a regex to replace the old block between markers
# Old block starts at "d = None" and ends before "# --- Data Merging & Logging ---"
# We'll use re.DOTALL and search for a pattern that matches the messy code.

# This regex is broad enough to catch the messy block I've seen in view_file.
pattern = r'# Extract\s+d = None\s+for attempt in range\(3\):.*?# --- Data Merging & Logging ---'
new_content = re.sub(pattern, start_marker + "\n" + new_extraction_block + "                            " + end_marker, content, flags=re.DOTALL)

if new_content == content:
    print("REPLACEMENT FAILED (Regex match not found)")
else:
    with open(path, 'w', encoding='utf-8') as f:
        f.write(new_content)
    print("REPLACEMENT SUCCESSFUL")
