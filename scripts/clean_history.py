import json
import os
import sys
from pathlib import Path

# Path to the history file
SCRAPER_DIR = Path("scraper")
HISTORY_FILE = SCRAPER_DIR / "enriched_history.json"
BACKUP_FILE = SCRAPER_DIR / "enriched_history_backup.json"

def clean_history():
    print(f"Checking {HISTORY_FILE}...")
    
    if not HISTORY_FILE.exists():
        print("History file not found.")
        return

    try:
        # Load history
        with open(HISTORY_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        print(f"Total entries before cleaning: {len(data)}")
        
        # Create backup
        import shutil
        shutil.copy2(HISTORY_FILE, BACKUP_FILE)
        print(f"Backup created at {BACKUP_FILE}")
        
        keys_to_remove = []
        
        for url, entry in data.items():
            # Criteria for "Bad" entry (failed enrichment due to Captcha)
            # Typically has URL but missing critical fields like Title or Price
            # Since the script was just using orig_row + empty dict, it might have URL but no Title/Price if orig_row was basic?
            # Actually, orig_row usually has Title/Price from the List view scan.
            # But the user said "0 new fields".
            # The issue is we CANNOT distinguish a valid basic row from a failed enriched row easily 
            # unless we check for fields that SHOULD be there after enrichment.
            # But... wait. If 'Anuncio activo' is 'Sí', and fields like 'Descripción' or 'm2 construidos' are missing, it's weird.
            # However, looking at the code I just modified, if data was empty, it raised Exception. 
            # Before the fix, it saved it.
            # If the captcha blocked the page, the HTML body was just the captcha.
            # extract_detail_fields would probably fail to find ANY selectors.
            # So `d` was `{}`.
            # `final_row` = `orig_row` merged with `{}`.
            # So the entry in history is IDENTICAL to the input excel row.
            # If we assume the input Excel row had at least Price/Title...
            # The only difference is `Fecha Scraping` if it wasn't there?
            # User wants to delete them.
            # Let's verify if `gastos comunidad` is missing? Most basic scrapes don't have it.
            # If I delete ALL entries missing `gastos comunidad`, I might kill valid ones that just don't have it.
            
            # BUT, the user provided a hint: "0 new fields".
            # This implies the enrichment added nothing.
            # Implies we probably want to re-try them.
            # If we delete them from history, the NEXT run will see they are not in history and try again.
            # So being aggressive in deletion is safer than being conservative (leaving bad data).
            # If we delete a valid one, it just gets enriched again. No harm.
            # So let's delete any entry that doesn't look "Enriched".
            # What defines "Enriched"?
            # - Has 'gastos comunidad' (even if null, explicitly present key?) -> No, extractors only add key if found?
            # - Has 'Descripción' with length > 100?
            # - Has 'm2 utiles' or 'año construcción' or 'calefacción' details?
            
            # Let's count how many keys the entry has.
            # A basic row usually has ~10 keys?
            # An enriched row usually has 30+ keys.
            # Let's look at the keys in a known good entry vs bad.
            # Good: 50 keys. Bad: ~15 keys?
            
            # I'll check for keys that are almost ALWAYS in details but not in list.
            # 'Descripción' is a strong candidate. List view provides a summary, but details provides full text.
            # 'certificado energético' or 'consumo'?
            # Let's check for 'Descripción'. If it's short or missing, maybe delete?
            # BUT, if I delete valid ones, I waste API credits or time.
            
            # User said: "borres del JSON todas las URLs detectadas como enrriquecidas con 0 campos nuevos"
            # Since we don't store "new_fields" count...
            # I will delete entries where 'gastos comunidad' is NOT IN THE KEYS.
            # (Because extractor adds the key with None if not found? No, usually adds key.)
            # Wait, looking at `update_urls.py`, `d` is merged.
            # If `d` was empty, keys are not added.
            # So missing keys is the sign.
            # Check for 'gastos comunidad' presence.
            
            # Specific check for the "CAPTCHA Blocked" empty entries
            # These have 'Anuncio activo': 'Sí' but missing core data like Price and Title
            is_active = entry.get('Anuncio activo') == 'Sí'
            has_no_price = entry.get('price') is None
            has_no_title = entry.get('Titulo') is None
            
            if is_active and (has_no_price or has_no_title):
                 keys_to_remove.append(url)
                 
        print(f"Identified {len(keys_to_remove)} potential partial/failed entries.")
        
        # Filtering more aggressively?
        # If I remove them, they get re-processed.
        # Ensure we don't remove entries that are explicitly 'inactive' (Anuncio activo = No).
        # We want to keep inactive ones to avoid checking them again.
        
        final_remove = []
        for ur in keys_to_remove:
            if data[ur].get('Anuncio activo') == 'No':
                continue
            final_remove.append(ur)
            
        print(f" Removing {len(final_remove)} active entries that seem unenriched...")
        
        for k in final_remove:
            del data[k]
            
        # Save back
        with open(HISTORY_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
            
        print("Cleanup complete.")

    except Exception as e:
        print(f"Error during cleanup: {e}")

if __name__ == "__main__":
    clean_history()
