import pandas as pd
import os

file1 = r"C:\Users\alvgu\.gemini\antigravity\playground\RealEstateMaster\scraper\salidas\idealista_Madrid_venta - 1.xlsx"
file2 = r"C:\Users\alvgu\.gemini\antigravity\playground\RealEstateMaster\scraper\salidas\idealista_Madrid_venta - 2.xlsx"

def compare_excels(p1, p2):
    try:
        df1 = pd.concat(pd.read_excel(p1, sheet_name=None).values(), ignore_index=True)
        df2 = pd.concat(pd.read_excel(p2, sheet_name=None).values(), ignore_index=True)
        
        # Ensure URL column exists
        url_col = 'url' if 'url' in df1.columns else df1.columns[0]
        
        common_urls = set(df1[url_col]).intersection(set(df2[url_col]))
        
        examples = []
        for url in common_urls:
            row1 = df1[df1[url_col] == url].iloc[0]
            row2 = df2[df2[url_col] == url].iloc[0]
            
            # Count non-null fields
            count1 = row1.count()
            count2 = row2.count()
            
            if count1 != count2:
                # One is likely more enriched than the other
                enriched = row1 if count1 > count2 else row2
                basic = row2 if count1 > count2 else row1
                
                diffs = {}
                for col in enriched.index:
                    if col in basic.index:
                        if pd.isna(basic[col]) and pd.notna(enriched[col]):
                            diffs[col] = enriched[col]
                        elif basic[col] != enriched[col] and pd.notna(basic[col]) and pd.notna(enriched[col]):
                            # Check if it's just a slight formatting diff
                            if str(basic[col]).strip() != str(enriched[col]).strip():
                                diffs[col] = f"{basic[col]} -> {enriched[col]}"
                    else:
                        diffs[col] = enriched[col]
                
                if diffs:
                    examples.append({
                        'url': url,
                        'basic_count': count2 if count1 > count2 else count1,
                        'enriched_count': count1 if count1 > count2 else count2,
                        'diffs': diffs
                    })
            
            if len(examples) >= 5:
                break
        
        return examples
    except Exception as e:
        return str(e)

results = compare_excels(file1, file2)
import json
print(json.dumps(results, indent=2, default=str))
