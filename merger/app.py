import sys
import os
import time
from pathlib import Path

# Add project root to path for shared imports
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

import pandas as pd
from flask import Flask, render_template, jsonify, request

app = Flask(__name__, static_folder='static', template_folder='templates')

# Allow embedding in iframes and add CORS headers for polling
@app.after_request
def after_request(response):
    response.headers.pop('X-Frame-Options', None)
    # Add CORS headers for cross-origin polling from dashboard
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Methods'] = 'GET, POST, OPTIONS'
    response.headers['Access-Control-Allow-Headers'] = 'Content-Type'
    return response

@app.route('/health', methods=['GET', 'OPTIONS'])
def health_check():
    """Simple health check endpoint for service readiness polling."""
    return jsonify({'status': 'ok'})


@app.route('/')
def index():
    """Main merger view."""
    return render_template('merger.html', cache_bust=int(time.time()))


@app.route('/list-files')
def list_files():
    """List Excel files from scraper salidas directory."""
    # Point to scraper's salidas output directory
    salidas_dir = Path(__file__).parent.parent / "scraper" / "salidas"
    files = []
    if salidas_dir.exists():
        for f in salidas_dir.glob("*.xlsx"):
            # Simple heuristic for type
            f_lower = f.name.lower()
            ftype = 'unknown'
            if 'venta' in f_lower or 'sale' in f_lower:
                ftype = 'venta'
            elif 'alquiler' in f_lower or 'rent' in f_lower:
                ftype = 'alquiler'
            
            # Get modification time and file size
            mtime = os.path.getmtime(f)
            mtime_str = time.strftime('%Y-%m-%d %H:%M', time.localtime(mtime))
            size_bytes = os.path.getsize(f)
            
            files.append({
                'filename': f.name, 
                'type': ftype,
                'last_modified': mtime_str,
                'mtime': mtime,
                'size': size_bytes
            })
    return jsonify(files)


@app.route('/api/merge', methods=['POST'])
def merge_files():
    """Merge two Excel files and deduplicate based on URL column."""
    try:
        data = request.json
        file1_name = data.get('file1')
        file2_name = data.get('file2')

        if not file1_name or not file2_name:
            return jsonify({'error': 'Missing files'}), 400

        # Validate filenames safety (simple check)
        if '..' in file1_name or '..' in file2_name:
            return jsonify({'error': 'Invalid filenames'}), 400

        # Validate business rule: must contain 'alquiler'/'rent' or 'venta'/'sale'
        def is_valid_type(name):
            n = name.lower()
            return 'alquiler' in n or 'rent' in n or 'venta' in n or 'sale' in n
        
        def get_file_type(name):
            n = name.lower()
            if 'alquiler' in n or 'rent' in n:
                return 'rent'
            if 'venta' in n or 'sale' in n:
                return 'sale'
            return None

        if not is_valid_type(file1_name) or not is_valid_type(file2_name):
            return jsonify({'error': 'Los archivos deben contener "alquiler"/"rent" o "venta"/"sale"'}), 400
        
        # Check that both files are the same type
        type1 = get_file_type(file1_name)
        type2 = get_file_type(file2_name)
        if type1 != type2:
            return jsonify({'error': 'Los archivos seleccionados no contienen el mismo tipo de transacción (VENTA o ALQUILER)'}), 400

        # Paths - point to scraper's salidas directory
        salidas_dir = Path(__file__).parent.parent / "scraper" / "salidas"
        path1 = salidas_dir / file1_name
        path2 = salidas_dir / file2_name

        if not path1.exists() or not path2.exists():
            return jsonify({'error': 'One or more files not found'}), 404

        # Output filename
        f1_stem = Path(file1_name).stem
        f2_stem = Path(file2_name).stem
        output_name = f"{f1_stem} - {f2_stem}_MERGED.xlsx"
        output_path = salidas_dir / output_name

        # Perform Merge using Context Managers to ensure file handles are released
        processed_sheets = 0
        
        # Statistics tracking
        stats = {
            'file1': {'properties': 0, 'districts': set()},
            'file2': {'properties': 0, 'districts': set()},
            'merged': {'properties': 0, 'districts': set()},
            'duplicates_removed': 0
        }
        
        def count_districts(df):
            """Count unique districts from Distrito or similar column."""
            district_col = None
            for col in df.columns:
                if isinstance(col, str) and 'distrito' in col.lower():
                    district_col = col
                    break
            if district_col and district_col in df.columns:
                return set(df[district_col].dropna().unique())
            return set()
        
        # Use Context Managers for input files to prevent locking
        with pd.ExcelFile(path1) as xl1, pd.ExcelFile(path2) as xl2:
            sheet_names1 = xl1.sheet_names
            sheet_names2 = set(xl2.sheet_names)
            
            all_sheets = sorted(list(set(sheet_names1) | sheet_names2))
            
            # Open Writer context manager
            with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                for sheet in all_sheets:
                    df_combined = None
                    
                    if sheet in sheet_names1 and sheet in sheet_names2:
                        # Case 1: Sheet exists in BOTH files -> MERGE
                        df1 = pd.read_excel(xl1, sheet_name=sheet)
                        df2 = pd.read_excel(xl2, sheet_name=sheet)
                        
                        # Track stats for file 1
                        stats['file1']['properties'] += len(df1)
                        stats['file1']['districts'].update(count_districts(df1))
                        
                        # Track stats for file 2
                        stats['file2']['properties'] += len(df2)
                        stats['file2']['districts'].update(count_districts(df2))
                        
                        # Find URL column
                        url_col = None
                        for col in df1.columns:
                            if isinstance(col, str) and 'url' in col.lower():
                                url_col = col
                                break
                        
                        if not url_col:
                            # Fallback: concat without dedup
                            print(f"Warning: No URL column found in sheet {sheet}")
                            df_combined = pd.concat([df1, df2], ignore_index=True)
                        else:
                            # --- SMART MERGE LOGIC ---
                            # 1. Identify common URLs and unique rows
                            # Ensure URL column is string for matching
                            df1[url_col] = df1[url_col].astype(str)
                            df2[url_col] = df2[url_col].astype(str)

                            # Create dictionaries for fast lookup by URL
                            # Orient 'index' makes dict of {index: row_dict}? No, 'records' is list of dicts.
                            # We need {url: row_series/dict}
                            
                            # Convert to list of records for easier manipulation
                            records1 = df1.to_dict('records')
                            records2 = df2.to_dict('records')
                            
                            map1 = {str(r.get(url_col)): r for r in records1 if r.get(url_col)}
                            map2 = {str(r.get(url_col)): r for r in records2 if r.get(url_col)}
                            
                            merged_records = []
                            processed_urls = set()
                            
                            # Fields to update from File 2 if present
                            # Note: Column names might vary in case (e.g. 'Ciudad' vs 'ciudad'). 
                            # We should try to be case-insensitive or stick to provided names.
                            # User said: "exterior", "Ciudad", "Fecha scraping"
                            update_fields = ['exterior', 'Ciudad', 'Fecha scraping', 'Fecha Scraping'] 
                            
                            # Process File 1 Records (Base)
                            for r1 in records1:
                                url = str(r1.get(url_col))
                                if url in map2:
                                    # URL exists in BOTH
                                    r2 = map2[url]
                                    final_row = r1.copy() # Start with File 1 data
                                    
                                    # Update specific fields from File 2
                                    for field in update_fields:
                                        # Check if field exists in r2 and is not empty/nan
                                        if field in r2 and pd.notna(r2[field]):
                                             final_row[field] = r2[field]
                                    
                                    merged_records.append(final_row)
                                else:
                                    # URL only in File 1
                                    merged_records.append(r1)
                                
                                processed_urls.add(url)
                            
                            # Process File 2 Records (Add unique only)
                            for r2 in records2:
                                url = str(r2.get(url_col))
                                if url not in processed_urls:
                                    merged_records.append(r2)
                                    processed_urls.add(url) # Just in case duplicate URLs within File 2 itself
                            
                            # Create DataFrame
                            df_combined = pd.DataFrame(merged_records)
                            
                            # STATS: Calculate duplicates removed
                            # Total input rows - Final unique rows
                            total_rows = len(df1) + len(df2)
                            stats['duplicates_removed'] += total_rows - len(df_combined)

                            # Enforce File 1 Columns Structure
                            # Filter columns to match df1.columns
                            # If File 2 introduced new columns (that are not in update_fields), they are dropped.
                            # If File 2 had 'exterior' and we updated it, it stays because 'exterior' is in df1 output likely.
                            cols1 = list(df1.columns)
                            
                            # Add missing columns with NaN if any (shouldn't happen for r1 rows, but r2 rows might miss some)
                            for col in cols1:
                                if col not in df_combined.columns:
                                    df_combined[col] = None
                            
                            # Select only File 1 columns in correct order
                            df_combined = df_combined[cols1]
                        
                        # Track merged stats
                        stats['merged']['properties'] += len(df_combined)
                        stats['merged']['districts'].update(count_districts(df_combined))
                            
                    elif sheet in sheet_names1:
                        # Case 2: Sheet only in File 1 -> COPY
                        df_combined = pd.read_excel(xl1, sheet_name=sheet)
                        stats['file1']['properties'] += len(df_combined)
                        stats['file1']['districts'].update(count_districts(df_combined))
                        stats['merged']['properties'] += len(df_combined)
                        stats['merged']['districts'].update(count_districts(df_combined))
                        
                    elif sheet in sheet_names2:
                        # Case 3: Sheet only in File 2 -> COPY
                        df_combined = pd.read_excel(xl2, sheet_name=sheet)
                        stats['file2']['properties'] += len(df_combined)
                        stats['file2']['districts'].update(count_districts(df_combined))
                        stats['merged']['properties'] += len(df_combined)
                        stats['merged']['districts'].update(count_districts(df_combined))
                    
                    # Write to output file
                    if df_combined is not None:
                        df_combined.to_excel(writer, sheet_name=sheet, index=False)
                        processed_sheets += 1
                
                if processed_sheets == 0:
                     return jsonify({'error': 'No matching sheets found to merge'}), 400

        # Convert sets to counts for JSON serialization
        result_stats = {
            'file1_properties': stats['file1']['properties'],
            'file1_districts': len(stats['file1']['districts']),
            'file2_properties': stats['file2']['properties'],
            'file2_districts': len(stats['file2']['districts']),
            'merged_properties': stats['merged']['properties'],
            'merged_districts': len(stats['merged']['districts']),
            'duplicates_removed': stats['duplicates_removed']
        }

        return jsonify({
            'status': 'success', 
            'output_file': output_name,
            'stats': result_stats
        })

    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    port = 5002
    url = f"http://127.0.0.1:{port}"
    print(f"Starting Merger Tool server on {url}")
    
    # Auto-open browser
    if not os.environ.get('NO_BROWSER_OPEN'):
        try:
            import webbrowser
            from threading import Timer
            def open_browser():
                webbrowser.open_new(url)
            Timer(1.5, open_browser).start()
        except:
            pass
        
    app.run(port=port, debug=False, use_reloader=False)
