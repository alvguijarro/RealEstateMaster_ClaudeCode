"""
Market Metrics Dashboard - Flask Server
Provides visual analytics for VENTA (Sales) Excel files.
Port: 5004
"""
from __future__ import annotations

import os
import sys
import json
import time
from pathlib import Path

from flask import Flask, render_template, jsonify, request
import pandas as pd

app = Flask(__name__, 
            static_folder='static', 
            template_folder='templates')
app.config['SECRET_KEY'] = 'metrics-dashboard-secret'

# Default output directory (same as scraper)
DEFAULT_OUTPUT_DIR = str(Path(__file__).parent.parent / 'scraper' / 'salidas')


@app.after_request
def after_request(response):
    """Allow embedding in iframes and add CORS headers."""
    response.headers.pop('X-Frame-Options', None)
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Methods'] = 'GET, POST, OPTIONS'
    response.headers['Access-Control-Allow-Headers'] = 'Content-Type'
    return response


@app.route('/health', methods=['GET', 'OPTIONS'])
def health_check():
    """Health check endpoint for service readiness polling."""
    return jsonify({'status': 'ok'})


@app.route('/')
def index():
    """Serve the main dashboard interface."""
    return render_template('index.html', cache_bust=int(time.time()))


@app.route('/api/files', methods=['GET'])
def get_files():
    """Get list of VENTA Excel files."""
    files = []
    
    search_dirs = [
        DEFAULT_OUTPUT_DIR,
        Path(__file__).parent.parent / 'scraper',
    ]
    
    for search_dir in search_dirs:
        if not search_dir:
            continue
        search_path = Path(search_dir)
        if search_path.exists():
            for f in search_path.glob('*.xlsx'):
                # Only include files with VENTA in name
                if 'VENTA' in f.name.upper() and f.is_file():
                    files.append({
                        'name': f.name,
                        'path': str(f.resolve())
                    })
    
    # Deduplicate by path
    seen_paths = set()
    unique_files = []
    for f in files:
        if f['path'] not in seen_paths:
            seen_paths.add(f['path'])
            unique_files.append(f)
    
    return jsonify({'files': unique_files})


@app.route('/api/districts', methods=['GET'])
def get_districts():
    """Get list of districts (sheet names) for a selected file."""
    file_path = request.args.get('file', '').strip()
    
    if not file_path or not os.path.exists(file_path):
        return jsonify({'error': 'File not found'}), 404
    
    try:
        xl = pd.ExcelFile(file_path)
        sheets = xl.sheet_names
        return jsonify({'districts': sheets})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/analytics', methods=['POST'])
def get_analytics():
    """
    Process the Excel file and return analytics data for charts.
    
    Request JSON:
    {
        "file": "path/to/file.xlsx",
        "districts": ["Distrito1", "Distrito2"]  // optional, empty = all
    }
    """
    data = request.get_json()
    file_path = data.get('file', '').strip()
    selected_districts = data.get('districts', [])
    
    if not file_path or not os.path.exists(file_path):
        return jsonify({'error': 'File not found'}), 404
    
    try:
        # Load all sheets or selected ones
        xl = pd.ExcelFile(file_path)
        all_sheets = xl.sheet_names
        
        if selected_districts:
            sheets_to_load = [s for s in selected_districts if s in all_sheets]
        else:
            sheets_to_load = all_sheets
        
        # Combine all selected sheets into one DataFrame
        dfs = []
        for sheet in sheets_to_load:
            df = pd.read_excel(file_path, sheet_name=sheet)
            df['_sheet'] = sheet  # Track source sheet
            dfs.append(df)
        
        if not dfs:
            return jsonify({'error': 'No data found'}), 400
        
        combined = pd.concat(dfs, ignore_index=True)
        
        # --- Calculate Metrics ---
        
        # Helper: find column by multiple possible names (case-insensitive)
        def find_column(df, candidates):
            df_cols_lower = {c.lower(): c for c in df.columns}
            for cand in candidates:
                if cand.lower() in df_cols_lower:
                    return df_cols_lower[cand.lower()]
            return None
        
        # 1. District Summary (count per district/sheet)
        district_counts = combined.groupby('_sheet').size().reset_index(name='count')
        district_summary = district_counts.to_dict('records')
        
        # 2. Room Distribution (for Pie Chart)
        room_col = find_column(combined, ['habs', 'habitaciones', 'rooms', 'dormitorios', 'num_rooms'])
        room_distribution = []
        if room_col:
            room_counts = combined[room_col].value_counts().sort_index()
            for rooms, count in room_counts.items():
                label = f"{int(rooms)} hab" if pd.notna(rooms) else "N/A"
                room_distribution.append({'label': label, 'value': int(count), 'raw': int(rooms) if pd.notna(rooms) else None})
        
        # 3. Size Distribution (for Histogram)
        size_col = find_column(combined, ['m2 construidos', 'm2 utiles', 'm2', 'dimensiones', 'size', 'superficie'])
        
        # 3. Size Distribution (Histogram)
        size_distribution = []
        if size_col:
            # Clean size column first
            clean_sizes = pd.to_numeric(
                combined[size_col].astype(str).str.replace('m²', '').str.replace('m2', '').str.strip(), 
                errors='coerce'
            ).dropna()
            
            if not clean_sizes.empty:
                # Create bins (0-20, 20-40, ... 300+)
                bins = list(range(0, 301, 20)) + [float('inf')]
                labels = [f"{i}-{i+20}" for i in range(0, 281, 20)] + ["300+"]
                
                # Use the original combined DataFrame to add the 'size_bin' column
                # This ensures the index aligns correctly for value_counts later
                temp_df = pd.DataFrame({'clean_sizes': clean_sizes}, index=clean_sizes.index)
                temp_df['size_bin'] = pd.cut(temp_df['clean_sizes'], bins=bins, labels=labels, right=False)
                
                size_counts = temp_df['size_bin'].value_counts().sort_index()
                
                # Format for chart
                for label, count in size_counts.items():
                    if count > 0:
                        # Find min/max for this bin to help filtering
                        if label == "300+":
                            interval = pd.Interval(300, 99999, closed='left')
                        else:
                            try:
                                parts = label.split('-')
                                interval = pd.Interval(int(parts[0]), int(parts[1]), closed='left')
                            except:
                                continue
                                
                        size_distribution.append({
                            'label': label, 
                            'value': int(count),
                            'min': int(interval.left),
                            'max': int(interval.right)
                        })
        
        # Helper: clean currency string
        def clean_currency(series):
            if series.dtype == 'object':
                return pd.to_numeric(series.astype(str).str.replace('€', '').str.replace('.', '').str.replace(',', '.').str.strip(), errors='coerce')
            return pd.to_numeric(series, errors='coerce')

        # 4. Price per m2 Statistics (for display)
        # First try direct price_per_m2 column
        price_per_m2_col = find_column(combined, ['precio por m2', 'price_per_m2', 'precio/m2', 'euro/m2'])
        price_stats = {}
        
        if price_per_m2_col:
            prices = clean_currency(combined[price_per_m2_col]).dropna()
            if not prices.empty:
                price_stats = {
                    'min': round(prices.min(), 2),
                    'max': round(prices.max(), 2),
                    'mean': round(prices.mean(), 2),
                    'median': round(prices.median(), 2)
                }
        else:
            # Calculate from Precio / Dimensiones
            precio_col = find_column(combined, ['precio', 'price', 'coste'])
            if precio_col and size_col:
                precios = clean_currency(combined[precio_col])
                sizes = pd.to_numeric(combined[size_col], errors='coerce')
                # Calculate price per m2
                price_per_m2 = (precios / sizes).dropna()
                price_per_m2 = price_per_m2[price_per_m2 > 0]  # Remove negatives/zeros
                if not price_per_m2.empty:
                    price_stats = {
                        'min': round(price_per_m2.min(), 2),
                        'max': round(price_per_m2.max(), 2),
                        'mean': round(price_per_m2.mean(), 2),
                        'median': round(price_per_m2.median(), 2)
                    }
        
        # 5. Total count
        total_properties = len(combined)
        
        # 6. Prepare raw data for frontend filtering
        # Include essential columns for each property
        raw_properties = []
        for _, row in combined.iterrows():
            prop = {'district': row['_sheet']}
            if room_col and pd.notna(row.get(room_col)):
                prop['rooms'] = int(row[room_col])
            if size_col and pd.notna(row.get(size_col)):
                prop['size'] = float(row[size_col])
            
            # Add price data if available
            p_m2 = None
            if price_per_m2_col and pd.notna(row.get(price_per_m2_col)):
                try:
                    val = str(row[price_per_m2_col]).replace('€','').replace('.','').replace(',','.').strip()
                    p_m2 = float(val)
                except:
                    pass
            elif precio_col and size_col:
                try:
                    p_val = str(row[precio_col]).replace('€','').replace('.','').replace(',','.').strip()
                    precio = float(p_val)
                    size = float(row[size_col])
                    if size > 0:
                        p_m2 = round(precio / size, 2)
                except:
                    pass
            
            if p_m2 is not None:
                prop['price_per_m2'] = p_m2
                
            raw_properties.append(prop)
        
        return jsonify({
            'total': total_properties,
            'district_summary': district_summary,
            'room_distribution': room_distribution,
            'size_distribution': size_distribution,
            'price_stats': price_stats,
            'raw_properties': raw_properties
        })
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


def run_server(host='127.0.0.1', port=5004):
    """Run the Flask server."""
    print(f"Starting Metrics Dashboard at http://{host}:{port}")
    app.run(host=host, port=port, debug=False)


if __name__ == '__main__':
    run_server()
