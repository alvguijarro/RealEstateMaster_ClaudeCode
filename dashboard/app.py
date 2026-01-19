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
from flask.json.provider import DefaultJSONProvider
import pandas as pd
import numpy as np


class NumpyJSONProvider(DefaultJSONProvider):
    """Custom JSON provider that handles NumPy types."""
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif pd.isna(obj):
            return None
        return super().default(obj)

# Get absolute paths based on this file's location
_THIS_DIR = Path(__file__).parent.resolve()
_STATIC_DIR = str(_THIS_DIR / 'static')
_TEMPLATE_DIR = str(_THIS_DIR / 'templates')

app = Flask(__name__, 
            static_folder=_STATIC_DIR, 
            template_folder=_TEMPLATE_DIR)
app.config['SECRET_KEY'] = 'metrics-dashboard-secret'
app.json = NumpyJSONProvider(app)  # Use custom provider for NumPy types

# Default output directory (same as scraper)
DEFAULT_OUTPUT_DIR = str(Path(__file__).parent.parent / 'scraper' / 'salidas')


@app.after_request
def after_request(response):
    """Allow embedding in iframes, add CORS headers, and prevent caching."""
    response.headers.pop('X-Frame-Options', None)
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Methods'] = 'GET, POST, OPTIONS'
    response.headers['Access-Control-Allow-Headers'] = 'Content-Type'
    # Aggressive anti-caching headers to prevent browser from caching wrong template
    response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
    response.headers['Pragma'] = 'no-cache'
    response.headers['Expires'] = '0'
    response.headers['X-Service-Identity'] = 'Market-Metrics-Dashboard-5004'
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
                if f.is_file():  # Allow all Excel files
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
        
        # --- NEW CHART FIELDS ---
        
        # Helper: get value counts for a column (returns list of {label, value, raw})
        def get_distribution(df, col_candidates, normalize_label=True):
            col = find_column(df, col_candidates)
            if not col:
                return [], None
            counts = df[col].fillna('N/A').value_counts()
            result = []
            for val, count in counts.items():
                label = str(val).strip() if normalize_label else val
                result.append({'label': label, 'value': int(count), 'raw': val if pd.notna(val) else None})
            return result, col
        
        # 5. Baños distribution
        banos_distribution, banos_col = get_distribution(combined, ['banos', 'baños', 'bathrooms', 'num_banos'])
        
        # 6. Tipo distribution
        tipo_distribution, tipo_col = get_distribution(combined, ['tipo', 'type', 'tipologia'])
        
        # 7. Barrio distribution
        barrio_distribution, barrio_col = get_distribution(combined, ['barrio', 'neighborhood', 'neighbourhood'])
        
        # 8. Zona distribution
        zona_distribution, zona_col = get_distribution(combined, ['zona', 'zone', 'area'])
        
        # 9. Okupado/Copropiedad/Nuda Propiedad (combined)
        okupado_col = find_column(combined, ['okupado', 'ocupado', 'occupied'])
        coprop_col = find_column(combined, ['copropiedad', 'co-propiedad', 'co_propiedad'])
        nuda_col = find_column(combined, ['nuda propiedad', 'nuda_propiedad', 'bare_ownership'])
        
        special_status_distribution = []
        for label, col in [('Okupado', okupado_col), ('Copropiedad', coprop_col), ('Nuda Propiedad', nuda_col)]:
            if col:
                # Count entries where value indicates "yes" (Sí, Si, Yes, 1, True, etc.)
                yes_vals = combined[col].astype(str).str.lower().isin(['sí', 'si', 'yes', '1', 'true', 's'])
                count = yes_vals.sum()
                if count > 0:
                    special_status_distribution.append({'label': label, 'value': int(count), 'raw': label.lower()})
        
        # 10. Altura distribution
        altura_distribution, altura_col = get_distribution(combined, ['altura', 'planta', 'floor', 'piso'])
        
        # 11. Terraza (Yes/No bar chart)
        terraza_col = find_column(combined, ['terraza', 'terrace', 'balcon'])
        terraza_distribution = []
        if terraza_col:
            vals = combined[terraza_col].astype(str).str.lower()
            yes_count = vals.isin(['sí', 'si', 'yes', '1', 'true', 's']).sum()
            no_count = vals.isin(['no', '0', 'false', 'n', '']).sum()
            other_count = len(combined) - yes_count - no_count
            terraza_distribution = [
                {'label': 'Sí', 'value': int(yes_count), 'raw': True},
                {'label': 'No', 'value': int(no_count + other_count), 'raw': False}
            ]
        
        # 12. Garaje (Yes/No bar chart)
        garaje_col = find_column(combined, ['garaje', 'garage', 'parking', 'plaza_garaje'])
        garaje_distribution = []
        if garaje_col:
            vals = combined[garaje_col].astype(str).str.lower()
            yes_count = vals.isin(['sí', 'si', 'yes', '1', 'true', 's']).sum()
            no_count = vals.isin(['no', '0', 'false', 'n', '']).sum()
            other_count = len(combined) - yes_count - no_count
            garaje_distribution = [
                {'label': 'Sí', 'value': int(yes_count), 'raw': True},
                {'label': 'No', 'value': int(no_count + other_count), 'raw': False}
            ]
        
        # 13. Trastero (Yes/No bar chart)
        trastero_col = find_column(combined, ['trastero', 'storage', 'almacen'])
        trastero_distribution = []
        if trastero_col:
            vals = combined[trastero_col].astype(str).str.lower()
            yes_count = vals.isin(['sí', 'si', 'yes', '1', 'true', 's']).sum()
            no_count = vals.isin(['no', '0', 'false', 'n', '']).sum()
            other_count = len(combined) - yes_count - no_count
            trastero_distribution = [
                {'label': 'Sí', 'value': int(yes_count), 'raw': True},
                {'label': 'No', 'value': int(no_count + other_count), 'raw': False}
            ]
        
        # 14. Estado (Column chart)
        estado_distribution, estado_col = get_distribution(combined, ['estado', 'condition', 'state', 'conservacion'])
        
        # 15. Total count
        total_properties = len(combined)
        
        # 16. Prepare raw data for frontend filtering
        # Include ALL detected columns for each property
        
        # Find new columns for property table
        titulo_col = find_column(combined, ['titulo', 'title', 'nombre', 'name'])
        url_col = find_column(combined, ['url', 'link', 'enlace', 'hyperlink'])
        precio_col = find_column(combined, ['precio', 'price', 'coste', 'rent', 'alquiler'])
        
        raw_properties = []
        for _, row in combined.iterrows():
            prop = {'district': row['_sheet']}
            
            # Title and URL for hyperlink
            if titulo_col and pd.notna(row.get(titulo_col)):
                prop['titulo'] = str(row[titulo_col])
            if url_col and pd.notna(row.get(url_col)):
                prop['url'] = str(row[url_col])
            
            # Price (raw, not per m2)
            if precio_col and pd.notna(row.get(precio_col)):
                try:
                    val = str(row[precio_col]).replace('€','').replace('.','').replace(',','.').strip()
                    # Handle "/mes" suffix for rent
                    val = val.split('/')[0].strip()
                    prop['precio'] = float(val)
                except: pass
            
            # Core fields
            if room_col and pd.notna(row.get(room_col)):
                try: prop['rooms'] = int(row[room_col])
                except: pass
            if size_col and pd.notna(row.get(size_col)):
                try: prop['size'] = float(str(row[size_col]).replace('m²','').replace('m2','').strip())
                except: pass
            
            # Price per m2
            p_m2 = None
            if price_per_m2_col and pd.notna(row.get(price_per_m2_col)):
                try:
                    val = str(row[price_per_m2_col]).replace('€','').replace('.','').replace(',','.').strip()
                    p_m2 = float(val)
                except: pass
            elif precio_col and size_col:
                try:
                    p_val = str(row[precio_col]).replace('€','').replace('.','').replace(',','.').strip()
                    precio = float(p_val)
                    size = float(str(row[size_col]).replace('m²','').replace('m2','').strip())
                    if size > 0: p_m2 = round(precio / size, 2)
                except: pass
            if p_m2 is not None: prop['price_per_m2'] = p_m2
            
            # New fields
            if banos_col and pd.notna(row.get(banos_col)):
                try: prop['banos'] = int(row[banos_col])
                except: prop['banos'] = str(row[banos_col])
            if tipo_col and pd.notna(row.get(tipo_col)):
                prop['tipo'] = str(row[tipo_col])
            if barrio_col and pd.notna(row.get(barrio_col)):
                prop['barrio'] = str(row[barrio_col])
            if zona_col and pd.notna(row.get(zona_col)):
                prop['zona'] = str(row[zona_col])
            if altura_col and pd.notna(row.get(altura_col)):
                prop['altura'] = str(row[altura_col])
            if estado_col and pd.notna(row.get(estado_col)):
                prop['estado'] = str(row[estado_col])
            
            # Boolean fields
            if terraza_col and pd.notna(row.get(terraza_col)):
                prop['terraza'] = str(row[terraza_col]).lower() in ['sí', 'si', 'yes', '1', 'true', 's']
            if garaje_col and pd.notna(row.get(garaje_col)):
                prop['garaje'] = str(row[garaje_col]).lower() in ['sí', 'si', 'yes', '1', 'true', 's']
            if trastero_col and pd.notna(row.get(trastero_col)):
                prop['trastero'] = str(row[trastero_col]).lower() in ['sí', 'si', 'yes', '1', 'true', 's']
            
            # Special status
            if okupado_col and pd.notna(row.get(okupado_col)):
                prop['okupado'] = str(row[okupado_col]).lower() in ['sí', 'si', 'yes', '1', 'true', 's']
            if coprop_col and pd.notna(row.get(coprop_col)):
                prop['copropiedad'] = str(row[coprop_col]).lower() in ['sí', 'si', 'yes', '1', 'true', 's']
            if nuda_col and pd.notna(row.get(nuda_col)):
                prop['nuda_propiedad'] = str(row[nuda_col]).lower() in ['sí', 'si', 'yes', '1', 'true', 's']
                
            raw_properties.append(prop)
        
        return jsonify({
            'total': total_properties,
            'district_summary': district_summary,
            'room_distribution': room_distribution,
            'size_distribution': size_distribution,
            'price_stats': price_stats,
            'banos_distribution': banos_distribution,
            'tipo_distribution': tipo_distribution,
            'barrio_distribution': barrio_distribution,
            'zona_distribution': zona_distribution,
            'special_status_distribution': special_status_distribution,
            'altura_distribution': altura_distribution,
            'terraza_distribution': terraza_distribution,
            'garaje_distribution': garaje_distribution,
            'trastero_distribution': trastero_distribution,
            'estado_distribution': estado_distribution,
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
