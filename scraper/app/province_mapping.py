"""
Province-to-File Mapping Utilities for Smart Enrichment

This module provides utilities for:
1. Detecting province and operation type from Idealista URLs
2. Looking up the correct output file based on province and operation
3. Loading enrichment status from existing Excel files
"""

import json
import os
import re
from pathlib import Path
from typing import Optional, Tuple, Set, Dict
from datetime import datetime

# Path to province file mapping
PROVINCE_FILE_MAPPING_PATH = Path(__file__).parent / "province_file_mapping.json"

# Path to low cost provinces (contains URL patterns)
LOW_COST_PROVINCES_PATH = Path(__file__).parent.parent / "low_cost_provinces.json"

# Default output directory
DEFAULT_OUTPUT_DIR = str(Path(__file__).parent.parent / "salidas")


def load_province_file_mapping() -> dict:
    """Load the province-to-file mapping from JSON."""
    if not PROVINCE_FILE_MAPPING_PATH.exists():
        return {}
    
    try:
        with open(PROVINCE_FILE_MAPPING_PATH, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return {}


def load_low_cost_provinces() -> list:
    """Load the low cost provinces configuration (URL patterns)."""
    if not LOW_COST_PROVINCES_PATH.exists():
        return []
    
    try:
        with open(LOW_COST_PROVINCES_PATH, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return []


def detect_province_and_operation(url: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Detect province name and operation type (venta/alquiler) from an Idealista URL.
    
    Args:
        url: The Idealista URL to analyze
        
    Returns:
        Tuple of (province_name, operation_type) where operation_type is 'venta' or 'alquiler'.
        Returns (None, None) if detection fails.
    """
    url_lower = url.lower()
    
    # Detect operation type
    operation = None
    if "alquiler-viviendas" in url_lower or "/alquiler-" in url_lower:
        operation = "alquiler"
    elif "venta-viviendas" in url_lower or "/venta-" in url_lower:
        operation = "venta"
    
    if not operation:
        return None, None
    
    # Load province patterns
    provinces = load_low_cost_provinces()
    
    for province in provinces:
        name = province.get("name", "")
        url_venta = province.get("url_venta", "").lower()
        url_alquiler = province.get("url_alquiler", "").lower()
        
        # Check if URL matches province pattern
        if operation == "venta" and url_venta:
            # Extract the path portion after domain
            venta_path = url_venta.replace("https://www.idealista.com/", "").rstrip("/")
            if venta_path in url_lower:
                return name, operation
        elif operation == "alquiler" and url_alquiler:
            alquiler_path = url_alquiler.replace("https://www.idealista.com/", "").rstrip("/")
            if alquiler_path in url_lower:
                return name, operation
    
    # Fallback: try to extract province from URL patterns like /provincia/ or province name
    # Pattern: venta-viviendas/{province}-provincia/
    match = re.search(r'(?:venta|alquiler)-viviendas/([^/]+)(?:-provincia)?/?', url_lower)
    if match:
        slug = match.group(1)
        # Try to find matching province by slug
        for province in provinces:
            name = province.get("name", "")
            url_venta = province.get("url_venta", "").lower()
            url_alquiler = province.get("url_alquiler", "").lower()
            
            if slug in url_venta or slug in url_alquiler:
                return name, operation
    
    return None, operation


def get_province_output_file(province: str, operation: str) -> Optional[str]:
    """
    Get the output filename for a given province and operation type.
    
    Args:
        province: Province name (e.g., "Toledo", "A Coruña")
        operation: Operation type ("venta" or "alquiler")
        
    Returns:
        Filename like "idealista_Toledo_venta_MERGED.xlsx" or None if not found.
    """
    mapping = load_province_file_mapping()
    
    province_entry = mapping.get(province, {})
    return province_entry.get(operation)


def get_output_file_for_url(url: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Get the output filename for a given Idealista URL.
    
    Args:
        url: The Idealista URL
        
    Returns:
        Tuple of (filename, province, operation) or (None, None, None) if not found.
    """
    province, operation = detect_province_and_operation(url)
    
    if not province or not operation:
        return None, province, operation
    
    filename = get_province_output_file(province, operation)
    return filename, province, operation


def load_enriched_urls(excel_path: str) -> Set[str]:
    """
    Load URLs that should be skipped (Enriched OR Inactive) from an Excel file.
    
    A URL is skipped if:
    1. It is already enriched (__enriched__=True AND Fecha Enriquecimiento exists)
    2. It is marked as INACTIVE (Anuncio activo = "No")
    
    Args:
        excel_path: Path to the Excel file
        
    Returns:
        Set of URLs to skip (Enriched + Inactive)
    """
    import pandas as pd
    
    skip_urls = set()
    
    if not os.path.exists(excel_path):
        return skip_urls
    
    try:
        # Read all sheets
        xlsx = pd.ExcelFile(excel_path)
        
        for sheet_name in xlsx.sheet_names:
            df = pd.read_excel(xlsx, sheet_name=sheet_name)
            
            # Find URL column (case-insensitive)
            url_col = None
            for col in df.columns:
                if col.lower() == 'url':
                    url_col = col
                    break
            
            if not url_col:
                continue
            
            # Check for columns
            enriched_col = None
            fecha_col = None
            active_col = None
            
            for col in df.columns:
                col_lower = str(col).lower().replace("_", "").replace(" ", "")
                if col_lower in ["enriched", "__enriched__"]:
                    enriched_col = col
                elif "fechaenriquecimiento" in col_lower:
                    fecha_col = col
                elif "anuncioactivo" in col_lower or "active" in col_lower or "activo" in col_lower:
                    active_col = col
            
            # Filter rows
            for idx, row in df.iterrows():
                url = row.get(url_col)
                if not url or pd.isna(url):
                    continue
                
                # Check Enrichment
                is_enriched = False
                has_fecha = False
                
                if enriched_col:
                    val = row.get(enriched_col)
                    if pd.notna(val):
                        is_enriched = str(val).upper() in ['TRUE', '1', 'VERDADERO', 'SÍ', 'SI', 'YES']
                
                if fecha_col:
                    val = row.get(fecha_col)
                    if pd.notna(val) and str(val).strip():
                        has_fecha = True
                
                # Check Inactive Status (User Request: Skip inactive properties forever)
                    if active_col:
                        val = str(row.get(active_col, "")).strip().lower()
                        if val in ["no", "false", "falso", "0"]:
                            is_inactive = True
                
                # Add to skip set if Enriched OR Inactive
                if (is_enriched and has_fecha) or is_inactive:
                    skip_urls.add(str(url).strip())
        
    except Exception as e:
        print(f"Error loading skip URLs: {e}")
    
    return skip_urls


def load_all_urls_from_excel(excel_path: str) -> Dict[str, dict]:
    """
    Load all URLs from an Excel file with their enrichment and active status.
    
    Args:
        excel_path: Path to the Excel file
        
    Returns:
        Dictionary mapping URL to a dict with 'enriched', 'fecha', 'sheet', 'is_inactive' keys
    """
    import pandas as pd
    
    url_data = {}
    
    if not os.path.exists(excel_path):
        return url_data
    
    try:
        # Read all sheets
        xlsx = pd.ExcelFile(excel_path)
        
        for sheet_name in xlsx.sheet_names:
            df = pd.read_excel(xlsx, sheet_name=sheet_name)
            
            # Find URL column
            url_col = None
            for col in df.columns:
                if col.lower() == 'url':
                    url_col = col
                    break
            
            if not url_col:
                continue
            
            # Find enrichment and active columns
            enriched_col = None
            fecha_col = None
            active_col = None
            
            for col in df.columns:
                col_lower = str(col).lower().replace("_", "").replace(" ", "")
                if col_lower in ["enriched", "__enriched__"]:
                    enriched_col = col
                elif "fechaenriquecimiento" in col_lower:
                    fecha_col = col
                elif "anuncioactivo" in col_lower or "active" in col_lower or "activo" in col_lower:
                    active_col = col

            for idx, row in df.iterrows():
                url = row.get(url_col)
                if not url or pd.isna(url):
                    continue
                
                url_str = str(url).strip()
                
                is_enriched = False
                fecha = None
                is_inactive = False
                
                if enriched_col:
                    val = row.get(enriched_col)
                    if pd.notna(val):
                        is_enriched = str(val).upper() in ['TRUE', '1', 'VERDADERO', 'SÍ', 'SI', 'YES']
                
                if fecha_col:
                    val = row.get(fecha_col)
                    if pd.notna(val) and str(val).strip():
                        fecha = str(val)
                
                if active_col:
                    val = str(row.get(active_col, "")).strip().lower()
                    if val in ["no", "false", "0", "falso"]:
                        is_inactive = True
                
                url_data[url_str] = {
                    'enriched': is_enriched,
                    'fecha': fecha,
                    'sheet': sheet_name,
                    'row_index': idx,
                    'is_inactive': is_inactive
                }
        
    except Exception as e:
        print(f"Error loading URLs from Excel: {e}")
    
    return url_data


def mark_as_enriched(row: dict) -> dict:
    """
    Add enrichment markers to a property row.
    
    Args:
        row: The property data dictionary
        
    Returns:
        Updated row with __enriched__ = True and Fecha Enriquecimiento
    """
    row["__enriched__"] = True
    row["Fecha Enriquecimiento"] = datetime.now().strftime("%d/%m/%Y")
    return row


if __name__ == "__main__":
    # Test detection
    test_urls = [
        "https://www.idealista.com/venta-viviendas/toledo-provincia/",
        "https://www.idealista.com/alquiler-viviendas/a-coruna-provincia/",
        "https://www.idealista.com/venta-viviendas/madrid-provincia/",
        "https://www.idealista.com/alquiler-viviendas/ceuta-ceuta/",
    ]
    
    print("Province-to-File Mapping Test")
    print("=" * 60)
    
    for url in test_urls:
        filename, province, operation = get_output_file_for_url(url)
        print(f"\nURL: {url}")
        print(f"  Province: {province}")
        print(f"  Operation: {operation}")
        print(f"  File: {filename}")
