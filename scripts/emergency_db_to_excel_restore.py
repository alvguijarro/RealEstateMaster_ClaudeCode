import sqlite3
import pandas as pd
import os
from pathlib import Path

# Configuration
DB_PATH = Path("scraper/real_estate.db")
OUTPUT_DIR = Path("scraper/salidas")
RECOVERY_DIR = Path("scraper/salidas_RECOVERED")

ORDERED_BASE = (
    "Titulo", "price", "old price", "price change %", "Ubicacion",
    "actualizado hace",
    "m2 construidos", "m2 utiles", "precio por m2", "Num plantas", "habs", "banos",
    "Terraza", "Garaje", "Armarios", "Trastero", "Calefaccion",
    "tipo", "parcela", "ascensor", "orientacion", "altura",
    "construido en", "jardin", "piscina", "aire acond",
    "Calle", "Barrio", "Distrito", "Zona", "Ciudad", "Provincia",
    "Consumo 1", "Consumo 2", "Emisiones 1", "Emisiones 2",
    "estado", "gastos comunidad",
    "okupado", "Copropiedad", "con inquilino", "nuda propiedad", "ces. remate",
    "tipo anunciante", "nombre anunciante",
    "Descripcion",
    "Fecha Scraping",
    "URL",
    "Anuncio activo", "Baja anuncio", "Comunidad Autonoma"
)

DB_TO_EXCEL_MAP = {
    "url": "URL",
    "titulo": "Titulo",
    "price": "price",
    "old_price": "old price",
    "price_change_pct": "price change %",
    "ubicacion": "Ubicacion",
    "actualizado_hace": "actualizado hace",
    "m2_const": "m2 construidos",
    "m2_utiles": "m2 utiles",
    "habitaciones": "habs",
    "banos": "banos",
    "plantas": "Num plantas",
    "ascensor": "ascensor",
    "garaje": "Garaje",
    "descripcion": "Descripcion",
    "terraza": "Terraza",
    "trastero": "Trastero",
    "consumo": "Consumo 1",
    "emisiones": "Emisiones 1",
    "gastos_comunidad": "gastos comunidad",
    "parcela": "parcela",
    "provincia": "Provincia",
    "fecha_scraping": "Fecha Scraping",
    "anuncio_activo": "Anuncio activo",
    "baja_anuncio": "Baja anuncio",
    "comunidad_autonoma": "Comunidad Autonoma"
}

def restore_data():
    if not DB_PATH.exists():
        print(f"❌ Error: Database not found at {DB_PATH}")
        return

    os.makedirs(RECOVERY_DIR, exist_ok=True)
    
    conn = sqlite3.connect(DB_PATH)
    try:
        # Get unique source files
        cursor = conn.execute("SELECT DISTINCT source_file FROM listings WHERE source_file != 'unknown'")
        source_files = [row[0] for row in cursor.fetchall()]
        
        print(f"🔍 Found {len(source_files)} source files in database.")
        
        for sf in source_files:
            if not sf or sf == "nan": continue
            print(f"📦 Restoring {sf}...")
            
            # Fetch all rows for this file
            df = pd.read_sql_query(f"SELECT * FROM listings WHERE source_file = ?", conn, params=(sf,))
            
            # Map columns
            df_renamed = df.rename(columns=DB_TO_EXCEL_MAP)
            
            # Ensure all ORDERED_BASE columns exist
            for col in ORDERED_BASE:
                if col not in df_renamed.columns:
                    df_renamed[col] = None
            
            # Reorder
            df_final = df_renamed[list(ORDERED_BASE)]
            
            # Save to recovered directory
            out_path = RECOVERY_DIR / sf
            df_final.to_excel(out_path, index=False)
            print(f"  ✅ Restored: {out_path} ({len(df_final)} rows)")
            
    finally:
        conn.close()

if __name__ == "__main__":
    restore_data()
