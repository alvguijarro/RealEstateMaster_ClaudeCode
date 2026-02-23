import pandas as pd
import sys

file_path = r'scraper\salidas\idealista_Almería_venta.xlsx'
try:
    xls = pd.ExcelFile(file_path)
    total_okupados = 0
    total_rows = 0
    
    print(f"Analyzing: {file_path}")
    for sheet in xls.sheet_names:
        df = pd.read_excel(xls, sheet_name=sheet)
        total_rows += len(df)
        ok_count = 0
        if 'okupado' in df.columns:
            ok_count = (df['okupado'] == 'Sí').sum()
            total_okupados += ok_count
            
            # Print samples of "Okupado" properties to see descriptions
            if ok_count > 0:
                print(f"Sheet: {sheet}, Total: {len(df)}, Okupados: {ok_count}")
                samples = df[df['okupado'] == 'Sí'].head(3)
                for i, row in samples.iterrows():
                    print(f"  - URL: {row.get('URL', 'N/A')}")
                    print(f"    Desc: {str(row.get('Descripcion', 'N/A'))[:200]}...")
        else:
            print(f"Sheet: {sheet}, Column 'okupado' NOT found.")
            
    print(f"\nFINAL TOTAL: {total_rows}")
    print(f"FINAL TOTAL OKUPADOS: {total_okupados}")
    if total_rows > 0:
        print(f"PERCENTAGE: {(total_okupados/total_rows)*100:.2f}%")

except Exception as e:
    print(f"Error: {e}")
