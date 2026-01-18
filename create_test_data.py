import pandas as pd
import numpy as np
import os

# Define sample data
districts = ['Centro', 'Chamberí', 'Salamanca', 'Retiro', 'Tetuan']
data = []

for _ in range(100):
    district = np.random.choice(districts)
    price = np.random.randint(150000, 2000000)
    size = np.random.randint(40, 300)
    rooms = np.random.randint(1, 6)
    
    data.append({
        'Titulo': f'Piso en {district}',
        'Precio': f'{price} €',
        'Dimensiones': size,
        'Habitaciones': rooms,
        'Descripcion': 'Lorem ipsum...'
    })

df = pd.DataFrame(data)

# Create output directory if it doesn't exist
output_dir = 'scraper/salidas'
os.makedirs(output_dir, exist_ok=True)

# Save to Excel - creating separate sheets for districts as expected by the dashboard
with pd.ExcelWriter(os.path.join(output_dir, 'VENTA_TEST_2026-01-18_10-00.xlsx')) as writer:
    for district in districts:
        # Filter data for this district and save to its own sheet
        district_data = df[df['Titulo'].str.contains(district)]
        district_data.to_excel(writer, sheet_name=district, index=False)

print("Created dummy VENTA file for testing")
