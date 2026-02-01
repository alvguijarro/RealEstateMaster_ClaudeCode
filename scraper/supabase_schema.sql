
-- Create a table for property listings
CREATE TABLE IF NOT EXISTS listings (
    url TEXT PRIMARY KEY,
    titulo TEXT,
    price NUMERIC,
    old_price NUMERIC,
    price_change_pct NUMERIC,
    ubicacion TEXT,
    actualizado_hace TEXT,
    m2_construidos NUMERIC,
    m2_utiles NUMERIC,
    precio_m2 NUMERIC,
    num_plantas INTEGER,
    habs INTEGER,
    banos INTEGER,
    terraza BOOLEAN DEFAULT FALSE,
    garaje BOOLEAN DEFAULT FALSE,
    armarios BOOLEAN DEFAULT FALSE,
    trastero BOOLEAN DEFAULT FALSE,
    calefaccion TEXT,
    tipo TEXT,
    parcela NUMERIC,
    ascensor BOOLEAN DEFAULT FALSE,
    orientacion TEXT,
    altura TEXT,
    construido_en INTEGER,
    jardin BOOLEAN DEFAULT FALSE,
    piscina BOOLEAN DEFAULT FALSE,
    aire_acond BOOLEAN DEFAULT FALSE,
    calle TEXT,
    barrio TEXT,
    distrito TEXT,
    zona TEXT,
    ciudad TEXT,
    provincia TEXT,
    consumo_1 TEXT,
    consumo_2 TEXT,
    emisiones_1 TEXT,
    emisiones_2 TEXT,
    estado TEXT,
    gastos_comunidad TEXT,
    okupado BOOLEAN DEFAULT FALSE,
    copropiedad BOOLEAN DEFAULT FALSE,
    con_inquilino BOOLEAN DEFAULT FALSE,
    nuda_propiedad BOOLEAN DEFAULT FALSE,
    ces_remate BOOLEAN DEFAULT FALSE,
    tipo_anunciante TEXT,
    nombre_anunciante TEXT,
    descripcion TEXT,
    fecha_scraping TEXT,
    anuncio_activo BOOLEAN DEFAULT TRUE,
    baja_anuncio BOOLEAN DEFAULT FALSE,
    comunidad_autonoma TEXT,
    source_file TEXT,
    ingestion_date TIMESTAMPTZ DEFAULT NOW(),
    
    -- Constraint to ensure URL is unique (PK handles this, but good to be explicit about intention)
    CONSTRAINT listings_url_key UNIQUE (url)
);

-- Enable Row Level Security (RLS) - Recommended
ALTER TABLE listings ENABLE ROW LEVEL SECURITY;

-- Policy to allow anonymous/public read access (since we are using publishable key for reading)
CREATE POLICY "Allow public read access" ON listings FOR SELECT USING (true);

-- Policy to allow anonymous/public insert/update (since we are using publishable key for scraping)
-- WARNING: In production this should be restricted, but for this personal tool it simplifies things.
CREATE POLICY "Allow public insert access" ON listings FOR INSERT WITH CHECK (true);
CREATE POLICY "Allow public update access" ON listings FOR UPDATE USING (true);
