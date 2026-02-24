"""Public API + column schema (trimmed)."""
__all__ = ["ORDERED_BASE", "ORDERED_HABITACIONES", "log", "ScraperSession"]
__version__ = "5.0.0"
ORDERED_BASE = (
    # Primary identification
    "Titulo", "price", "old price", "price change %", "Ubicacion",

    "actualizado hace",

    # Property details
    "m2 construidos", "m2 utiles", "precio por m2", "Num plantas", "habs", "banos",

    "Terraza", "Garaje", "Armarios", "Trastero", "Calefaccion",

    "tipo", "parcela", "ascensor", "orientacion", "altura", "exterior",
    "construido en", "jardin", "piscina", "aire acond",

    # Location (moved after a/a as requested)
    "Calle", "Barrio", "Distrito", "Zona", "Ciudad", "Provincia",

    # Energy & condition
    "Consumo 1", "Consumo 2", "Emisiones 1", "Emisiones 2",

    "estado", "gastos comunidad",

    "okupado", "Copropiedad", "con inquilino", "nuda propiedad", "ces. remate",

    "tipo anunciante", "nombre anunciante",

    "Descripcion",
    
    "Fecha Scraping",

    # URL last as requested
    "URL",
    
    "Anuncio activo", "Baja anuncio", "Comunidad Autonoma"
)

# Column schema for room rentals (habitaciones)
# Order matches user specification exactly
ORDERED_HABITACIONES = (
    "Titulo", "price", "old price", "price change %", "Ubicacion",
    "actualizado hace",
    "habs", "m2_habs", "banos",
    "Terraza", "Garaje", "Armarios", "Trastero", "Calefaccion",
    "ascensor", "orientacion", "altura",
    "jardin", "piscina", "aire acond",
    "Calle", "Barrio", "Distrito", "Zona", "Ciudad", "Provincia",
    "estado",
    "tipo anunciante", "nombre anunciante",
    "Descripcion",
    "Fecha Scraping",
    "URL",
    "Anuncio activo", "Baja anuncio", "Comunidad Autonoma"
)

from .utils import log
from .scraper import ScraperSession
