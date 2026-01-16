"""Public API + column schema (trimmed)."""
__all__ = ["ORDERED_BASE", "log", "ScraperSession"]
__version__ = "5.0.0"
ORDERED_BASE = (
    # Primary identification
    "Titulo", "price", "old price", "price change %", "Ubicacion",

    "actualizado hace",

    # Property details
    "m2 construidos", "m2 utiles", "precio por m2", "Num plantas", "habs", "banos",

    "Terraza", "Garaje", "Armarios", "Trastero", "Calefaccion",

    "tipo", "parcela", "ascensor", "orientacion", "altura",
    "construido en", "jardin", "piscina", "aire acond",

    # Location (moved after a/a as requested)
    "Calle", "Barrio", "Distrito", "Zona", "Ciudad", "Provincia",

    # Energy & condition
    "Consumo 1", "Consumo 2", "Emisiones 1", "Emisiones 2",

    "estado", "gastos comunidad",

    "okupado", "Copropiedad", "con inquilino", "nuda propiedad",

    "tipo anunciante", "nombre anunciante",

    "Descripcion",
    
    "Fecha Scraping",

    # URL last as requested
    "URL"
)
from .utils import log
from .scraper import ScraperSession
