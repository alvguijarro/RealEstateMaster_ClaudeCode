import re

# Generic M2
RX_M2_CONSTRUIDOS = re.compile(r"(\d+(?:[\.,]\d+)?)\s*(?:m²|m2|\s*m\s*)\s*const", re.IGNORECASE)
RX_M2_UTILES = re.compile(r"(\d+(?:[\.,]\d+)?)\s*(?:m²|m2|\s*m\s*)\s*util", re.IGNORECASE)

# Basics
ORIENT_REGEX = re.compile(r"orientaci[oó]n\s+([a-zA-Z\s]+)", re.IGNORECASE)
YEAR_4D_RE = re.compile(r"\b((?:18|19|20)\d{2})\b")

# Fallbacks for text search
PARCELA_FALLBACK_RE = re.compile(r"parcela\s*(?:de)?\s*(\d+(?:[\.,]\d+)?)", re.IGNORECASE)
HABS_FALLBACK_RE = re.compile(r"(\d+)\s*(?:hab|dorm)", re.IGNORECASE)
BANOS_FALLBACK_RE = re.compile(r"(\d+)\s*(?:bañ|aseo)", re.IGNORECASE)
PLANTAS_FALLBACK_RE = re.compile(r"(\d+)\s*planta", re.IGNORECASE)
CONSTRUIDO_ANO_FALLBACK_RE = re.compile(r"construido\s*en\s*(\d{4})", re.IGNORECASE)

# Energy
CONSUMO_KWH_RE = re.compile(r"(\d+(?:[\.,]\d+)?)\s*kWh/m²", re.IGNORECASE)
EMISIONES_KG_RE = re.compile(r"(\d+(?:[\.,]\d+)?)\s*kg\s*CO2/m²", re.IGNORECASE)

# Floor / Altura
ALT_PB_RE = re.compile(r"\b(bajo|planta\s*baja|pb)\b", re.IGNORECASE)
ALT_PB_SIG_RE = re.compile(r"^bj\b", re.IGNORECASE)
ALT_PRAL_RE = re.compile(r"\b(principal|pral)\b", re.IGNORECASE)
ALT_PRAL_SIG_RE = re.compile(r"^pr\b", re.IGNORECASE)
ALT_ENTRE_RE = re.compile(r"\b(entresuelo|entreplanta)\b", re.IGNORECASE)
ALT_ATICO_RE = re.compile(r"\b(atico|ático)\b", re.IGNORECASE)

ALT_NUM_ORD_RE = re.compile(r"(\d+)(?:º|ª|er|o|a)?\s*planta", re.IGNORECASE)
ALT_NUM_FORM_RE = re.compile(r"planta\s*(\d+)", re.IGNORECASE)
ALT_NUM_AFTER_RE = re.compile(r"piso\s*(\d+)", re.IGNORECASE) # e.g. Piso 3
ALT_WORDS_RE = re.compile(r"\b(primera|segunda|tercera|cuarta|quinta|sexta|septima|octava|novena|decima)\b", re.IGNORECASE)
ALT_SNUM_RE = re.compile(r"^(\d+)(?:º|ª)?$", re.IGNORECASE) # just a number
ALT_NEGNUM_RE = re.compile(r"sótano|sotano", re.IGNORECASE) # Simplified
ALT_SEM_SOT_RE = re.compile(r"semisótano|semisotano", re.IGNORECASE)
ALT_SOT_RE = re.compile(r"^sótano|^sotano", re.IGNORECASE)


# --- NEW: Room Rental Patterns ---
RX_ROOM_SIZE = re.compile(r"(?:habitaci[oó]n|hab\.|dormitorio)\s*(?:de)?\s*(\d+(?:[\.,]\d+)?)\s*(?:m²|m2)", re.IGNORECASE)
# Matches "Tamaño de la habitación: 9 m²" pattern from details_property_features
RX_TAMANO_HABITACION = re.compile(r"tama[ñn]o\s*(?:de\s*la)?\s*habitaci[oó]n[:.\s]*(\d+(?:[\.,]\d+)?)\s*(?:m²|m2)?", re.IGNORECASE)
# Matches pure size in features list often just "12 m²"
RX_PURE_M2 = re.compile(r"^(\d+(?:[\.,]\d+)?)\s*(?:m²|m2)$", re.IGNORECASE)

RX_FLAT_SIZE_CONTEXT = re.compile(r"(?:en|de)\s*piso\s*(?:de)?\s*(\d+(?:[\.,]\d+)?)", re.IGNORECASE)
# Matches "4 hab." from info-features span
RX_NUM_HAB_TOTAL = re.compile(r"(\d+)\s*hab", re.IGNORECASE)

RX_GASTOS_INCLUIDOS = re.compile(r"(gastos\s*incluidos|facturas\s*incluidas)", re.IGNORECASE)
RX_AMUEBLADA = re.compile(r"(amueblada|con\s*muebles|armario\s*empotrado)", re.IGNORECASE)

