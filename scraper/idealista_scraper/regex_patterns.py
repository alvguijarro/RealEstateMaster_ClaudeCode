"""Precompiled regex patterns for property data extraction.

This module centralizes all regular expression patterns used throughout the scraper
for extracting property details like measurements, floor numbers, orientations, etc.

All patterns are precompiled for performance, as they are used repeatedly
during the scraping process. Patterns are grouped logically by the type of
data they extract.
"""
from __future__ import annotations
import re


# =============================================================================
# Area/Size (m²) Patterns
# =============================================================================

RX_M2_NUM = r"(\d{1,3}(?:[.\s]\d{3})*|\d+)(?:[.,]\d+)?"
"""Base pattern for matching square meter numbers.

Matches formats like: 100, 1.500, 100,5, 1.500,75
Handles European number formatting with dots as thousand separators.
"""

RX_M2_CONSTRUIDOS = re.compile(RX_M2_NUM + r"\s*m(?:²|2)\s*(?:construidos|const\.?)", re.I)
"""Pattern to extract constructed area in m².

Examples that match:
- "100 m² construidos"
- "150 m2 const."
- "1.500,5 m² construidos"
"""

RX_M2_UTILES = re.compile(RX_M2_NUM + r"\s*m(?:²|2)\s*(?:útiles|utiles|utlies)", re.I)
"""Pattern to extract usable area in m².

Examples that match:
- "85 m² útiles"
- "90 m2 utiles"
- "100,5 m² útiles"

Note: Also matches common misspelling "utlies"
"""


# =============================================================================
# Orientation Patterns
# =============================================================================

ORIENT_REGEX = re.compile(r"orientaci[oó]n\s+(norte|noroeste|oeste|suroeste|sur|sureste|este|noreste)", re.I)
"""Pattern to extract property orientation (compass direction).

Examples that match:
- "orientación sur"
- "orientacion norte"
- "Orientación suroeste"

Captures: norte, sur, este, oeste, and combinations (noreste, suroeste, etc.)
"""


# =============================================================================
# URL Validation Patterns
# =============================================================================

CANONICAL_LISTING_RE = re.compile(r"(https?://[^/]+)/(?:[a-z]{2}/)?(inmueble[s]?/\d+/?)", re.I)
"""Pattern to normalize listing URLs by removing language prefixes.

Matches URLs like:
- "https://www.idealista.com/inmueble/12345/"
- "https://www.idealista.com/en/inmueble/12345/"
- "https://www.idealista.com/es/inmuebles/12345/"

Captures domain and property path separately for canonicalization.
"""

LISTING_URL_RE = re.compile(r"/inmueble[s]?/\d+", re.I)
"""Pattern to identify property listing URLs by their path structure.

Simple check for the presence of /inmueble/[numbers] or /inmuebles/[numbers]
"""


# =============================================================================
# Fallback Property Detail Extraction Patterns
# =============================================================================

YEAR_4D_RE = re.compile(r"((?:18|19|20)\d{2})")
"""Pattern to extract 4-digit years (1800-2099).

Used for extracting construction years when structured data is unavailable.
"""

PARCELA_FALLBACK_RE = re.compile(r"parcela[^\d]*(\d[\d\.]*)", re.I)
"""Pattern to extract plot/land size from text.

Examples that match:
- "parcela de 500"
- "Parcela: 1.200"
"""

HABS_FALLBACK_RE = re.compile(r"(\d+)\s*(?:hab(?:itaciones)?|dormitorios?)\b", re.I)
"""Pattern to extract number of bedrooms from text.

Examples that match:
- "3 habitaciones"
- "2 hab"
- "4 dormitorios"
"""

BANOS_FALLBACK_RE = re.compile(r"(\d+)\s*(?:bañ(?:os|o)|banos|aseos?)\b", re.I)
"""Pattern to extract number of bathrooms from text.

Examples that match:
- "2 baños"
- "1 baño"
- "2 aseos"
"""

PLANTAS_FALLBACK_RE = re.compile(r"(\d+)\s*plantas\b", re.I)
"""Pattern to extract number of floors/stories in a property.

Examples that match:
- "2 plantas"
- "3 Plantas"
"""

CONSTRUIDO_ANO_FALLBACK_RE = re.compile(r"(?:construido en|año de construcci[oó]n)\D*((?:18|19|20)\d{2})", re.I)
"""Pattern to extract construction year from descriptive text.

Examples that match:
- "construido en 2015"
- "año de construcción: 1980"
"""

CONSUMO_KWH_RE = re.compile(r"(\d[\d\.,]*)\s*kwh\s*\/\s*m(?:²|2)\s*año", re.I)
"""Pattern to extract energy consumption in kWh/m²/year.

Examples that match:
- "150 kWh/m² año"
- "120,5 kwh / m2 año"
"""

EMISIONES_KG_RE = re.compile(r"(\d[\d\.,]*)\s*kg\s*co2\s*\/\s*m(?:²|2)\s*año", re.I)
"""Pattern to extract CO2 emissions in kg/m²/year.

Examples that match:
- "30 kg CO2/m² año"
- "25,5 kg co2 / m2 año"
"""


# =============================================================================
# Floor/Height (Altura) Detection Patterns
# =============================================================================
# These patterns are ordered by priority for the find_altura() function.
# The first match found is used, so more specific patterns come first.

ALT_PB_RE = re.compile(r"\b(planta\s*baja|bajo)\b", re.I)
"""Matches ground floor descriptions: "planta baja" or "bajo" → returns 0"""

ALT_PB_SIG_RE = re.compile(r"\bpb\b", re.I)
"""Matches ground floor abbreviation: "pb" → returns 0"""

ALT_PRAL_RE = re.compile(r"\b(?:planta|piso)\s+principal\b", re.I)
"""Matches principal floor: "planta principal" or "piso principal" → returns 1"""

ALT_PRAL_SIG_RE = re.compile(r"\bpral\.?\b", re.I)
"""Matches principal floor abbreviation: "pral" or "pral." → returns 1"""

ALT_ENTRE_RE = re.compile(r"\b(entresuelo|entreplanta|ent\.?\s)\b", re.I)
"""Matches mezzanine/intermediate floor: "entresuelo" or "entreplanta" → returns 0.5"""

ALT_ATICO_RE = re.compile(r"\b(ático|atico)\b", re.I)
"""Matches penthouse: "ático" or "atico" → returns 999 (special marker)"""

ALT_NUM_ORD_RE = re.compile(r"\b(\d{1,2})\s*\.?\s*(º|ª|o|a)\b", re.I)
"""Matches ordinal numbers: "3º", "2ª", "4o" → extracts the number"""

ALT_NUM_FORM_RE = re.compile(r"\b(\d{1,2})\s*\.?\s*(º|ª|o|a)?\s*(planta|piso)\b", re.I)
"""Matches floor with explicit word: "3 planta", "2º piso" → extracts the number"""

ALT_NUM_AFTER_RE = re.compile(r"\b(planta|piso)\s*(\d{1,2})\b", re.I)
"""Matches floor number after word: "planta 3", "piso 2" → extracts the number"""

ALT_WORDS_RE = re.compile(
    r"\b(?:planta|piso)\s+(primera|segunda|tercera|cuarta|quinta|sexta|séptima|septima|octava|novena|décima|decima|undécima|undecima|duodécima|duodecima)\b",
    re.I,
)
"""Matches spelled-out floor numbers: "planta primera", "piso segunda" → converts to number"""

ALT_SNUM_RE = re.compile(r"\bS-?\s*(\d+)\b", re.I)
"""Matches basement notation: "S-1", "S1" → extracts as negative number"""

ALT_NEGNUM_RE = re.compile(r"(?<!\d)-\s*(\d+)\b")
"""Matches explicit negative floor: "-1", "-2" → extracts as negative number"""

ALT_SEM_SOT_RE = re.compile(r"\bsemis[oó]tano\b", re.I)
"""Matches semi-basement: "semisótano" → returns -0.5"""

ALT_SOT_RE = re.compile(r"\bs[oó]tano\b", re.I)
"""Matches basement: "sótano" → returns -1"""
