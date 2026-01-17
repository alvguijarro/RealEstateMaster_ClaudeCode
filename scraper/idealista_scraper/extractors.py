from __future__ import annotations
import re
from typing import List, Optional
from .utils import (
    fold_text, sanitize_units, split_location, normalize_price, digits_only,
    infer_tipo_from_title, get_comunidad
)
from .regex_patterns import (
    RX_M2_CONSTRUIDOS, RX_M2_UTILES, ORIENT_REGEX, YEAR_4D_RE, PARCELA_FALLBACK_RE,
    HABS_FALLBACK_RE, BANOS_FALLBACK_RE, PLANTAS_FALLBACK_RE, CONSTRUIDO_ANO_FALLBACK_RE,
    CONSUMO_KWH_RE, EMISIONES_KG_RE, ALT_PB_RE, ALT_PB_SIG_RE, ALT_PRAL_RE, ALT_PRAL_SIG_RE,
    ALT_ENTRE_RE, ALT_ATICO_RE, ALT_NUM_ORD_RE, ALT_NUM_FORM_RE, ALT_NUM_AFTER_RE,
    ALT_WORDS_RE, ALT_SNUM_RE, ALT_NEGNUM_RE, ALT_SEM_SOT_RE, ALT_SOT_RE,
    RX_ROOM_SIZE, RX_FLAT_SIZE_CONTEXT, RX_NUM_HAB_TOTAL, RX_GASTOS_INCLUIDOS, RX_AMUEBLADA, RX_PURE_M2,
    RX_TAMANO_HABITACION
)

def find_altura(raw: str) -> Optional[str]:
    """Floor/height parser with priority: labels -> numeric positives -> negatives."""
    if not raw:
        return None

    s = str(raw)
    sf = fold_text(s)

    if ALT_PB_RE.search(sf) or ALT_PB_SIG_RE.search(sf):
        return "bajo"
    if ALT_PRAL_RE.search(sf) or ALT_PRAL_SIG_RE.search(sf):
        return "principal"
    if ALT_ENTRE_RE.search(sf):
        return "entresuelo"
    if ALT_ATICO_RE.search(sf):
        return "ático"

    m = ALT_NUM_ORD_RE.search(s)
    if m:
        n = int(m.group(1))
        if 0 < n <= 30:
            return f"{n}º"
    m = ALT_NUM_FORM_RE.search(s)
    if m:
        n = int(m.group(1))
        if 0 < n <= 30:
            return f"{n}ª"
    m = ALT_NUM_AFTER_RE.search(s)
    if m:
        n = int(m.group(1))
        if 0 < n <= 30:
            return f"{n}ª"
    words = {
        "primera": 1, "segunda": 2, "tercera": 3, "cuarta": 4, "quinta": 5,
        "sexta": 6, "séptima": 7, "septima": 7, "octava": 8, "novena": 9,
        "décima": 10, "decima": 10, "undécima": 11, "undecima": 11,
        "duodécima": 12, "duodecima": 12
    }
    mw = ALT_WORDS_RE.search(s)
    if mw:
        n = words[mw.group(1).lower()]
        return f"{n}ª"

    m = ALT_SNUM_RE.search(s)
    if m:
        try:
            return f"-{int(m.group(1))}"
        except Exception:
            pass
    m = ALT_NEGNUM_RE.search(s)
    if m:
        try:
            return f"-{int(m.group(1))}"
        except Exception:
            pass
    if ALT_SEM_SOT_RE.search(s) or ALT_SOT_RE.search(s):
        return "-1"

    return None

from .dictionaries import BARRIO_TO_DISTRITO

def extract_location_details(map_lis: List[str]):
    """Parse Calle, Barrio, Distrito, Ciudad, Zona, Provincia from header map list items."""
    calle = barrio = distrito = ciudad = zona = provincia = None
    
    # First pass: Extract Ciudad from the last line with a comma (e.g., "Madrid capital, Madrid" -> "Madrid")
    # The last item in map_lis with a comma typically has format "Zone/Area, City"
    for raw in reversed(map_lis or []):
        txt = (raw or "").strip()
        if "," in txt:
            parts = txt.split(",")
            candidate_city = parts[-1].strip()
            # Validate: must be a valid province (which works for major cities like Madrid, Barcelona, etc.)
            if get_comunidad(candidate_city):
                ciudad = candidate_city
                provincia = candidate_city  # In Spain, major cities often match province name
                break
    
    # 1. Standard Extraction
    for raw in (map_lis or []):
        txt = (raw or "").strip()
        
        # Try to find valid province using our lookup (fallback if not found above)
        if "," in txt and provincia is None:
            parts = txt.split(",")
            candidate = parts[-1].strip()
            # If this candidate maps to a community, it's a valid province
            if get_comunidad(candidate):
                provincia = candidate
                
        if "," in txt and zona is None:
            if "zona" in txt.lower():
                potential_zona = txt.split(",")[0].strip()
                # Validation: Block street names being assigned to Zona
                if not re.match(r"(?i)^(cl|calle|traves[ií]a|avenida|ronda)\b", potential_zona):
                    zona = potential_zona
        if re.match(r"\s*calle\b", txt, flags=re.I) and calle is None:
            m = re.search(r"(?i)\bcalle\s+(.*)", txt)
            calle = (m.group(1).strip() if m else txt)
            continue
        if re.match(r"\s*barrio\b", txt, flags=re.I) and barrio is None:
            # Remove "Barrio " prefix - e.g., "Barrio Delicias" -> "Delicias"
            barrio = re.sub(r"(?i)^barrio\s+(de\s+)?", "", txt).strip()
            continue
        if re.match(r"\s*distrito\b", txt, flags=re.I) and distrito is None:
            # Remove "Distrito " prefix - e.g., "Distrito Arganzuela" -> "Arganzuela"
            distrito = re.sub(r"(?i)^distrito\s+", "", txt).strip()
            continue
        # Fallback ciudad extraction for simple entries (only if not already set)
        if ciudad is None and (not re.match(r"\s*(calle|barrio|distrito)\b", txt, flags=re.I)) and ("," not in txt):
            # Validation: Block street names and urbanization names from being assigned to Ciudad
            if not re.match(r"(?i)^(cl|calle|traves[ií]a|avenida|ronda|paseo|camino|plaza|glorieta|urb\.?|urbanizaci[oó]n)\b", txt):
                ciudad = txt
            
    # 2. Logic Inference / Dictionary Lookup
    if not distrito:
        # Strategy A: Infer from Barrio (Fastest)
        if barrio:
            # Clean "Barrio " prefix for lookup keys if needed, or keep map keys robust.
            # Our dict keys are like "Malasaña", but text might be "Barrio Malasaña"
            # Let's clean standard prefixes
            clean_b = re.sub(r"(?i)^barrio\s+(de\s+)?", "", barrio).strip()
            if clean_b in BARRIO_TO_DISTRITO:
                distrito = BARRIO_TO_DISTRITO[clean_b]  # No longer adding "Distrito " prefix
                
        # Strategy B: Clean "Zona X" breadcrumbs
        # Sometimes district is just "Centro" in the breadcrumbs but identified as a zone or generic text
        if not distrito and zona:
            # If 'Zona Centro' -> Distrito Centro
            if "centro" in zona.lower():
                distrito = "Centro"

    # 3. Ultimate Fallback: Small Villages/Towns
    # If it's a small town (no administrative districts), the "Distrito" is effectively the town itself.
    if not distrito and ciudad:
        distrito = ciudad

    return calle, barrio, distrito, ciudad, zona, provincia



async def extract_detail_fields(page, debug_items: bool = False, is_room_mode: bool = False) -> dict:
    """Main orchestrator for detail page field extraction using Page.evaluate + Python fallbacks."""
    data = await page.evaluate(r"""
      () => {
        const getText = sel => {
          const el = document.querySelector(sel);
          return el ? el.textContent.trim() : null;
        };

        const title = getText('.main-info__title-main, h1');
        const ubicFull = getText('.main-info__title-minor, .main-info__title-minor--bold, [data-test="address"]');

        let price = getText('.info-data-price, [itemprop="price"], .price-features, .info-price');
        if (!price && document.body) {
          const m = (document.body.innerText.match(/\d[\d\.,\s]*\s*€/)||[])[0]||null;
          price = m;
        }

        function stripActualizado(s) {
          if (!s) return null;
          const withoutAnuncio = s.replace(/^Anuncio\s*/i, '').trim();
          const m = withoutAnuncio.match(/actualizado hace\s*(.*)$/i);
          return m ? m[1].trim() : withoutAnuncio;
        }
        let actualizadoRaw = getText('.date-update-text, span.date-update-text, [data-test="lastUpdate"]');
        let actualizado = stripActualizado(actualizadoRaw);
        if (!actualizado && document.body) {
          const t = document.body.innerText;
          const m = t.match(/Anuncio actualizado hace[^\n]*/i);
          actualizado = stripActualizado(m ? m[0] : null);
        }

        let oldPriceRaw = null;
        const elOld1 = document.querySelector('.pricedown_price');
        const elOld2 = elOld1 ? null : document.querySelector('.pricedown-price');
        if (elOld1) oldPriceRaw = elOld1.textContent.trim();
        else if (elOld2) oldPriceRaw = elOld2.textContent.trim();

        let ppm2_raw = null;
        const detailEls = Array.from(document.querySelectorAll('.flex-feature-details'));
        for (const el of detailEls) {
          const t = (el.textContent || '').trim();
          if (/€\s*\/\s*m(?:²|2)/i.test(t)) { ppm2_raw = t; break; }
        }
        if (!ppm2_raw) {
          const pf = document.querySelector('.price-features__container');
          const t = pf ? pf.innerText : (document.body ? document.body.innerText : '');
          const m = (t.match(/([\d\.,\s]+)\s*€\s*\/\s*m(?:²|2)/i) || [])[1];
          ppm2_raw = m ? m : null;
        }

        function stripQuotesAny(s) {
          if (!s) return s;
          s = s.trim();
          if (/^["'“”‘’].*["'“”‘’]$/.test(s)) return s.slice(1, -1);
          return s;
        }
        function unescapeCssContent(s) {
          if (!s) return s;
          s = s.replace(/\\A/g, " ");
          s = s.replace(/\\"/g, '"').replace(/\\'/g, "'");
          s = s.replace(/\\([0-9a-fA-F]{1,6})\s?/g, (_, hex) => {
            try { return String.fromCodePoint(parseInt(hex, 16)); } catch(e) { return ""; }
          });
          return s;
        }
        function readCssContent(el) {
          try {
            const g = getComputedStyle(el, '::before').getPropertyValue('content') || '';
            let v = stripQuotesAny(g);
            if (v && v.toLowerCase() !== 'none' && v.toLowerCase() !== 'normal' && v !== '""' && v !== "\'\'") {
              return unescapeCssContent(v).trim();
            }
          } catch (e) {}
          try {
            const g = getComputedStyle(el, '::after').getPropertyValue('content') || '';
            let v = stripQuotesAny(g);
            if (v && v.toLowerCase() !== 'none' && v.toLowerCase() !== 'normal' && v !== '""' && v !== "\'\'") {
              return unescapeCssContent(v).trim();
            }
          } catch (e) {}
          return null;
        }

        const propLis = Array.from(document.querySelectorAll(
          '.details-property_features li, .details-property-features li, [class*="details-property"] li, [class*="property_features"] li, .details-features li, .item-detail li'
        ));
        function liText(el) {
            const clone = el.cloneNode(true);
            try { clone.querySelectorAll('a,script,style').forEach(n => n.remove()); } catch(e){}
            return (clone.textContent || '').trim();
        }
        const items = propLis.map(liText).filter(Boolean);

        let c1=null, c2=null, e1=null, e2=null;
        for (const li of propLis) {
          const txt = liText(li) || '';
          const spanIcon = li.querySelector('span[class*="icon-energy-c-"]');

          if (spanIcon) {
            const cls = spanIcon.className || '';
            const mm  = cls.match(/icon-energy-c-([a-g])/i);
            if (mm) {
              if (/consumo/i.test(txt))  c1 = (mm[1]||'').toUpperCase();
              if (/emisi/i.test(txt))    e1 = (mm[1]||'').toUpperCase();
            }
          }

          if (/consumo/i.test(txt)) {
            if (spanIcon) {
              const cssVal = readCssContent(spanIcon);
              if (cssVal) c2 = cssVal;
            }
            if (!c2) {
              const m = txt.match(/(\d[\d\.,]*)\s*kwh\s*\/\s*m(?:²|2)\s*año/i);
              if (m) c2 = m[0].trim();
            }
          }
          if (/emisi/i.test(txt)) {
            if (spanIcon) {
              const cssVal = readCssContent(spanIcon);
              if (cssVal) e2 = cssVal;
            }
            if (!e2) {
              const m = txt.match(/(\d[\d\.,]*)\s*kg\s*co2\s*\/\s*m(?:²|2)\s*año/i);
              if (m) e2 = m[0].trim();
            }
          }
        }

        const mapLis = Array.from(document.querySelectorAll('li.header-map-list')).map(el => (el.textContent || '').trim());

        let description = null;
        const descEl = document.querySelector('.adCommentsLanguage.expandable.is-expandable.with-expander-button');
        if (descEl) description = descEl.innerText.replace(/[\r\n]+/g, ' ').replace(/\s+/g, ' ').trim();

        let gastosComunidad = null;
        const gcSpans = Array.from(document.querySelectorAll('.price-features__container .flex-feature-details'));
        for (const sp of gcSpans) {
          const t = (sp.textContent || '').trim();
          if (/gastos\s+de\s+comunidad/i.test(t)) {
            // Extract just the amount (e.g., "50 €/mes" from "Gastos de comunidad 50 €/mes")
            const amountMatch = t.match(/(\d[\d\.,]*\s*€(?:\s*\/\s*mes)?)/i);
            gastosComunidad = amountMatch ? amountMatch[1].trim() : t.replace(/gastos\s+de\s+comunidad\s*/i, '').trim();
            break;
          }
        }

        const fullText = (document.body && document.body.innerText) ? document.body.innerText : '';

        // Extract advertiser type (Agency vs Particular)
        let advertiserType = null;
        let advertiserName = null;

        const profNameEl = document.querySelector('.professional-name');
        if (profNameEl) {
            const profText = profNameEl.textContent.trim();
            if (/particular/i.test(profText)) {
                advertiserType = "Particular";
                // Get name from advertiser-name or similar
                const nameEl = document.querySelector('.advertiser-name, .name');
                if (nameEl) advertiserName = nameEl.textContent.trim();
            } else if (/profesional/i.test(profText)) {
                advertiserType = "Agencia";
                // For agency, the name might be in a different element, but usually advertiser-name works too
                // or sometimes it's the element itself if it's just the badge
                const nameEl = document.querySelector('.advertiser-name, .name, .professional-name span');
                 if (nameEl) {
                    advertiserName = nameEl.textContent.trim();
                 } else {
                     // Fallback: sometimes the agency name is in the advertiser-data container
                     const logoEl = document.querySelector('.logo-branding');
                     if (logoEl) advertiserName = logoEl.getAttribute('alt') || logoEl.getAttribute('title');
                 }
                 
                 // If we still don't have a name but it's an agency, try to get it from context
                 if (!advertiserName) {
                     const container = document.querySelector('.advertiser-data');
                     if (container) advertiserName = container.textContent.replace('Profesional', '').trim();
                 }
            }
        }
        
        // Fallback if no professional-name element found
        if (!advertiserType) {
             const particularEl = document.querySelector('.owner-data .particular, [class*="particular"], .icon-particular');
             if (particularEl) {
                 advertiserType = "Particular";
                 const nameEl = document.querySelector('.advertiser-name');
                 if (nameEl) advertiserName = nameEl.textContent.trim();
             }
             
             const agencyEl = document.querySelector('.logo-branding, .agency-logo');
             if (agencyEl) {
                 advertiserType = "Agencia";
                 advertiserName = agencyEl.getAttribute('alt') || agencyEl.getAttribute('title');
             }
        }

        // Detect expired/removed listings
        const isExpired = /este anuncio ya no est[aá] publicado|anuncio no disponible|property not available/i.test(fullText);

        // Detect specific "Baja" date
        // Detect specific "Baja" date
        let lowDate = null;
        // Text is usually: "El anunciante lo dio de baja el 05/01/2026" or "ayer"
        // We look for this pattern in the full text
        const lowMatch = fullText.match(/El anunciante lo dio de baja\s*(?:el)?\s*((?:\d{2}\/\d{2}\/\d{4})|ayer|hoy|anteayer)/i);
        if (lowMatch) {
            lowDate = lowMatch[1];
        }

        return {
          title, ubicFull, price, items,
          actualizado, ppm2_raw, description, oldPriceRaw, mapLis,
          energy: { c1, c2, e1, e2 },
          gastosComunidad,
          fullText,
          advertiserType,
          advertiserName,
          isExpired,
          lowDate
        };
      }
    """)

    ubic, provincia = split_location(data.get("ubicFull"))
    title = data.get("title")
    price = normalize_price(data.get("price"))
    raw_items: List[str] = data.get("items") or []
    map_lis: List[str] = data.get("mapLis") or []
    actualizado_hace = data.get("actualizado")
    ppm2_raw = data.get("ppm2_raw")
    descripcion = data.get("description")
    old_price = normalize_price(data.get("oldPriceRaw"))
    energy = data.get("energy") or {}
    full_text = data.get("fullText") or ""
    gastos_comunidad = sanitize_units(data.get("gastosComunidad"))

    precio_por_m2 = normalize_price(ppm2_raw)

    tipo = None
    num_plantas: Optional[int] = None
    habs: Optional[object] = None  # puede ser int o "estudio"
    banos: Optional[int] = None
    estado = None
    m2_construidos: Optional[int] = None
    m2_utiles: Optional[int] = None
    terraza = garaje = armarios = trastero = calefaccion = None
    parcela: Optional[int] = None
    ascensor: Optional[str] = None
    orientacion: Optional[str] = None
    altura: Optional[str] = None
    piscina = "no"
    aa = "no"
    jardin = "no"
    construido_en: Optional[int] = None
    
    # Room specific
    habitacion_m2: Optional[int] = None
    piso_m2: Optional[int] = None
    num_habitaciones_total: Optional[int] = None
    gastos_incluidos: Optional[str] = "No"
    amueblada: Optional[str] = "No"

    calle, barrio, distrito, ciudad, zona, provincia_extracted = extract_location_details(map_lis)
    
    # Prioritize extracted province, fallback to split_location's (though split_location usually returns city/distrito)
    if provincia_extracted:
        provincia = provincia_extracted
        
    comunidad_autonoma = get_comunidad(provincia)

    try:
        features_joined = " ".join(raw_items)
    except Exception:
        features_joined = ""
    if not altura and re.search(r"\bchalet\b", fold_text(features_joined)):
        altura = "chalet"

    for raw in raw_items:
        txt = fold_text(raw)

        if ("m2" in txt or "m²" in txt or " m " in txt) and ("construid" in txt or "const." in txt):
            n = re.search(r"\d[\d\.\s,]*", raw)
            # Standard mode: this is the property size. Room mode: this might be flat size.
            val = normalize_price(n.group(0)) if n else None
            
            if is_room_mode:
                 # In room mode, "construidos" usually refers to the flat
                 piso_m2 = val if val else piso_m2
            else:
                 m2_construidos = val if val else m2_construidos
            continue

        if is_room_mode:
            # Try to find room size from "Tamaño de la habitación: X m²" (highest priority)
            m_tamano = RX_TAMANO_HABITACION.search(raw)
            if m_tamano:
                 habitacion_m2 = normalize_price(m_tamano.group(1))
                 continue
            
            # Alternative: room size patterns like "habitación de X m²"
            m_room = RX_ROOM_SIZE.search(raw)
            if m_room:
                 habitacion_m2 = normalize_price(m_room.group(1))
                 continue
            m_pure = RX_PURE_M2.search(txt)
            if m_pure:
                 # If we see just "12 m2" in a list, likely room size
                 habitacion_m2 = normalize_price(m_pure.group(1))
                 continue
            
            # Try to find context for flat size e.g. "en piso de 100 m2"
            m_flat = RX_FLAT_SIZE_CONTEXT.search(raw) or RX_FLAT_SIZE_CONTEXT.search(title)
            if m_flat:
                 piso_m2 = normalize_price(m_flat.group(1))
            
            # Total rooms in flat from info-features "4 hab."
            m_habs = RX_NUM_HAB_TOTAL.search(raw)
            if m_habs:
                 num_habitaciones_total = int(m_habs.group(1))
                 continue
                 
            # Furnished
            if RX_AMUEBLADA.search(raw):
                 amueblada = "Sí"
                 continue
        
        if ("m2" in txt or "m²" in txt or " m " in txt) and ("util" in txt):
            n = re.search(r"\d[\d\.\s,]*", raw)
            m2_utiles = normalize_price(n.group(0)) if n else m2_utiles
            continue

        if "habitac" in txt or "dormitorio" in txt:
            n = re.search(r"\d+", txt)
            habs = int(n.group(0)) if n else habs
            continue
        if "bañ" in txt or "ban" in txt or "aseo" in txt:
            n = re.search(r"\d+", txt)
            banos = int(n.group(0)) if n else banos
            continue

        if altura is None:
            alt = find_altura(raw)
            if alt:
                altura = alt
                continue
        if altura is None and ("bajo exterior" in txt or "bajo interior" in txt or "planta baja" in txt or re.search(r"\bpb\b", txt)):
            altura = "bajo"
            continue

        if "plantas" in txt:
            n = re.search(r"\d+", txt)
            num_plantas = int(n.group(0)) if n else num_plantas
            if altura is None:
                alt = find_altura(raw)
                altura = alt or altura
            continue
        if "parcela" in txt:
            val = digits_only(raw)
            parcela = val if val is not None else parcela
            continue
        if "construid" in txt or "construccion" in txt:
            m = YEAR_4D_RE.search(raw)
            construido_en = int(m.group(1)) if m else construido_en
            continue

        if "sin ascensor" in txt:
            ascensor = "No"
            continue
        if "con ascensor" in txt or ("ascensor" in txt and "sin ascensor" not in txt):
            ascensor = "Sí"
            continue

        if orientacion is None:
            m = ORIENT_REGEX.search(raw)
            if m:
                orientacion = f"orientación {m.group(1).lower()}"
                continue

        if "obra nueva" in txt:
            estado = "Obra nueva"
            continue
        if ("a reformar" in txt) or ("para reformar" in txt) or re.search(r"\breformar\b", txt):
            estado = "Para reformar"
            continue
        if ("segunda mano" in txt) or ("buen estado" in txt) or ("en buen estado" in txt) or ("reformado" in txt) or ("reforma integral" in txt):
            estado = "Segunda mano / buen estado"
            continue

        if "terraza" in txt or "balcon" in txt or "balcón" in raw.lower():
            terraza = "Sí"
            continue
        if "garaje" in txt or "aparcamiento" in txt or "plaza de garaje" in txt:
            garaje = "Sí"
            continue
        if "armario" in txt or "empotrado" in txt:
            armarios = "Sí"
            continue
        if "trastero" in txt:
            trastero = "Sí"
            continue
        if "calefacc" in txt or "radiador" in txt:
            calefaccion = "Sí"
            continue
        if "piscina" in txt:
            piscina = "sí"
            continue
        if "aire acond" in txt or "aire acondicionado" in txt or "a/a" in txt:
            aa = "sí"
            continue
        if "jardin" in txt or "jardín" in raw.lower():
            jardin = "sí"
            continue

    vt_full = " ".join([descripcion or "", full_text or ""])
    vt_full_fold = fold_text(vt_full)

    if m2_construidos is None:
        m = RX_M2_CONSTRUIDOS.search(vt_full)
        m2_construidos = normalize_price(m.group(1)) if m else None
    if m2_utiles is None:
        m = RX_M2_UTILES.search(vt_full)
        m2_utiles = normalize_price(m.group(1)) if m else None

    if habs is None:
        m = HABS_FALLBACK_RE.search(vt_full)
        if m:
            habs = int(m.group(1))
        elif descripcion and re.search(r"\bestudio\b", descripcion, re.I):
            habs = "estudio"

    if banos is None:
        m = BANOS_FALLBACK_RE.search(vt_full)
        banos = int(m.group(1)) if m else None
    if num_plantas is None:
        m = PLANTAS_FALLBACK_RE.search(vt_full)
        num_plantas = int(m.group(1)) if m else None
    if parcela is None:
        m = PARCELA_FALLBACK_RE.search(vt_full)
        parcela = digits_only(m.group(1)) if m else None
    if orientacion is None and descripcion:
        m = ORIENT_REGEX.search(descripcion)
        orientacion = m.group(1).lower() if m else None
    if altura is None:
        alt = find_altura(vt_full)
        altura = alt or altura
    if altura is None:
        if "bajo exterior" in vt_full_fold or "bajo interior" in vt_full_fold:
            altura = "bajo"
    if ascensor is None:
        if "sin ascensor" in vt_full_fold:
            ascensor = "No"
        elif "ascensor" in vt_full_fold:
            ascensor = "Sí"
    if construido_en is None:
        m = CONSTRUIDO_ANO_FALLBACK_RE.search(vt_full)
        if m:
            construido_en = int(m.group(1))
    if estado is None:
        if "obra nueva" in vt_full_fold:
            estado = "Obra nueva"
        elif ("a reformar" in vt_full_fold) or ("para reformar" in vt_full_fold) or re.search(r"\breformar\b", vt_full_fold):
            estado = "Para reformar"
        elif ("segunda mano" in vt_full_fold) or ("buen estado" in vt_full_fold) or ("en buen estado" in vt_full_fold) or ("reformado" in vt_full_fold) or ("reforma integral" in vt_full_fold):
            estado = "Segunda mano / buen estado"

    tipo = infer_tipo_from_title(title)

    price_change_decimal = None
    if price is not None and old_price and old_price > 0:
        price_change_decimal = (price - old_price) / old_price

    joined_items = " | ".join(raw_items).lower()
    joined_items_fold = fold_text(joined_items)

    # Enhanced Okupado detection
    okupado = None
    if "ocupada ilegalmente" in joined_items or "vivienda ocupada" in joined_items:
        okupado = "Sí"
    elif re.search(r"\b(okupa|ocupada ilegal|ocupante sin t[ií]tulo|ocupaci[oó]n ilegal|vivienda ocupada)\b", vt_full_fold):
        okupado = "Sí"

    # Enhanced Con Inquilino detection
    con_inquilino = None
    # First check: Look for "Alquilada" tag (exact match in tags)
    if "alquilada" in joined_items_fold:
        con_inquilino = "Sí"
    # Second check: Only if first criteria not met, check description
    elif re.search(r"\b(con inquilino|actualmente alquilado|arrendado)\b", vt_full_fold):
        con_inquilino = "Sí"

    # Enhanced Nuda Propiedad detection
    nuda_propiedad = None
    if re.search(r"\bnuda propiedad\b", joined_items_fold):
        nuda_propiedad = "Sí"
    elif re.search(r"\b(nuda propiedad|usufructo|solo la nuda)\b", vt_full_fold):
        nuda_propiedad = "Sí"

    # Copropiedad detection - check description for "copropiedad"
    copropiedad = None
    if descripcion and re.search(r"\bcopropiedad\b", descripcion, re.I):
        copropiedad = "Sí"

    consumo1 = energy.get("c1")
    consumo2 = sanitize_units(energy.get("c2"))
    emisiones1 = energy.get("e1")
    emisiones2 = sanitize_units(energy.get("e2"))
    if not consumo2:
        m = CONSUMO_KWH_RE.search(vt_full)
        if m:
            consumo2 = sanitize_units(m.group(0))
    if not emisiones2:
        m = EMISIONES_KG_RE.search(vt_full)
        if m:
            emisiones2 = sanitize_units(m.group(0))

    # Post-processing for rooms
    if is_room_mode:
        # Check gastos in description
        if RX_GASTOS_INCLUIDOS.search(vt_full_fold):
            gastos_incluidos = "Sí"
        
        # Fallback for room size if not found in list but description says "habitacion de X m2"
        if not habitacion_m2:
             m = RX_ROOM_SIZE.search(descripcion or "")
             if m: habitacion_m2 = normalize_price(m.group(1))
             
        # Fallback for flat size
        if not piso_m2:
             # Sometimes m2_construidos caught it if text was "100 m2 construidos"
             piso_m2 = m2_construidos
             
        # Fallback for num rooms
        if not num_habitaciones_total:
             m = RX_NUM_HAB_TOTAL.search(vt_full)
             if m: num_habitaciones_total = int(m.group(1))

    if is_room_mode:
        # Return column set specific to room rentals (habitaciones)
        # Order matches user specification exactly
        # NOTE: URL and Fecha Scraping are NOT included here - they are added by scraper_wrapper
        return {
            "Titulo": title,
            "price": price,
            "old price": old_price,
            "price change %": price_change_decimal,
            "Ubicacion": ubic,
            "actualizado hace": actualizado_hace,
            "habs": num_habitaciones_total or habs,  # Total rooms in the flat
            "m2_habs": habitacion_m2,  # Room size in m2
            "banos": banos,
            "Terraza": terraza,
            "Garaje": garaje,
            "Armarios": armarios,
            "Trastero": trastero,
            "Calefaccion": calefaccion,
            "ascensor": ascensor,
            "orientacion": orientacion,
            "altura": altura,
            "jardin": jardin,
            "piscina": piscina,
            "aire acond": aa,
            "Calle": calle,
            "Barrio": barrio,
            "Distrito": distrito,
            "Zona": zona,
            "Ciudad": ciudad,
            "Provincia": provincia,
            "estado": estado,
            "tipo anunciante": data.get("advertiserType"),
            "nombre anunciante": data.get("advertiserName"),
            "Descripcion": descripcion,
            "Anuncio activo": "No" if (data.get("lowDate") or data.get("isExpired")) else "Sí",
            "Baja anuncio": data.get("lowDate"),
            "Comunidad Autonoma": comunidad_autonoma,
        }


    return {
        "price": price,
        "old price": old_price,
        "price change %": price_change_decimal,
        "Titulo": title,
        "Ubicacion": ubic,
        "Provincia": provincia,
        "Comunidad Autonoma": comunidad_autonoma,
        "Calle": calle, "Barrio": barrio, "Distrito": distrito, "Ciudad": ciudad, "Zona": zona,
        "actualizado hace": actualizado_hace,
        "tipo": tipo, "Num plantas": num_plantas, "habs": habs, "banos": banos,
        "Descripcion": descripcion,
        "m2 construidos": m2_construidos, "m2 utiles": m2_utiles, "precio por m2": precio_por_m2,
        "Terraza": terraza, "Garaje": garaje, "Armarios": armarios, "Trastero": trastero, "Calefaccion": calefaccion,
        "parcela": parcela, "ascensor": ascensor, "orientacion": orientacion, "altura": altura,
        "construido en": construido_en, "jardin": jardin, "piscina": piscina, "aire acond": aa,
        "Consumo 1": consumo1, "Consumo 2": consumo2,
        "Emisiones 1": emisiones1, "Emisiones 2": emisiones2,
        "estado": estado, "gastos comunidad": gastos_comunidad,
        "okupado": okupado, "Copropiedad": copropiedad, "con inquilino": con_inquilino, "nuda propiedad": nuda_propiedad,
        "tipo anunciante": data.get("advertiserType"),
        "nombre anunciante": data.get("advertiserName"),
        "Anuncio activo": "No" if (data.get("lowDate") or data.get("isExpired")) else "Sí",
        "Baja anuncio": data.get("lowDate"),
    }

def missing_fields(row: dict, is_room_mode: bool = False):
    """Validation helper for required output fields.
    
    For room mode, we're less strict since habitaciones listings often have
    different data availability (e.g., Provincia may not be easily extractable).
    """
    if is_room_mode:
        # For rooms, only require the absolute essentials
        required = ["URL", "price", "Titulo"]
    else:
        required = ["URL", "price", "Titulo", "Ubicacion", "Provincia"]
    return [k for k in required if row.get(k) in (None, "", float("nan"))]

