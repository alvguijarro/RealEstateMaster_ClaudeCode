"""Proxy configuration for 2Captcha and Scraper.

IMPORTANT: Replace the placeholder values below with your residential proxy credentials.
The proxy MUST support sticky sessions (same IP for several minutes) so that the browser
and 2Captcha solver use the same IP — otherwise DataDome rejects the cookie.

Recommended providers: Bright Data, IPRoyal, SmartProxy (residential, sticky 10-30 min).
The old 2Captcha rotating proxy (eu.proxy.2captcha.com:2333) does NOT work because it
rotates IPs between connections, causing IP mismatch between browser and solver.

Multi-worker support:
  Copy shared/.env.proxy.example → shared/.env.proxy and fill in credentials.
  Each worker selects its proxy via SCRAPER_WORKER_ID env var:
    SCRAPER_WORKER_ID=1  →  PROXY_1_*
    SCRAPER_WORKER_ID=2  →  PROXY_2_*
    SCRAPER_WORKER_ID=3  →  PROXY_3_*
  If .env.proxy does not exist, falls back to the hardcoded credentials below.
"""
import os
import random
import string
from pathlib import Path


def _generate_session_id(length=12):
    """Generate a random sticky session ID (plain alphanumeric, no prefix)."""
    return ''.join(random.choices(string.ascii_lowercase + string.digits, k=length))


# ── Auto-cargar shared/.env.proxy si existe ───────────────────────────────────
def _load_env_file():
    env_file = Path(__file__).parent / '.env.proxy'
    if not env_file.exists():
        return
    with open(env_file, encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#') or '=' not in line:
                continue
            key, _, val = line.partition('=')
            os.environ.setdefault(key.strip(), val.strip())

_load_env_file()


# ── Construir pool de proxies desde PROXY_1_*, PROXY_2_*, ... ─────────────────
def _build_proxy_from_env(prefix: str):
    host = os.environ.get(f'{prefix}_HOST')
    if not host:
        return None
    return {
        'type': 'HTTP',
        'host': host,
        'port': int(os.environ.get(f'{prefix}_PORT', 33335)),
        'login': os.environ.get(f'{prefix}_LOGIN', ''),
        'password': os.environ.get(f'{prefix}_PASS', ''),
        'sticky_session_id': _generate_session_id(),
    }

_PROXY_POOL = []
for _i in range(1, 10):
    _p = _build_proxy_from_env(f'PROXY_{_i}')
    if _p:
        _PROXY_POOL.append(_p)


# ── Seleccionar proxy según SCRAPER_WORKER_ID ─────────────────────────────────
_selected_proxy = None
if _PROXY_POOL:
    _worker_id_str = os.environ.get('SCRAPER_WORKER_ID', '1')
    try:
        _idx = (int(_worker_id_str) - 1) % len(_PROXY_POOL)
    except ValueError:
        _idx = 0
    _selected_proxy = _PROXY_POOL[_idx]


# ── PROXY_CONFIG: proxy activo para este proceso ──────────────────────────────
if _selected_proxy:
    PROXY_CONFIG = _selected_proxy
else:
    # Fallback hardcodeado — comportamiento idéntico al original si no hay .env.proxy
    PROXY_CONFIG = {
        'type': 'HTTP',
        # ── Bright Data Residential Proxy (residential_proxy1) ──
        'host': 'brd.superproxy.io',
        'port': 33335,
        'login': 'brd-customer-hl_e2c01f5d-zone-residential_proxy1',
        'password': 'utd291dsjrds',
        # ── Sticky session support ──
        'sticky_session_id': _generate_session_id(),
    }

PROXY_CONFIG_GLOBAL = {
    **PROXY_CONFIG,
    # Sin -country-es: usa IPs de cualquier país del pool residencial de Bright Data.
    # Útil cuando el pool español está masivamente bloqueado por DataDome para Idealista.
    # Browser y solver 2Captcha usan el mismo proxy (misma IP no española) → sin IP mismatch.
    'login': PROXY_CONFIG['login'],
}


def regenerate_session():
    """Generate a new sticky session ID (new Bright Data exit IP)."""
    new_id = _generate_session_id()
    PROXY_CONFIG['sticky_session_id'] = new_id
    return new_id


def get_proxy_uri():
    """Returns the proxy URI in format user:pass@host:port.
    Appends sticky session ID if the provider uses it in the username
    (common pattern: user-session-XXXX:pass@host:port).
    """
    login = PROXY_CONFIG['login']
    sid = PROXY_CONFIG.get('sticky_session_id')
    if sid:
        # Most residential proxies accept session in the username: user-session-XXXX
        login = f"{login}-session-{sid}"
    return f"{login}:{PROXY_CONFIG['password']}@{PROXY_CONFIG['host']}:{PROXY_CONFIG['port']}"


def get_2captcha_proxy_dict():
    """Returns proxy dictionary as expected by some 2Captcha SDK methods."""
    return {
        'type': PROXY_CONFIG['type'],
        'uri': get_proxy_uri()
    }


def get_2captcha_proxy_params():
    """Returns proxy parameters formatted for 2Captcha SDK as kwargs."""
    login = PROXY_CONFIG['login']
    sid = PROXY_CONFIG.get('sticky_session_id')
    if sid:
        login = f"{login}-session-{sid}"
    return {
        'proxytype': PROXY_CONFIG['type'],
        'proxyaddress': PROXY_CONFIG['host'],
        'proxyport': PROXY_CONFIG['port'],
        'proxylogin': login,
        'proxypassword': PROXY_CONFIG['password']
    }
