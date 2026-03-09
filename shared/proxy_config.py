"""Proxy configuration for 2Captcha and Scraper.

IMPORTANT: Replace the placeholder values below with your residential proxy credentials.
The proxy MUST support sticky sessions (same IP for several minutes) so that the browser
and 2Captcha solver use the same IP — otherwise DataDome rejects the cookie.

Recommended providers: Bright Data, IPRoyal, SmartProxy (residential, sticky 10-30 min).
The old 2Captcha rotating proxy (eu.proxy.2captcha.com:2333) does NOT work because it
rotates IPs between connections, causing IP mismatch between browser and solver.
"""
import random
import string


def _generate_session_id(length=12):
    """Generate a random sticky session ID (plain alphanumeric, no prefix)."""
    return ''.join(random.choices(string.ascii_lowercase + string.digits, k=length))


# Sticky session ID — regenerated each time the module is imported (i.e. each scraper run).
# This keeps the same IP for the duration of the session.
_STICKY_SESSION_ID = _generate_session_id()

PROXY_CONFIG = {
    'type': 'HTTP',
    # ── Bright Data Residential Proxy (residential_proxy1) ──
    'host': 'brd.superproxy.io',
    'port': 33335,
    'login': 'brd-customer-hl_e2c01f5d-zone-residential_proxy1-country-es',  # -country-es fuerza IPs españolas
    'password': 'utd291dsjrds',
    # ── Sticky session support ──
    'sticky_session_id': _STICKY_SESSION_ID,
}


PROXY_CONFIG_GLOBAL = {
    **PROXY_CONFIG,
    # Sin -country-es: usa IPs de cualquier país del pool residencial de Bright Data.
    # Útil cuando el pool español está masivamente bloqueado por DataDome para Idealista.
    # Browser y solver 2Captcha usan el mismo proxy (misma IP no española) → sin IP mismatch.
    'login': 'brd-customer-hl_e2c01f5d-zone-residential_proxy1',
}


def regenerate_session():
    """Generate a new sticky session ID (new Bright Data exit IP)."""
    global _STICKY_SESSION_ID
    _STICKY_SESSION_ID = _generate_session_id()
    PROXY_CONFIG['sticky_session_id'] = _STICKY_SESSION_ID
    return _STICKY_SESSION_ID


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
