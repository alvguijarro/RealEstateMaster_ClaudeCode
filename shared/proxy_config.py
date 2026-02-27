"""Proxy configuration for 2Captcha and Scraper.
"""

PROXY_CONFIG = {
    'type': 'HTTP',
    'host': 'eu.proxy.2captcha.com',
    'port': 2333,
    'login': 'u5b30cedd579a05ca-zone-custom',
    'password': 'u5b30cedd579a05ca'
}

def get_proxy_uri():
    """Returns the proxy URI in format user:pass@host:port"""
    return f"{PROXY_CONFIG['login']}:{PROXY_CONFIG['password']}@{PROXY_CONFIG['host']}:{PROXY_CONFIG['port']}"

def get_2captcha_proxy_dict():
    """Returns proxy dictionary as expected by some 2Captcha SDK methods."""
    return {
        'type': PROXY_CONFIG['type'],
        'uri': get_proxy_uri()
    }

def get_2captcha_proxy_params():
    """Returns proxy parameters formatted for 2Captcha SDK as kwargs."""
    return {
        'proxytype': PROXY_CONFIG['type'],
        'proxyaddress': PROXY_CONFIG['host'],
        'proxyport': PROXY_CONFIG['port'],
        'proxylogin': PROXY_CONFIG['login'],
        'proxypassword': PROXY_CONFIG['password']
    }
