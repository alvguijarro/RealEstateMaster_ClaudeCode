"""
Utilidades de navegador para el Trends Tracker.

Copia aislada de get_browser_executable_path y generate_stealth_script
de scraper/app/scraper_wrapper.py, sin ningún estado compartido con el
scraper principal.
"""
import os
import sys
import random
from pathlib import Path
from typing import Optional

# trends/ -> RealEstateMaster/
_PROJECT_ROOT = Path(__file__).parent.parent

GPU_FINGERPRINTS = [
    ("NVIDIA Corporation", "NVIDIA GeForce RTX 3060/PCIe/SSE2"),
    ("NVIDIA Corporation", "NVIDIA GeForce GTX 1660 Ti/PCIe/SSE2"),
    ("NVIDIA Corporation", "NVIDIA GeForce RTX 2070 SUPER/PCIe/SSE2"),
    ("AMD", "AMD Radeon RX 6700 XT"),
    ("AMD", "AMD Radeon RX 580 Series"),
    ("Intel", "Intel(R) UHD Graphics 630"),
    ("Intel", "Intel(R) Iris(R) Xe Graphics"),
    ("NVIDIA Corporation", "NVIDIA GeForce GTX 1080 Ti/PCIe/SSE2"),
    ("AMD", "AMD Radeon RX 5700 XT"),
]


def get_random_gpu():
    """Selecciona un fingerprint de GPU aleatorio para esta sesión."""
    return random.choice(GPU_FINGERPRINTS)


def get_browser_executable_path(channel: Optional[str]) -> Optional[str]:
    """Detecta la ruta del ejecutable para navegadores personalizados (Brave, Opera, etc.)."""
    if not channel or channel in ["chrome", "msedge"]:
        return None

    if sys.platform != "win32":
        return None

    local_app_data = os.environ.get("LOCALAPPDATA", "")
    program_files = os.environ.get("ProgramFiles", "C:\\Program Files")
    program_files_x86 = os.environ.get("ProgramFiles(x86)", "C:\\Program Files (x86)")

    possible_browsers_dirs = [
        _PROJECT_ROOT / "python_portable" / "browsers"
    ]

    browsers_dir = None
    for d in possible_browsers_dirs:
        if d.exists():
            browsers_dir = str(d)
            break

    if not browsers_dir:
        browsers_dir = str(_PROJECT_ROOT / "python_portable" / "browsers")
        try:
            os.makedirs(browsers_dir, exist_ok=True)
        except:
            pass

    if channel == "chrome":
        paths = [
            os.path.join(browsers_dir, "GoogleChromePortable", "App", "Chrome-bin", "chrome.exe"),
            os.path.join(browsers_dir, "GoogleChromePortable", "GoogleChromePortable.exe"),
        ]
        for p in paths:
            if os.path.exists(p):
                return p
        return None

    elif channel == "opera":
        paths = [
            os.path.join(browsers_dir, "OperaPortable", "App", "Opera", "opera.exe"),
            os.path.join(browsers_dir, "OperaPortable", "OperaPortable.exe"),
            os.path.join(browsers_dir, "Opera", "opera.exe"),
            os.path.join(local_app_data, "Programs", "Opera", "opera.exe"),
            os.path.join(program_files, "Opera", "opera.exe"),
        ]
        for p in paths:
            if os.path.exists(p):
                return p

    elif channel == "iron":
        paths = [
            os.path.join(browsers_dir, "IronPortable", "App", "Iron", "iron.exe"),
            os.path.join(browsers_dir, "IronPortable", "IronPortable.exe"),
        ]
        for p in paths:
            if os.path.exists(p):
                return p

    elif channel == "falkon":
        paths = [
            os.path.join(browsers_dir, "FalkonPortable", "App", "Falkon", "falkon.exe"),
            os.path.join(browsers_dir, "FalkonPortable", "FalkonPortable.exe"),
        ]
        for p in paths:
            if os.path.exists(p):
                return p

    elif channel == "brave":
        paths = [
            os.path.join(browsers_dir, "BravePortable", "App", "Brave", "brave.exe"),
            os.path.join(browsers_dir, "BravePortable", "BravePortable.exe"),
            os.path.join(browsers_dir, "Brave", "brave.exe"),
            os.path.join(program_files, "BraveSoftware", "Brave-Browser", "Application", "brave.exe"),
        ]
        for p in paths:
            if os.path.exists(p):
                return p

    elif channel == "vivaldi":
        paths = [
            os.path.join(browsers_dir, "VivaldiPortable", "App", "Vivaldi", "Application", "vivaldi.exe"),
            os.path.join(browsers_dir, "VivaldiPortable", "VivaldiPortable.exe"),
            os.path.join(browsers_dir, "Vivaldi", "Application", "vivaldi.exe"),
            os.path.join(program_files, "Vivaldi", "Application", "vivaldi.exe"),
        ]
        for p in paths:
            if os.path.exists(p):
                return p

    return None


def generate_stealth_script(gpu_vendor=None, gpu_renderer=None):
    """Genera el script de stealth con fingerprint de GPU aleatorizado por sesión."""
    if gpu_vendor is None or gpu_renderer is None:
        gpu_vendor, gpu_renderer = get_random_gpu()
    return f'''
// ==================== PHASE 1: DEEP FINGERPRINT SPOOFING ====================

// 1. Remove Chrome DevTools Protocol (CDP) signatures
try {{
    if (window.chrome && window.chrome.runtime) {{
        delete window.chrome.runtime;
    }}
}} catch (e) {{}}

// 2. Spoof WebGL to match a real GPU (randomized per session)
try {{
    const getParameterProto = WebGLRenderingContext.prototype.getParameter;
    WebGLRenderingContext.prototype.getParameter = function(param) {{
        if (param === 37445) return '{gpu_vendor}';
        if (param === 37446) return '{gpu_renderer}';
        return getParameterProto.call(this, param);
    }};
    if (typeof WebGL2RenderingContext !== 'undefined') {{
        const getParameter2Proto = WebGL2RenderingContext.prototype.getParameter;
        WebGL2RenderingContext.prototype.getParameter = function(param) {{
            if (param === 37445) return '{gpu_vendor}';
            if (param === 37446) return '{gpu_renderer}';
            return getParameter2Proto.call(this, param);
        }};
    }}
}} catch (e) {{}}

// 3. Add realistic navigator.plugins (PluginArray-like with Symbol.iterator)
try {{
    const pluginData = [
        {{type: 'application/x-google-chrome-pdf', suffixes: 'pdf', description: 'Portable Document Format', name: 'Chrome PDF Plugin'}},
        {{type: 'application/pdf', suffixes: 'pdf', description: '', name: 'Chrome PDF Viewer'}},
        {{type: 'application/x-nacl', suffixes: '', description: 'Native Client Executable', name: 'Native Client'}}
    ];
    const plugins = Object.create(PluginArray.prototype);
    pluginData.forEach((p, i) => {{ plugins[i] = p; }});
    Object.defineProperty(plugins, 'length', {{value: pluginData.length, writable: false, enumerable: true}});
    plugins[Symbol.iterator] = function*() {{ for (let i = 0; i < this.length; i++) yield this[i]; }};
    plugins.item = function(i) {{ return this[i] || null; }};
    plugins.namedItem = function(name) {{ for (let i = 0; i < this.length; i++) {{ if (this[i].name === name) return this[i]; }} return null; }};
    plugins.refresh = function() {{}};
    Object.defineProperty(navigator, 'plugins', {{
        get: () => plugins
    }});
}} catch (e) {{}}

// 4. Fix navigator.languages
try {{
    Object.defineProperty(navigator, 'languages', {{
        get: () => ['es-ES', 'es', 'en-US', 'en']
    }});
}} catch (e) {{}}

// 5. Patch Permissions API
try {{
    const originalQuery = window.navigator.permissions.query;
    window.navigator.permissions.query = (params) => {{
        if (params.name === 'notifications') {{
            return Promise.resolve({{state: 'denied', onchange: null}});
        }}
        return originalQuery.call(window.navigator.permissions, params);
    }};
}} catch (e) {{}}

// 6. Timing randomization
try {{
    const originalNow = Date.now;
    const randomOffset = Math.floor(Math.random() * 50);
    Date.now = function() {{
        return originalNow() + randomOffset;
    }};
}} catch (e) {{}}

// 7. Override connection info (jittered per session)
try {{
    const rtt = [50, 75, 100, 150][Math.floor(Math.random() * 4)];
    const downlink = [1.5, 5, 10, 15][Math.floor(Math.random() * 4)];
    Object.defineProperty(navigator, 'connection', {{
        get: () => ({{
            effectiveType: '4g',
            rtt: rtt,
            downlink: downlink,
            saveData: false
        }})
    }});
}} catch (e) {{}}

// 8. Hide automation indicators
try {{
    delete window.cdc_adoQpoasnfa76pfcZLmcfl_Array;
    delete window.cdc_adoQpoasnfa76pfcZLmcfl_Promise;
    delete window.cdc_adoQpoasnfa76pfcZLmcfl_Symbol;
    Object.defineProperty(navigator, 'webdriver', {{
        get: () => undefined
    }});
}} catch (e) {{}}

// 9. Extra Hardware Randomization (varied per session)
try {{
    const cores = [4, 6, 8, 12, 16][Math.floor(Math.random() * 5)];
    const mem = [4, 8, 8, 16][Math.floor(Math.random() * 4)];
    Object.defineProperty(navigator, 'hardwareConcurrency', {{ get: () => cores }});
    Object.defineProperty(navigator, 'deviceMemory', {{ get: () => mem }});
}} catch (e) {{}}

// 11. ADVANCED: Canvas Noise Fingerprinting (toDataURL + getImageData)
try {{
    const originalToDataURL = HTMLCanvasElement.prototype.toDataURL;
    HTMLCanvasElement.prototype.toDataURL = function(type) {{
        if (type === 'image/png') {{
            const ctx = this.getContext('2d');
            if (ctx) {{
                const imageData = ctx.getImageData(0, 0, 1, 1);
                imageData.data[0] = (imageData.data[0] + 1) % 255;
                ctx.putImageData(imageData, 0, 0);
            }}
        }}
        return originalToDataURL.apply(this, arguments);
    }};
    const origGetImageData = CanvasRenderingContext2D.prototype.getImageData;
    CanvasRenderingContext2D.prototype.getImageData = function() {{
        const data = origGetImageData.apply(this, arguments);
        data.data[0] = (data.data[0] + 1) % 255;
        return data;
    }};
}} catch (e) {{}}

// 12. ADVANCED: WebRTC IP Protection (Complete — blocks all real IP leaks via STUN/TURN)
try {{
    const RTCStub = function() {{
        return {{
            close: () => {{}},
            createDataChannel: () => ({{}}),
            createOffer: () => Promise.resolve({{}}),
            createAnswer: () => Promise.resolve({{}}),
            setLocalDescription: () => Promise.resolve(),
            setRemoteDescription: () => Promise.resolve(),
            addIceCandidate: () => Promise.resolve(),
            addEventListener: () => {{}},
            removeEventListener: () => {{}},
            getStats: () => Promise.resolve(new Map()),
            getSenders: () => [],
            getReceivers: () => [],
            onicecandidate: null,
            ontrack: null,
            ondatachannel: null,
            onnegotiationneeded: null,
            onsignalingstatechange: null,
            oniceconnectionstatechange: null,
            onicegatheringstatechange: null,
            onconnectionstatechange: null,
            signalingState: 'closed',
            iceConnectionState: 'closed',
            connectionState: 'closed',
            iceGatheringState: 'complete'
        }};
    }};
    if (window.RTCPeerConnection) {{
        window.RTCPeerConnection = RTCStub;
    }}
    if (window.webkitRTCPeerConnection) {{
        window.webkitRTCPeerConnection = RTCStub;
    }}
}} catch (e) {{}}

// 13. ADVANCED: Font List Obfuscation
try {{
    const originalFT = document.fonts.check;
    document.fonts.check = function(font) {{
        const standardFonts = ['arial', 'times new roman', 'helvetica', 'sans-serif'];
        if (standardFonts.some(f => font.toLowerCase().includes(f))) {{
            return originalFT.apply(document.fonts, arguments);
        }}
        return false;
    }};
}} catch (e) {{}}

// 14. MODERN: userAgentData Spoofing
try {{
    if (navigator.userAgentData) {{
        const majorVersion = (navigator.userAgent.match(/Chrome\\/(\\d+)/) || [null, '137'])[1];
        const brands = [
            {{ brand: 'Not(A:Brand', version: '99' }},
            {{ brand: 'Google Chrome', version: majorVersion }},
            {{ brand: 'Chromium', version: majorVersion }}
        ];
        Object.defineProperty(navigator, 'userAgentData', {{
            get: () => ({{
                brands: brands,
                mobile: false,
                platform: 'Windows',
                getHighEntropyValues: (hints) => Promise.resolve({{
                    brands: brands,
                    mobile: false,
                    platform: 'Windows',
                    platformVersion: ['10.0.0', '15.0.0'][Math.floor(Math.random() * 2)],
                    architecture: 'x86',
                    model: '',
                    uaFullVersion: `${{majorVersion}}.0.0.0`
                }})
            }})
        }});
    }}
}} catch (e) {{}}

'''
