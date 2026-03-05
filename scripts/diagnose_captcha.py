#!/usr/bin/env python
"""Diagnóstico del pipeline de captchas DataDome.

Prueba cada componente del pipeline de forma aislada y reporta PASS/FAIL
para localizar el punto exacto de fallo.

Uso:
    python_portable/python.exe scripts/diagnose_captcha.py
    python_portable/python.exe scripts/diagnose_captcha.py --no-browser
    python_portable/python.exe scripts/diagnose_captcha.py --verbose
    python_portable/python.exe scripts/diagnose_captcha.py --headless
"""
import argparse
import asyncio
import json
import sys
import os
import tempfile
import shutil

# Forzar UTF-8 en stdout/stderr para Windows
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

# Añadir raíz del proyecto al path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import httpx

# ─── ANSI colors ─────────────────────────────────────────────────────────────
GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
CYAN = "\033[96m"
BOLD = "\033[1m"
RESET = "\033[0m"

STATUS_COLORS = {
    "PASS": GREEN,
    "FAIL": RED,
    "WARN": YELLOW,
    "SKIP": CYAN,
    "INFO": CYAN,
}


class DiagResult:
    """Resultado de un paso de diagnóstico."""
    def __init__(self, step: int, name: str, status: str, detail: str = ""):
        self.step = step
        self.name = name
        self.status = status  # PASS, FAIL, WARN, SKIP
        self.detail = detail

    def print(self, verbose=False):
        color = STATUS_COLORS.get(self.status, RESET)
        tag = f"{color}[{self.status:4s}]{RESET}"
        print(f"  {tag}  #{self.step:2d}  {self.name}")
        if self.detail and (verbose or self.status in ("FAIL", "WARN")):
            for line in self.detail.strip().split("\n"):
                print(f"              {line}")


class CaptchaDiagnostic:
    """Diagnóstico completo del pipeline de captchas DataDome."""

    def __init__(self, no_browser=False, verbose=False, headless=True):
        self.no_browser = no_browser
        self.verbose = verbose
        self.headless = headless
        self.results: list[DiagResult] = []

        # State compartido entre fases
        self.proxy_ip = None
        self.browser_ip = None
        self.browser_ua = None
        self.captcha_url = None
        self.captcha_detected = False
        self.browser_ok = False
        self.page = None
        self.browser = None
        self.context = None
        self.profile_dir = None

    def _add(self, step, name, status, detail=""):
        r = DiagResult(step, name, status, detail)
        self.results.append(r)
        r.print(self.verbose)
        return r

    # ===========================================================================
    # FASE 1: Configuración
    # ===========================================================================
    async def phase1_config(self):
        print(f"\n{BOLD}=== Fase 1: Configuracion ==={RESET}")

        # Step 1: TWOCAPTCHA_API_KEY
        try:
            from shared.config import TWOCAPTCHA_API_KEY
            if TWOCAPTCHA_API_KEY and len(TWOCAPTCHA_API_KEY) > 5:
                self._add(1, "TWOCAPTCHA_API_KEY presente", "PASS",
                          f"Key: ...{TWOCAPTCHA_API_KEY[-4:]}")
            else:
                self._add(1, "TWOCAPTCHA_API_KEY presente", "FAIL",
                          "Key vacía o demasiado corta")
        except Exception as e:
            self._add(1, "TWOCAPTCHA_API_KEY presente", "FAIL", str(e))

        # Step 2: PROXY_CONFIG completo
        try:
            from shared.proxy_config import PROXY_CONFIG
            required = ['host', 'port', 'login', 'password', 'sticky_session_id']
            missing = [k for k in required if not PROXY_CONFIG.get(k)]
            if not missing:
                self._add(2, "PROXY_CONFIG completo", "PASS",
                          f"Host: {PROXY_CONFIG['host']}:{PROXY_CONFIG['port']}")
            else:
                self._add(2, "PROXY_CONFIG completo", "FAIL",
                          f"Campos faltantes: {', '.join(missing)}")
        except Exception as e:
            self._add(2, "PROXY_CONFIG completo", "FAIL", str(e))

        # Step 3: Balance 2Captcha
        try:
            from shared.config import TWOCAPTCHA_API_KEY
            async with httpx.AsyncClient(timeout=15) as client:
                resp = await client.post(
                    "https://api.2captcha.com/getBalance",
                    json={"clientKey": TWOCAPTCHA_API_KEY}
                )
                data = resp.json()
                balance = data.get("balance", 0)
                if balance and float(balance) > 0:
                    self._add(3, "Balance 2Captcha", "PASS",
                              f"${float(balance):.2f}")
                else:
                    self._add(3, "Balance 2Captcha", "FAIL",
                              f"Saldo: ${float(balance):.2f}" if balance else f"Respuesta: {data}")
        except Exception as e:
            self._add(3, "Balance 2Captcha", "FAIL", str(e))

        # Step 4: CAPSOLVER_API_KEY (opcional)
        try:
            from shared.config import CAPSOLVER_API_KEY
            if CAPSOLVER_API_KEY and len(CAPSOLVER_API_KEY) > 5:
                self._add(4, "CAPSOLVER_API_KEY (opcional)", "PASS",
                          f"Key: ...{CAPSOLVER_API_KEY[-4:]}")
            else:
                self._add(4, "CAPSOLVER_API_KEY (opcional)", "SKIP",
                          "No configurada (fallback deshabilitado)")
        except Exception:
            self._add(4, "CAPSOLVER_API_KEY (opcional)", "SKIP",
                      "No disponible")

    # ===========================================================================
    # FASE 2: Proxy Connectivity
    # ===========================================================================
    async def phase2_proxy(self):
        print(f"\n{BOLD}=== Fase 2: Proxy Connectivity ==={RESET}")

        from shared.proxy_config import PROXY_CONFIG, regenerate_session, get_proxy_uri

        def _build_proxy_url():
            uri = get_proxy_uri()
            return f"http://{uri}"

        # Step 5: Request via proxy a lumtest + ipify
        ip1 = None
        geo = None
        try:
            proxy_url = _build_proxy_url()
            async with httpx.AsyncClient(proxy=proxy_url, timeout=20, verify=False) as client:
                # lumtest para geo, ipify para IP
                resp_geo = await client.get("https://lumtest.com/myip.json")
                geo_data = resp_geo.json()
                geo = geo_data.get("country")
                resp_ip = await client.get("https://api.ipify.org?format=json")
                ip1 = resp_ip.json().get("ip")
                self._add(5, "Request via proxy (lumtest+ipify)", "PASS",
                          f"IP: {ip1}, Country: {geo}")
                self.proxy_ip = ip1
        except Exception as e:
            self._add(5, "Request via proxy (lumtest)", "FAIL", str(e))
            # Si el proxy falla, skip el resto de la fase
            for step, name in [(6, "Sticky session"), (7, "Regenerar sesión"), (8, "IP española")]:
                self._add(step, name, "SKIP", "Proxy no disponible")
            return

        # Step 6: Sticky session (misma IP)
        try:
            proxy_url = _build_proxy_url()
            async with httpx.AsyncClient(proxy=proxy_url, timeout=20, verify=False) as client:
                resp = await client.get("https://api.ipify.org?format=json")
                ip2 = resp.json().get("ip")
                if ip1 == ip2:
                    self._add(6, "Sticky session (misma IP)", "PASS",
                              f"Ambas requests: {ip1}")
                else:
                    self._add(6, "Sticky session (misma IP)", "FAIL",
                              f"IP1: {ip1}, IP2: {ip2} — sesión no sticky")
        except Exception as e:
            self._add(6, "Sticky session (misma IP)", "FAIL", str(e))

        # Step 7: Regenerar sesión → IP diferente
        try:
            old_sid = PROXY_CONFIG['sticky_session_id']
            regenerate_session()
            new_sid = PROXY_CONFIG['sticky_session_id']
            proxy_url = _build_proxy_url()
            async with httpx.AsyncClient(proxy=proxy_url, timeout=20, verify=False) as client:
                resp = await client.get("https://api.ipify.org?format=json")
                ip3 = resp.json().get("ip")
                if ip3 != ip1:
                    self._add(7, "Regenerar sesión (IP nueva)", "PASS",
                              f"Antes: {ip1}, Después: {ip3}")
                else:
                    self._add(7, "Regenerar sesión (IP nueva)", "WARN",
                              f"IP no cambió ({ip1}) — puede ser coincidencia")
                # Actualizar IP de referencia
                self.proxy_ip = ip3
        except Exception as e:
            self._add(7, "Regenerar sesión (IP nueva)", "FAIL", str(e))

        # Step 8: IP española
        try:
            proxy_url = _build_proxy_url()
            async with httpx.AsyncClient(proxy=proxy_url, timeout=20, verify=False) as client:
                resp = await client.get("https://lumtest.com/myip.json")
                data = resp.json()
                country = data.get("country")
                if country == "ES":
                    self._add(8, "IP española (country=ES)", "PASS",
                              f"Country: {country}")
                else:
                    self._add(8, "IP española (country=ES)", "FAIL",
                              f"Country: {country} (esperado: ES)")
        except Exception as e:
            self._add(8, "IP española (country=ES)", "FAIL", str(e))

    # ===========================================================================
    # FASE 3: Acceso al sitio objetivo
    # ===========================================================================
    async def phase3_target(self):
        print(f"\n{BOLD}=== Fase 3: Acceso al sitio objetivo ==={RESET}")

        from shared.proxy_config import get_proxy_uri

        proxy_url = f"http://{get_proxy_uri()}"

        # Step 9: GET idealista.com
        try:
            async with httpx.AsyncClient(proxy=proxy_url, timeout=25,
                                         follow_redirects=True, verify=False) as client:
                resp = await client.get("https://www.idealista.com")
                status = resp.status_code
                if status in (200, 403):
                    self._add(9, "GET idealista.com via proxy", "PASS",
                              f"Status: {status}, Content-Length: {len(resp.content)}")
                else:
                    self._add(9, "GET idealista.com via proxy", "WARN",
                              f"Status inesperado: {status}")
        except Exception as e:
            self._add(9, "GET idealista.com via proxy", "FAIL", str(e))

        # Step 10: Detectar DataDome en headers/body
        try:
            async with httpx.AsyncClient(proxy=proxy_url, timeout=25,
                                         follow_redirects=True, verify=False) as client:
                resp = await client.get("https://www.idealista.com")
                headers_str = str(dict(resp.headers)).lower()
                body = resp.text.lower()
                dd_header = "datadome" in headers_str or "set-cookie" in headers_str and "datadome" in headers_str
                dd_body = "captcha-delivery.com" in body or "datadome" in body
                if dd_header or dd_body:
                    self._add(10, "Detección DataDome", "INFO",
                              f"DataDome activo — Header: {dd_header}, Body: {dd_body}")
                else:
                    self._add(10, "Detección DataDome", "INFO",
                              "DataDome no detectado en respuesta HTTP")
        except Exception as e:
            self._add(10, "Detección DataDome", "WARN", str(e))

    # ===========================================================================
    # FASE 4: Browser
    # ===========================================================================
    async def phase4_browser(self):
        if self.no_browser:
            print(f"\n{BOLD}=== Fase 4: Browser (SKIP --no-browser) ==={RESET}")
            for step, name in [(11, "Lanzar Chromium"), (12, "Navegar idealista"),
                               (13, "Detectar DataDome iframe"), (14, "User-Agent browser"),
                               (15, "IP desde browser")]:
                self._add(step, name, "SKIP", "--no-browser")
            return

        print(f"\n{BOLD}=== Fase 4: Browser ==={RESET}")

        from shared.proxy_config import PROXY_CONFIG

        login = PROXY_CONFIG['login']
        sid = PROXY_CONFIG.get('sticky_session_id')
        if sid:
            login = f"{login}-session-{sid}"

        pw_proxy = {
            "server": f"http://{PROXY_CONFIG['host']}:{PROXY_CONFIG['port']}",
            "username": login,
            "password": PROXY_CONFIG['password'],
        }

        # Step 11: Lanzar Chromium
        try:
            from playwright.async_api import async_playwright
            self._pw = await async_playwright().__aenter__()
            self.profile_dir = tempfile.mkdtemp(prefix="diag_captcha_")

            self.browser = await self._pw.chromium.launch_persistent_context(
                self.profile_dir,
                headless=self.headless,
                proxy=pw_proxy,
                viewport={"width": 1366, "height": 768},
                ignore_https_errors=True,
                args=["--disable-blink-features=AutomationControlled"],
            )
            self.page = self.browser.pages[0] if self.browser.pages else await self.browser.new_page()
            self.browser_ok = True
            self._add(11, "Lanzar Chromium con proxy", "PASS",
                      f"Headless: {self.headless}")
        except Exception as e:
            self._add(11, "Lanzar Chromium con proxy", "FAIL", str(e))
            for step, name in [(12, "Navegar idealista"), (13, "Detectar DataDome iframe"),
                               (14, "User-Agent browser"), (15, "IP desde browser")]:
                self._add(step, name, "SKIP", "Browser no disponible")
            return

        # Step 12: Navegar a idealista.com
        try:
            resp = await self.page.goto("https://www.idealista.com", timeout=30000,
                                        wait_until="domcontentloaded")
            status = resp.status if resp else "?"
            self._add(12, "Navegar a idealista.com", "PASS",
                      f"Status: {status}")
        except Exception as e:
            self._add(12, "Navegar a idealista.com", "FAIL", str(e))

        # Step 13: Detectar DataDome iframe + param t=
        try:
            dd_data = await self.page.evaluate("""() => {
                const iframe = document.querySelector('iframe[src*="captcha-delivery.com"]');
                return iframe ? { is_datadome: true, captcha_url: iframe.src } : { is_datadome: false };
            }""")
            if dd_data.get("is_datadome"):
                self.captcha_detected = True
                self.captcha_url = dd_data.get("captcha_url", "")
                # Parsear param t=
                t_param = ""
                if "t=" in self.captcha_url:
                    t_param = self.captcha_url.split("t=")[1].split("&")[0]
                if t_param == "bv":
                    self._add(13, "Detectar DataDome iframe (t=)", "WARN",
                              f"t=bv — IP permanentemente bloqueada\nURL: {self.captcha_url[:120]}...")
                else:
                    self._add(13, "Detectar DataDome iframe (t=)", "PASS",
                              f"Captcha detectado, t={t_param}\nURL: {self.captcha_url[:120]}...")
            else:
                self._add(13, "Detectar DataDome iframe (t=)", "PASS",
                          "Sin captcha DataDome — acceso limpio")
        except Exception as e:
            self._add(13, "Detectar DataDome iframe (t=)", "FAIL", str(e))

        # Step 14: User-Agent del browser
        try:
            ua = await self.page.evaluate("navigator.userAgent")
            self.browser_ua = ua
            self._add(14, "User-Agent del browser", "PASS", ua[:100])
        except Exception as e:
            self._add(14, "User-Agent del browser", "FAIL", str(e))

        # Step 15: IP desde browser (api.ipify.org)
        try:
            await self.page.goto("https://api.ipify.org?format=json", timeout=15000)
            content = await self.page.text_content("body")
            ip_data = json.loads(content)
            self.browser_ip = ip_data.get("ip", content.strip())
            self._add(15, "IP desde browser (ipify)", "PASS",
                      f"IP: {self.browser_ip}")
        except Exception as e:
            self._add(15, "IP desde browser (ipify)", "FAIL", str(e))

    # ===========================================================================
    # FASE 5: Test del solver 2Captcha
    # ===========================================================================
    async def phase5_solver(self):
        if self.no_browser:
            print(f"\n{BOLD}=== Fase 5: 2Captcha Solver (SKIP --no-browser) ==={RESET}")
            for step in range(16, 20):
                self._add(step, "2Captcha solver", "SKIP", "--no-browser")
            return

        if not self.captcha_detected:
            print(f"\n{BOLD}=== Fase 5: 2Captcha Solver (SKIP — sin captcha) ==={RESET}")
            for step, name in [(16, "createTask"), (17, "getTaskResult polling"),
                               (18, "Extraer cookie"), (19, "Inyectar cookie + reload")]:
                self._add(step, name, "SKIP", "No se detectó captcha DataDome")
            return

        print(f"\n{BOLD}=== Fase 5: 2Captcha Solver ==={RESET}")

        from shared.config import TWOCAPTCHA_API_KEY
        from shared.proxy_config import PROXY_CONFIG

        login = PROXY_CONFIG['login']
        sid = PROXY_CONFIG.get('sticky_session_id')
        if sid:
            login = f"{login}-session-{sid}"

        page_url = "https://www.idealista.com"
        ua = self.browser_ua or "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"

        # Step 16: createTask
        task_id = None
        try:
            payload = {
                "clientKey": TWOCAPTCHA_API_KEY,
                "task": {
                    "type": "DataDomeSliderTask",
                    "websiteURL": page_url,
                    "captchaUrl": self.captcha_url,
                    "userAgent": ua,
                    "proxyType": PROXY_CONFIG.get('type', 'http').lower(),
                    "proxyAddress": PROXY_CONFIG['host'],
                    "proxyPort": PROXY_CONFIG['port'],
                    "proxyLogin": login,
                    "proxyPassword": PROXY_CONFIG['password'],
                }
            }
            async with httpx.AsyncClient(timeout=20) as client:
                resp = await client.post("https://api.2captcha.com/createTask", json=payload)
                data = resp.json()
                task_id = data.get("taskId")
                if task_id:
                    self._add(16, "createTask DataDomeSliderTask", "PASS",
                              f"taskId: {task_id}")
                else:
                    err = data.get("errorDescription") or data.get("errorCode") or str(data)
                    self._add(16, "createTask DataDomeSliderTask", "FAIL",
                              f"Error: {err}")
        except Exception as e:
            self._add(16, "createTask DataDomeSliderTask", "FAIL", str(e))

        if not task_id:
            for step, name in [(17, "getTaskResult polling"), (18, "Extraer cookie"),
                               (19, "Inyectar cookie + reload")]:
                self._add(step, name, "SKIP", "No taskId")
            return

        # Step 17: Polling getTaskResult
        solution = None
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                for attempt in range(13):  # 13 * 5s = 65s max
                    await asyncio.sleep(5)
                    resp = await client.post(
                        "https://api.2captcha.com/getTaskResult",
                        json={"clientKey": TWOCAPTCHA_API_KEY, "taskId": task_id}
                    )
                    data = resp.json()
                    status = data.get("status")
                    if status == "ready":
                        solution = data.get("solution", {})
                        self._add(17, "getTaskResult polling", "PASS",
                                  f"Resuelto en ~{(attempt+1)*5}s")
                        break
                    elif data.get("errorId") and data.get("errorId") != 0:
                        err = data.get("errorDescription") or data.get("errorCode")
                        self._add(17, "getTaskResult polling", "FAIL",
                                  f"Error: {err}")
                        break
                else:
                    self._add(17, "getTaskResult polling", "FAIL",
                              "Timeout (65s) — captcha no resuelto")
        except Exception as e:
            self._add(17, "getTaskResult polling", "FAIL", str(e))

        if not solution:
            for step, name in [(18, "Extraer cookie"), (19, "Inyectar cookie + reload")]:
                self._add(step, name, "SKIP", "Sin solución")
            return

        # Step 18: Extraer cookie
        cookie_value = None
        try:
            cookie_value = (solution.get("cookie")
                            or solution.get("datadome")
                            or solution.get("token"))
            if cookie_value:
                self._add(18, "Extraer cookie de solución", "PASS",
                          f"Cookie: {str(cookie_value)[:80]}...")
            else:
                self._add(18, "Extraer cookie de solución", "FAIL",
                          f"Claves disponibles: {list(solution.keys())}")
        except Exception as e:
            self._add(18, "Extraer cookie de solución", "FAIL", str(e))

        if not cookie_value or not self.browser_ok:
            self._add(19, "Inyectar cookie + reload", "SKIP",
                      "Sin cookie o browser")
            return

        # Step 19: Inyectar cookie + reload
        try:
            # Parsear cookie string (formato: "datadome=XXXX")
            if isinstance(cookie_value, str) and "=" in cookie_value:
                cookie_name, cookie_val = cookie_value.split("=", 1)
            else:
                cookie_name, cookie_val = "datadome", str(cookie_value)

            await self.page.goto("https://www.idealista.com", timeout=10000,
                                 wait_until="domcontentloaded")
            await self.context.add_cookies([{
                "name": cookie_name.strip(),
                "value": cookie_val.strip(),
                "domain": ".idealista.com",
                "path": "/",
            }]) if self.context else await self.browser.add_cookies([{
                "name": cookie_name.strip(),
                "value": cookie_val.strip(),
                "domain": ".idealista.com",
                "path": "/",
            }])
            await self.page.reload(timeout=15000, wait_until="domcontentloaded")

            # Verificar si captcha desapareció
            dd_check = await self.page.evaluate("""() => {
                const iframe = document.querySelector('iframe[src*="captcha-delivery.com"]');
                return iframe ? true : false;
            }""")
            if not dd_check:
                self._add(19, "Inyectar cookie + reload", "PASS",
                          "Captcha desapareció — cookie aceptada")
            else:
                self._add(19, "Inyectar cookie + reload", "FAIL",
                          "Captcha persiste tras inyectar cookie")
        except Exception as e:
            self._add(19, "Inyectar cookie + reload", "FAIL", str(e))

    # ===========================================================================
    # FASE 6: Consistencia de IP
    # ===========================================================================
    async def phase6_ip_consistency(self):
        if self.no_browser:
            print(f"\n{BOLD}=== Fase 6: IP Consistency (SKIP --no-browser) ==={RESET}")
            for step in range(20, 23):
                self._add(step, "IP consistency", "SKIP", "--no-browser")
            return

        print(f"\n{BOLD}=== Fase 6: Consistencia de IP ==={RESET}")

        # Step 20: IP del browser
        browser_ip = self.browser_ip
        if browser_ip:
            self._add(20, "IP del browser (cache)", "PASS", f"IP: {browser_ip}")
        elif self.browser_ok:
            try:
                await self.page.goto("https://api.ipify.org?format=json", timeout=15000)
                content = await self.page.text_content("body")
                ip_data = json.loads(content)
                browser_ip = ip_data.get("ip", content.strip())
                self.browser_ip = browser_ip
                self._add(20, "IP del browser (ipify)", "PASS", f"IP: {browser_ip}")
            except Exception as e:
                self._add(20, "IP del browser (ipify)", "FAIL", str(e))
        else:
            self._add(20, "IP del browser", "SKIP", "Browser no disponible")

        # Step 21: IP via httpx con mismo proxy+session
        python_ip = None
        try:
            from shared.proxy_config import get_proxy_uri
            proxy_url = f"http://{get_proxy_uri()}"
            async with httpx.AsyncClient(proxy=proxy_url, timeout=15, verify=False) as client:
                resp = await client.get("https://api.ipify.org?format=json")
                data = resp.json()
                python_ip = data.get("ip", resp.text.strip())
                self.proxy_ip = python_ip
                self._add(21, "IP Python via proxy (ipify)", "PASS",
                          f"IP: {python_ip}")
        except Exception as e:
            self._add(21, "IP Python via proxy (ipify)", "FAIL", str(e))

        # Step 22: Comparar IPs
        if browser_ip and python_ip:
            if browser_ip == python_ip:
                self._add(22, "Browser IP == Proxy IP", "PASS",
                          f"Ambas: {browser_ip} OK DataDome aceptará cookies")
            else:
                self._add(22, "Browser IP == Proxy IP", "FAIL",
                          f"Browser: {browser_ip} != Proxy: {python_ip}\n"
                          "!! MISMATCH: 2Captcha resuelve con IP-A pero browser "
                          "presenta cookie desde IP-B → DataDome la rechazará")
        else:
            missing = []
            if not browser_ip:
                missing.append("browser")
            if not python_ip:
                missing.append("proxy")
            self._add(22, "Browser IP == Proxy IP", "SKIP",
                      f"IP no disponible: {', '.join(missing)}")

    # ===========================================================================
    # Cleanup & Summary
    # ===========================================================================
    async def cleanup(self):
        """Cerrar browser y limpiar perfil temporal."""
        try:
            if self.browser:
                await self.browser.close()
        except Exception:
            pass
        try:
            if hasattr(self, '_pw') and self._pw:
                await self._pw.__aexit__(None, None, None)
        except Exception:
            pass
        try:
            if self.profile_dir and os.path.exists(self.profile_dir):
                shutil.rmtree(self.profile_dir, ignore_errors=True)
        except Exception:
            pass

    def print_summary(self):
        """Imprimir resumen final."""
        counts = {"PASS": 0, "FAIL": 0, "WARN": 0, "SKIP": 0, "INFO": 0}
        for r in self.results:
            counts[r.status] = counts.get(r.status, 0) + 1

        # Agrupar por fase
        phases = [
            ("Phase 1: Configuration", range(1, 5)),
            ("Phase 2: Proxy", range(5, 9)),
            ("Phase 3: Target Site", range(9, 11)),
            ("Phase 4: Browser", range(11, 16)),
            ("Phase 5: 2Captcha Solver", range(16, 20)),
            ("Phase 6: IP Consistency", range(20, 23)),
        ]

        print(f"\n{'='*60}")
        print(f"{BOLD}CAPTCHA PIPELINE DIAGNOSTIC SUMMARY{RESET}")
        print(f"{'='*60}")

        for phase_name, step_range in phases:
            phase_results = [r for r in self.results if r.step in step_range]
            pass_count = sum(1 for r in phase_results if r.status == "PASS")
            total = len(phase_results)
            fail_count = sum(1 for r in phase_results if r.status == "FAIL")

            if fail_count > 0:
                color = RED
            elif pass_count == total:
                color = GREEN
            else:
                color = YELLOW

            dots = "." * (40 - len(phase_name))
            print(f"  {phase_name} {dots} {color}{pass_count}/{total} PASS{RESET}")

        print(f"{'='*60}")
        parts = []
        for status in ["PASS", "WARN", "FAIL", "SKIP"]:
            c = counts.get(status, 0)
            if c > 0:
                color = STATUS_COLORS.get(status, RESET)
                parts.append(f"{color}{c} {status}{RESET}")
        print(f"  Result: {', '.join(parts)}")
        print(f"{'='*60}")

        return counts.get("FAIL", 0)

    async def run(self):
        """Ejecutar todas las fases."""
        print(f"\n{BOLD}{'='*60}")
        print(f"  CAPTCHA PIPELINE DIAGNOSTIC")
        print(f"  Mode: {'no-browser' if self.no_browser else 'full'}")
        print(f"{'='*60}{RESET}")

        await self.phase1_config()
        await self.phase2_proxy()
        await self.phase3_target()
        await self.phase4_browser()
        await self.phase5_solver()
        await self.phase6_ip_consistency()
        await self.cleanup()

        fail_count = self.print_summary()
        return fail_count


async def main():
    parser = argparse.ArgumentParser(description="Diagnóstico del pipeline DataDome captcha")
    parser.add_argument("--no-browser", action="store_true",
                        help="Skip fases que requieren browser (4-6)")
    parser.add_argument("--verbose", action="store_true",
                        help="Mostrar detalles de todos los pasos")
    parser.add_argument("--headless", action="store_true", default=False,
                        help="Ejecutar browser en modo headless")
    args = parser.parse_args()

    diag = CaptchaDiagnostic(
        no_browser=args.no_browser,
        verbose=args.verbose,
        headless=args.headless,
    )
    try:
        fail_count = await diag.run()
    except KeyboardInterrupt:
        print(f"\n{YELLOW}Diagnóstico interrumpido por usuario{RESET}")
        await diag.cleanup()
        sys.exit(130)
    except Exception as e:
        print(f"\n{RED}Error fatal: {e}{RESET}")
        await diag.cleanup()
        sys.exit(2)

    sys.exit(1 if fail_count > 0 else 0)


if __name__ == "__main__":
    asyncio.run(main())
