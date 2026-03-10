# Mejoras pendientes en el sistema de captchas

> Generado: 2026-03-09 | Estado: P0 y P1 completadas, resto pendiente

---

## Cambios P0 ya implementados

- **GeeTest solver**: Corregido bug `NameError` en `except` (variable `logger` no definida), guard cambiado de `SOLVER` a `ASYNC_SOLVER`, añadido stats tracking, logger forwarded desde `solve_captcha_advanced`.
- **Auto-solve en detalle de propiedad**: Ambos bloques (primera propiedad y siguientes) en `scraper_wrapper.py` ahora llaman a `solve_captcha_advanced()` antes de la espera pasiva de 30s.

---

## P1 — Prioridad alta ✅ COMPLETADAS

### P1-1: Auto-resolver captchas en `enrich_worker.py` ✅

- **Archivo**: `scripts/enrich_worker.py`, líneas ~264-287
- **Problema**: `check_for_blocks()` tiene la detección de captchas más completa del codebase (WAF IDs, `window.dd`, keywords extendidos), pero cuando detecta un captcha solo espera 30s por resolución manual. Nunca llama a `solve_captcha_advanced()`.
- **Impacto**: Cada captcha durante el enriquecimiento = propiedad perdida + rotación de identidad completa.
- **Cómo implementar**:
  1. Importar `solve_captcha_advanced` desde `idealista_scraper.utils` en `enrich_worker.py`.
  2. Antes del bucle de espera manual (línea ~272), añadir una llamada a `solve_captcha_advanced(page, logger=..., use_proxy=True)` envuelta en `asyncio.wait_for(timeout=180.0)`.
  3. Si auto-solve resuelve, re-extraer datos y continuar.
  4. Mantener la espera manual de 30s como fallback (igual que el patrón ya implementado en P0 para `scraper_wrapper.py`).
- **Resultado esperado**: Los captchas durante enriquecimiento se resuelven automáticamente en vez de depender de intervención manual.

### P1-2: Detección de `t=bv` en `update_urls.py`, `trends_tracker.py` y `enrich_worker.py` ✅

- **Archivos**: `scraper/update_urls.py`, `trends/trends_tracker.py`, `scripts/enrich_worker.py`
- **Problema**: El parámetro `t=bv` en la URL del captcha DataDome indica que la IP está permanentemente bloqueada. Solo se comprueba en `scraper_wrapper.py` / `utils.py`. Los otros tres archivos gastan intentos de solver pagados (2Captcha/CapSolver) en IPs irrecuperables.
- **Impacto**: Coste económico innecesario en créditos de solver + tiempo perdido (~65s por intento fallido).
- **Cómo implementar**:
  1. Extraer la lógica de detección `t=bv` a una función reutilizable en `utils.py` (o usar la que ya existe implícitamente en `solve_datadome_2captcha`).
  2. En cada archivo, antes de llamar a `solve_captcha_advanced()`, extraer el `captcha_url` del iframe DataDome y comprobar si contiene `t=bv`.
  3. Si `t=bv`, loggear y saltar directamente a rotación de identidad sin intentar solver.
  4. Alternativa más simple: confiar en que `solve_captcha_advanced` ya maneja `t=bv` internamente (lo hace via `solve_datadome_2captcha`). Solo asegurarse de que estos archivos **llaman** a `solve_captcha_advanced` (ver P1-1 y P1-3).
- **Resultado esperado**: Cero créditos gastados en IPs permanentemente bloqueadas fuera del scraper principal.

### P1-3: Timeout en bucle manual de `update_urls.py` ✅

- **Archivo**: `scraper/update_urls.py`, líneas ~951-954
- **Problema**: Si `solve_captcha_advanced` falla, el código entra en un `while await detect_captcha(page) == "captcha"` infinito sin timeout, esperando resolución manual con `play_captcha_alert()` cada 5s. Si nadie resuelve manualmente, el proceso se cuelga para siempre.
- **Impacto**: Proceso `update_urls.py` puede quedar bloqueado indefinidamente.
- **Cómo implementar**:
  1. Añadir un timeout al bucle manual: contar el tiempo transcurrido y romper el bucle tras 60-90 segundos.
  2. Tras el timeout, marcar el perfil como bloqueado y rotar identidad (o continuar con la siguiente URL).
  3. Patrón de referencia: ver cómo `_goto_with_retry` en `scraper_wrapper.py` (líneas ~1891-1918) implementa el timeout de 60s en su espera manual.
- **Resultado esperado**: `update_urls.py` nunca se cuelga indefinidamente por un captcha no resuelto.

---

## P2 — Prioridad media ✅ COMPLETADAS

### P2-1: Race condition en `_last_solver_fail_reason` (variable global mutable) ✅

- **Archivo**: `scraper/idealista_scraper/utils.py`, línea ~94
- **Problema**: `_last_solver_fail_reason` es una variable global que se escribe dentro de `solve_datadome_2captcha` y `solve_datadome_capsolver`, y se lee en `solve_captcha_advanced`. Múltiples workers paralelos comparten este estado. Un worker puede leer el `'tbv'` escrito por otro worker y hacer fast-fail incorrectamente (o viceversa, no hacer fast-fail cuando debería).
- **Impacto**: En ejecución paralela, el fast-fail por `t=bv` puede activarse/desactivarse de forma impredecible.
- **Cómo implementar**:
  1. Eliminar la variable global `_last_solver_fail_reason`.
  2. Hacer que `solve_datadome_2captcha` y `solve_datadome_capsolver` devuelvan un valor más descriptivo en vez de `False`, por ejemplo una tupla `(False, 'tbv')` o un string `'tbv'` cuando fallan por IP bloqueada.
  3. En `solve_captcha_advanced`, capturar el valor de retorno y decidir el fast-fail basándose en el retorno local, no en estado global.
  4. Alternativa más simple: pasar un diccionario mutable como parámetro `context={}` que se usa solo dentro de una llamada a `solve_captcha_advanced`, evitando compartir estado entre workers.
- **Resultado esperado**: Workers paralelos no interfieren entre sí en la lógica de fast-fail.

### P2-2: Race condition en contador `t=bv` basado en fichero ✅

- **Archivo**: `scraper/idealista_scraper/utils.py`, líneas ~97-136 (`tbv_state.json`)
- **Problema**: `_increment_tbv_counter()` y `_get_tbv_count()` leen/escriben `scraper/app/tbv_state.json` sin file locking. Workers concurrentes pueden corromper el JSON o perder incrementos.
- **Impacto**: El circuit breaker puede no activarse cuando debería (conteos perdidos) o activarse prematuramente (conteos duplicados).
- **Cómo implementar**:
  1. Usar `filelock` (o `fcntl`/`msvcrt` para locking nativo) alrededor de las operaciones de lectura/escritura.
  2. Alternativa: usar un valor in-memory protegido por `asyncio.Lock` si todos los workers corren en el mismo proceso. Si corren en procesos separados, usar `multiprocessing.Value` o un fichero con locking.
  3. Alternativa más simple: aceptar la race condition como tolerable (el circuit breaker es una heurística, no necesita ser exacto).
- **Resultado esperado**: Contador `t=bv` fiable en entornos con múltiples workers.

### P2-3: Circuit breaker de 30 minutos no interrumpible ✅

- **Archivo**: `scraper/idealista_scraper/utils.py`, línea ~1587
- **Problema**: `await asyncio.sleep(TBV_CIRCUIT_BREAKER_PAUSE_MIN * 60)` bloquea la tarea 30 minutos. Si el usuario pulsa Stop durante este período, el scraper no responde hasta que termine el sleep. La función `solve_captcha_advanced` no tiene acceso al controlador del scraper para verificar `_should_stop`.
- **Impacto**: El usuario puede tener que matar el proceso manualmente si el circuit breaker se activa.
- **Cómo implementar**:
  1. Reemplazar el `asyncio.sleep(1800)` con un bucle de sleeps cortos (ej. 30 iteraciones de 60s) que permitan comprobar si hay una señal de cancelación.
  2. Pasar un parámetro `cancel_event: asyncio.Event` a `solve_captcha_advanced` que el wrapper puede activar al recibir Stop.
  3. Alternativa: usar `asyncio.wait_for()` con un timeout más corto y re-evaluar el circuit breaker en cada iteración.
  4. Logear progreso cada 5 minutos durante la pausa para que el usuario sepa que el sistema no está colgado.
- **Resultado esperado**: El circuit breaker es cancelable por el usuario en cualquier momento.

### P2-4: `solve_captcha_advanced` sin timeout al llamarse desde handler de 0 propiedades ✅

- **Archivo**: `scraper/app/scraper_wrapper.py`, línea ~2770
- **Problema**: Cuando el conteo de propiedades es 0 (posible captcha en la página de listado), se llama a `solve_captcha_advanced(page, logger=self.log)` sin envolver en `asyncio.wait_for()`. A diferencia de `_goto_with_retry` (que usa timeout de 180s), aquí no hay límite. Si el circuit breaker se activa (30 min) o los solvers se cuelgan, bloqueo indefinido.
- **Impacto**: Bloqueo potencial del scraper sin timeout.
- **Cómo implementar**:
  1. Envolver la llamada en `asyncio.wait_for(solve_captcha_advanced(...), timeout=180.0)`.
  2. Capturar `asyncio.TimeoutError` y loggear.
  3. Patrón de referencia: la misma llamada en `_goto_with_retry` línea ~1879.
- **Resultado esperado**: Timeout garantizado de 180s en todos los puntos de llamada a `solve_captcha_advanced`.

---

## P3 — Prioridad baja ✅ COMPLETADAS

### P3-1: Unificar funciones de detección de captcha duplicadas ✅

- **Archivos afectados**:
  - `scraper/update_urls.py` → `detect_captcha()` (líneas ~356-385)
  - `trends/trends_tracker.py` → `detect_block()` (líneas ~220-243)
  - `scripts/enrich_worker.py` → `check_for_blocks()` (líneas ~113-161)
  - `scraper/app/scraper_wrapper.py` → `_check_for_blocks()` (líneas ~1657-1702)
- **Problema**: Cuatro funciones duplicadas con cobertura inconsistente. `update_urls.py` y `trends_tracker.py` no detectan `window.dd`, WAF IDs, ni páginas vacías "idealista.com". Captchas que se detectan en el scraper principal pasan desapercibidos en los otros módulos.
- **Impacto**: Detección inconsistente según qué módulo ejecuta el scraping.
- **Cómo implementar**:
  1. Crear una función canónica `detect_captcha_or_block(page)` en `idealista_scraper/utils.py` que incluya todas las comprobaciones de la versión más completa (`enrich_worker.py`).
  2. Retornar un enum/string: `'block'`, `'captcha'`, `'ssl_error'`, `None`.
  3. Reemplazar las cuatro funciones locales con imports de la función canónica.
  4. Añadir tests manuales: navegar a una URL de Idealista con cada módulo y verificar que la detección funciona.
- **Resultado esperado**: Una sola función de detección mantenida en un solo lugar, cobertura completa en todos los módulos.

### P3-2: Stats tracking faltante en `solve_slider_2captcha` ✅

- **Archivo**: `scraper/idealista_scraper/utils.py`, función `solve_slider_2captcha` (líneas ~1317-1538)
- **Problema**: La función nunca llama a `_captcha_inc()` para sus propios intentos/éxitos. Sus estadísticas no aparecen en el dashboard de captchas.
- **Impacto**: No se puede medir la eficacia del solver de coordenadas.
- **Cómo implementar**:
  1. Añadir `_captcha_inc("2Captcha Coordenadas|intentos")` al inicio de la función.
  2. Añadir `_captcha_inc("2Captcha Coordenadas|resueltos")` cuando retorna `True`.
  3. Añadir `_captcha_inc("2Captcha Coordenadas|errores")` en el `except`.
- **Resultado esperado**: Visibilidad completa de todos los métodos de resolución en las estadísticas.

### P3-3: Detección de tipo de captcha en propiedades sin logging detallado ✅

- **Archivo**: `scraper/app/scraper_wrapper.py`, líneas ~3535-3560
- **Problema**: Cuando se detecta un captcha en una página de detalle de propiedad, el log solo dice "CAPTCHA detectado en propiedad" sin indicar qué tipo es (DataDome iframe, slider, GeeTest, página vacía).
- **Impacto**: Dificulta el diagnóstico post-mortem de por qué fallan los captchas en propiedades.
- **Cómo implementar**:
  1. Antes de llamar a `solve_captcha_advanced`, hacer una detección rápida del tipo: comprobar `iframe[src*="captcha-delivery.com"]`, `window.initGeetest`, y slider containers.
  2. Loggear el tipo detectado: "CAPTCHA detectado en propiedad (tipo: DataDome)" / "(tipo: GeeTest)" / "(tipo: slider)" / "(tipo: desconocido)".
- **Resultado esperado**: Logs que permiten identificar el tipo de captcha más frecuente en detalle de propiedades.

### P3-4: `solve_slider_captcha` siempre retorna `True` sin verificación ✅

- **Archivo**: `scraper/idealista_scraper/utils.py`, línea ~786
- **Problema**: Tras completar el drag del slider, la función retorna `True` sin verificar que el captcha desapareció. El caller (`solve_captcha_advanced`) sí verifica después via título de página, pero el dato de éxito del slider es falso — infla las estadísticas de "resuelto" cuando en realidad solo hizo el drag.
- **Impacto**: Estadísticas infladas del slider local. El caller compensa, así que no afecta funcionalidad real.
- **Cómo implementar**:
  1. Tras el `await asyncio.sleep(2)`, verificar si el captcha sigue presente (ej. comprobar si el handle del slider sigue visible).
  2. Retornar `True` solo si la verificación confirma que desapareció.
  3. Alternativa: aceptar que la verificación real la hace el caller y documentar que el retorno de `solve_slider_captcha` es "drag completado" no "captcha resuelto".
- **Resultado esperado**: Estadísticas precisas del slider local.

### P3-5: `trends_tracker.py` re-navega innecesariamente tras resolver captcha ✅

- **Archivo**: `trends/trends_tracker.py`, líneas ~591-594
- **Problema**: Tras resolver exitosamente un captcha con `solve_captcha_advanced`, hace `continue` que reinicia el bucle de intentos, re-navegando desde cero a la URL. Esto desperdicia una navegación y arriesga un nuevo captcha.
- **Impacto**: Latencia innecesaria (~5-10s) y riesgo de captcha adicional.
- **Cómo implementar**:
  1. Tras `solve_captcha_advanced` exitoso, extraer datos directamente de la página actual en vez de hacer `continue`.
  2. Solo hacer `continue` si la extracción falla (por si la página necesita recarga).
- **Resultado esperado**: Una navegación menos por captcha resuelto en trends_tracker.

---

## Mejoras en implementación de solvers 2Captcha / CapSolver ✅ COMPLETADAS

### S-1: Logging de error codes específicos de 2Captcha y CapSolver ✅

- **Severidad**: Media
- **Archivos**: `utils.py`, funciones `solve_datadome_2captcha` (~línea 1014) y `solve_datadome_capsolver` (~línea 1231)
- **Problema**: Cuando el solver falla, solo se loggea el raw `res_data` o el `errorCode` genérico. No se distinguen errores accionables como:
  - `ERROR_PROXY_NOT_AUTHORISED` → credenciales proxy incorrectas
  - `ERROR_BAD_PROXY` → proxy no responde
  - `ERROR_PROXY_TIMEOUT` → proxy lento
  - `ERROR_CAPTCHA_UNSOLVABLE` → captcha irresoluble (ya detectado en 2Captcha, no en CapSolver)
  - `ERROR_INVALID_TASK_DATA` → UA inválido o parámetros incorrectos
- **Impacto**: Sin diagnóstico de la causa raíz de fallos. No se puede distinguir entre "proxy malo" y "captcha imposible".
- **Cómo implementar**:
  1. En `solve_datadome_2captcha`, línea ~1014: parsear `errorCode` y logear un mensaje descriptivo para cada código conocido. Añadir `_captcha_inc` con el código específico.
  2. En `solve_datadome_capsolver`, línea ~1231: parsear `errorCode` de CapSolver (formato `ERROR_XXX`) y logear mensajes descriptivos. Detectar `ERROR_CAPTCHA_UNSOLVABLE` explícitamente.
  3. Crear un diccionario `KNOWN_ERROR_CODES` con mensajes en español para cada código.
- **Resultado esperado**: Logs que indican exactamente por qué falló cada intento de solver.

### S-2: Pre-flight IP check en CapSolver ✅

- **Severidad**: Media
- **Archivo**: `utils.py`, función `solve_datadome_capsolver` (~línea 1170)
- **Problema**: A diferencia de `solve_datadome_2captcha` (que verifica la IP de salida del proxy antes de enviar al solver), CapSolver no hace esta comprobación. Si la cookie se rechaza, no se puede saber si fue por IP mismatch.
- **Impacto**: Diagnóstico incompleto cuando CapSolver falla con `cookie_rechazada`.
- **Cómo implementar**:
  1. Copiar el bloque de pre-flight IP check de `solve_datadome_2captcha` (líneas ~936-944) a `solve_datadome_capsolver`, justo antes del check `t=bv` (línea ~1158).
  2. Loggear la IP con el mismo formato: `"🌐 Proxy exit IP (session {sticky_sid}): {proxy_exit_ip}"`.
- **Resultado esperado**: Visibilidad completa de la IP del proxy en ambos solvers para diagnóstico de IP mismatch.

### S-3: Chrome version cap dinámica en CapSolver ✅

- **Severidad**: Media
- **Archivo**: `utils.py`, líneas ~1134-1137
- **Problema**: El cap de Chrome version está hardcodeado a 144. A medida que Chrome saque versiones nuevas y CapSolver actualice su soporte, este cap impedirá usar UAs legítimos con versiones superiores.
- **Impacto**: CapSolver dejará de funcionar con navegadores actualizados sin intervención manual.
- **Cómo implementar**:
  1. Mover el valor `144` a una constante en `config.py`: `CAPSOLVER_MAX_CHROME_VERSION = 144`.
  2. Referenciar la constante en `solve_datadome_capsolver`.
  3. Documentar en `config.py` que este valor debe actualizarse cuando CapSolver anuncie soporte para versiones superiores.
  4. Alternativa avanzada: consultar la API de CapSolver (`/getBalance` o similar) para determinar la versión máxima soportada dinámicamente (si la API lo expone).
- **Resultado esperado**: Actualización del cap de Chrome en un solo lugar, sin buscar en el código.

### S-4: Verificación post-inyección no extrae `t=` del nuevo captcha ✅

- **Severidad**: Baja
- **Archivos**: `utils.py`, funciones `solve_datadome_2captcha` (~líneas 1088-1103) y `solve_datadome_capsolver` (~líneas 1294-1308)
- **Problema**: Tras inyectar la cookie y recargar, la verificación two-pass solo comprueba si el iframe DataDome sigue presente. Si sigue presente, no extrae el nuevo `t=` parameter para saber si la cookie fue rechazada por IP (`t=bv`) o por token expirado (`t=fe` con challenge nuevo).
- **Impacto**: No se puede distinguir "cookie rechazada por IP" de "cookie expirada" en los logs.
- **Cómo implementar**:
  1. En el bloque de verificación two-pass, cuando `is_still_blocked` es `True`, extraer el `src` del iframe y parsear el `t=` parameter.
  2. Loggear: `"Cookie rechazada (nuevo t={t_param})"`.
  3. Si `t=bv`, incrementar el contador tbv.
- **Resultado esperado**: Diagnóstico preciso del motivo de rechazo de cookie tras inyección.

### S-5: Primer poll innecesariamente temprano ✅

- **Severidad**: Baja
- **Archivos**: `utils.py`, funciones `solve_datadome_2captcha` (~línea 1000) y `solve_datadome_capsolver` (~línea 1219)
- **Problema**: El primer poll a `getTaskResult` se hace a los 5s del `createTask`. Los DataDome típicamente tardan 10-20s en resolverse, por lo que los primeros 2-3 polls siempre devuelven `processing`.
- **Impacto**: ~10s de overhead de polling innecesario (no afecta a la resolución, solo a la eficiencia del loop).
- **Cómo implementar**:
  1. Hacer el primer `asyncio.sleep()` de 10s en vez de 5s.
  2. Mantener los siguientes polls cada 5s.
  3. O usar un backoff inicial: 10s, 5s, 5s, 5s...
- **Resultado esperado**: Menos peticiones innecesarias a las APIs de captcha.
