# Despliegue del Market Trends Tracker en Synology NAS

## Estado del despliegue

**✅ DESPLIEGUE COMPLETADO** — El tracker corre en el NAS 24/7 y el Programador de Tareas
de DSM lo lanza automáticamente cada día.

### Trabajo de código realizado (commit `f389c523` en `claudecode`)

| Archivo | Cambio | Estado |
|---------|--------|--------|
| `trends/app.py` | `CREATE_NO_WINDOW` condicional a `win32` (×2) | ✅ Hecho |
| `trends/app.py` | `taskkill` → `SIGKILL` en Linux (×1) | ✅ Hecho |
| `trends/app.py` | Host binding `0.0.0.0` en Linux, `127.0.0.1` en Windows | ✅ Hecho |
| `Dockerfile` | Python 3.11-slim + Playwright + Chromium | ✅ Hecho |
| `docker-compose.yml` | Servicio con volumen, shm_size=256mb, restart=unless-stopped | ✅ Hecho |

### Pasos de despliegue completados en el NAS

| Paso | Descripción | Estado |
|------|-------------|--------|
| 1 | Instalar Container Manager en DSM | ✅ Hecho |
| 2 | Habilitar SSH | ✅ Hecho |
| 3 | Conectar por SSH | ✅ Hecho |
| 4 | Clonar repo en `/volume1/docker/RealEstateMaster` | ✅ Hecho |
| 5 | `docker build -t realestate-trends:latest .` | ✅ Hecho |
| 6 | `docker-compose up -d` | ✅ Hecho |
| 7 | Verificar que el servicio responde en puerto 5005 | ✅ Hecho |
| 8 | Programar tarea diaria en DSM Task Scheduler | ✅ Hecho |

### Detalles de la instalación en el NAS

- **Ruta del repo**: `/volume1/docker/RealEstateMaster`
- **Usuario SSH**: `alvaro` (con alias `docker='sudo docker'` en `~/.bashrc`)
- **IP del NAS**: `192.168.1.2`
- **Puerto del servicio**: `5005`
- **Script de la tarea DSM**: `curl -s -X POST http://localhost:5005/api/start_tracker`
  (llamada desde el host del NAS, no desde dentro del contenedor — ver nota abajo)
- **Usuario de la tarea DSM**: `root`

El NAS ejecuta **únicamente** el servicio `trends` (puerto 5005). El resto de servicios
(analyzer, scraper, merger, dashboard) siguen corriendo en el PC Windows.

---

## Lecciones aprendidas durante el despliegue

### Permisos Docker en Synology
- El socket `/var/run/docker.sock` pertenece a `root`, no al grupo `docker`, aunque el
  usuario esté en ese grupo.
- Solución definitiva: alias en `~/.bashrc`:
  ```bash
  echo "alias docker='sudo docker'" >> ~/.bashrc
  echo "alias docker-compose='sudo docker-compose'" >> ~/.bashrc
  source ~/.bashrc
  ```
- Para tareas DSM usar siempre usuario `root`.

### curl no disponible dentro del contenedor
- `python:3.11-slim` no incluye `curl`.
- El script de la tarea DSM **no debe usar `docker exec ... curl`** sino llamar directamente
  al puerto expuesto desde el host:
  ```bash
  curl -s -X POST http://localhost:5005/api/start_tracker   # ✅ correcto
  docker exec realestate-trends curl ...                    # ❌ falla: curl not found
  ```

### Permisos en /volume1
- No se puede clonar directamente en `/volume1` (Permission denied).
- Clonar en una carpeta compartida existente, p.ej. `/volume1/docker/`.

### Timeout de red al hacer docker build
- `python:3.11-slim` se descarga de Docker Hub (~300 MB). Si hay un timeout, simplemente
  reintentar el build — suele ser un corte puntual.

### Comprobar grupos en Synology
- `groups` no existe en el shell ash de Synology. Usar `id` en su lugar.

---

## Contexto de arquitectura relevante

### Qué hace el trends tracker
- `trends/app.py`: servidor Flask-SocketIO en puerto 5005
- `trends/trends_tracker.py`: proceso hijo que lanza Playwright/Chromium headless,
  navega por Idealista y guarda recuentos de propiedades por provincia/zona/subzona en
  `trends/data/market_trends.db` (SQLite)
- El tracker lee el mapeo de URLs desde `scraper/documentation/province_urls_mapping.md`
  y las subzonas desde `scraper/documentation/subzones_complete.json`
- Usa el proxy Bright Data (`shared/proxy_config.py`) y los solucionadores de captcha
  (`shared/config.py`: claves 2Captcha y CapSolver ya hardcodeadas como fallback)
- Persiste progreso en `trends/data/checkpoint.json` para poder reanudar (`--resume`)
- Los perfiles stealth de Chromium se crean en `trends/stealth_profile_*/` al primer uso

### Dependencias que importa trends_tracker.py
```
scraper/browser_utils.py              ← get_browser_executable_path, generate_stealth_script
scraper/idealista_scraper/config.py   ← VIEWPORT_SIZES, USER_AGENTS, BROWSER_ROTATION_POOL
scraper/idealista_scraper/utils.py    ← solve_captcha_advanced, detect_captcha_or_block
scraper/identity_manager.py           ← rotate_identity, mark_current_profile_blocked
shared/proxy_config.py                ← PROXY_CONFIG (Bright Data)
shared/config.py                      ← TRENDS_PORT, claves API
```
Por eso el `docker-compose.yml` monta **todo el proyecto** como volumen (`.:/app`), no solo
la carpeta `trends/`.

### Proxy y captchas
- **Bright Data** residential proxy, zona `residential_proxy1`, IPs españolas (`-country-es`)
- **2Captcha** (`f49b4e9ed2e2b36add9c6ef3af3e6e4c`) y **CapSolver**
  (`CAP-80466E39...`) — claves hardcodeadas en `shared/config.py`, sobreescribibles por env var
- El proxy usa sticky sessions para que el browser y el solver compartan la misma IP
- **ADVERTENCIA**: Si el PC y el NAS ejecutan el tracker simultáneamente, competirán por el
  mismo proxy y las mismas claves de captcha. Usar solo uno a la vez.

---

## Requisitos previos en el NAS

- Synology DS720+/DS920+/DS923+ (o cualquier NAS Synology con CPU Intel x86_64)
- DSM 7.x
- **Container Manager** instalado (Package Center)
- **Git** instalado (Package Center → buscar "Git" o "Git Server")
- SSH habilitado
- Al menos **3 GB libres** en el volumen donde se clone el repo (imagen Docker ~1.5 GB +
  código + datos)

---

## Cómo hacer un despliegue desde cero (referencia)

### 1. Habilitar SSH en el NAS

DSM → **Panel de Control** → **Terminal y SNMP** → activar **Habilitar servicio SSH**
(puerto 22) → Aplicar.

### 2. Conectar por SSH desde el PC

```bash
ssh alvaro@192.168.1.2
```

### 3. Configurar alias Docker y clonar el repo

```bash
# Alias permanente para evitar problemas de permisos con el socket Docker:
echo "alias docker='sudo docker'" >> ~/.bashrc
echo "alias docker-compose='sudo docker-compose'" >> ~/.bashrc
source ~/.bashrc

# Clonar en la carpeta compartida docker (NO directamente en /volume1):
cd /volume1/docker
git clone https://github.com/alvguijarro/RealEstateMaster_ClaudeCode.git RealEstateMaster
cd RealEstateMaster
```

### 4. Verificar que los archivos Docker existen

```bash
ls -la Dockerfile docker-compose.yml
```

### 5. Construir la imagen Docker

```bash
docker build -t realestate-trends:latest .
```

- Primera vez: **5-15 minutos** (descarga ~300 MB de Chromium + deps del sistema)
- Si hay timeout de red, simplemente reintentar
- Solo hay que repetir este paso si cambia `requirements_master.txt` o el `Dockerfile`

### 6. Lanzar el contenedor

```bash
cd /volume1/docker/RealEstateMaster
docker-compose up -d
```

### 7. Verificar que el servicio responde

```bash
docker logs realestate-trends
# Debe aparecer: "Starting Trends Service on port 5005..."

curl http://localhost:5005/api/status
# Debe responder: {"running": false, ...}
```

Desde el navegador en la red local: `http://192.168.1.2:5005`

### 8. Programar el rastreo diario en DSM

DSM → **Panel de Control** → **Programador de Tareas** → **Crear** →
**Tarea programada** → **Script de usuario**

| Campo | Valor |
|-------|-------|
| Nombre | `Market Trends Tracker` |
| Usuario | `root` |
| Horario | Diario, hora deseada (recomendado: 02:00 o 03:00) |
| Script | `curl -s -X POST http://localhost:5005/api/start_tracker` |

> ⚠️ No usar `docker exec realestate-trends curl ...` — `curl` no está disponible dentro
> del contenedor (`python:3.11-slim` no lo incluye). Llamar directamente al puerto expuesto
> desde el host del NAS.

Pestaña **Configuración de la tarea** → activar **Enviar detalles de ejecución por email**
si se quieren notificaciones de fallo.

---

## Operación diaria

### Iniciar rastreo manualmente

```bash
# Nuevo rastreo desde el principio:
curl -X POST http://192.168.1.2:5005/api/start_tracker

# Reanudar desde el checkpoint (si se interrumpió a medias):
curl -X POST http://192.168.1.2:5005/api/resume_tracker
```

### Parar el rastreo

```bash
curl -X POST http://192.168.1.2:5005/api/stop_tracker
```

### Ver logs en tiempo real

```bash
docker logs -f realestate-trends
```

### Exportar datos a CSV

```bash
curl http://192.168.1.2:5005/api/export_csv -o trends_export.csv
```

---

## Actualizar el código en el futuro

```bash
ssh alvaro@192.168.1.2
cd /volume1/docker/RealEstateMaster
git pull origin main
```

**No hace falta reconstruir la imagen** salvo que cambien `requirements_master.txt` o
el `Dockerfile`. Si cambian, ejecutar:

```bash
docker-compose down
docker build -t realestate-trends:latest .
docker-compose up -d
```

### Añadir el remote claudecode al NAS (si se quiere tirar de ese repo)

```bash
git remote add claudecode https://github.com/alvguijarro/RealEstateMaster_ClaudeCode.git
git remote -v
```

---

## Gestión del contenedor

```bash
docker-compose stop                      # parar sin eliminar
docker-compose start                     # arrancar de nuevo
docker-compose down                      # parar y eliminar contenedor (imagen intacta)
docker-compose restart                   # reiniciar
docker ps                                # ver estado del contenedor
docker stats realestate-trends           # CPU y memoria en tiempo real
docker exec -it realestate-trends bash   # abrir shell dentro del contenedor
```

---

## Estructura de datos persistidos en el NAS

Al estar todo montado como volumen, los datos se guardan directamente en el repo clonado:

```
/volume1/docker/RealEstateMaster/
├── trends/data/
│   ├── market_trends.db          ← Base de datos SQLite principal
│   ├── checkpoint.json           ← Progreso del rastreo (para --resume)
│   ├── resume_point.json         ← Punto de reanudación cross-session
│   ├── execution_log.jsonl       ← Log histórico de ejecuciones
│   └── debug/                    ← Capturas de pantalla de bloqueos/captchas
├── trends/stealth_profile_*/     ← Perfiles de Chromium (se crean al primer uso)
└── scraper/documentation/
    ├── province_urls_mapping.md  ← URLs de Idealista por provincia
    └── subzones_complete.json    ← Mapeo completo de subzonas
```

---

## Notas técnicas importantes

- **`shm_size: 256mb`**: Chromium en Docker necesita más memoria compartida de la que
  asigna Docker por defecto (64 MB). Sin este parámetro, Chromium puede crashear al
  renderizar páginas con mucho JavaScript.
- **`--no-sandbox`**: Playwright lo gestiona automáticamente en entornos Docker/Linux.
  No hace falta configurarlo manualmente.
- **Host binding**: `trends/app.py` detecta `sys.platform` y escucha en `0.0.0.0` en
  Linux (necesario para que el puerto sea accesible desde fuera del contenedor) y en
  `127.0.0.1` en Windows (comportamiento original).
- **Perfiles stealth**: Se crean en `trends/stealth_profile_chromium/` etc. al primer
  rastreo y persisten entre ejecuciones gracias al volumen montado.
- **Concurrencia PC+NAS**: Si ambos corren a la vez comparten el mismo proxy Bright Data
  (zona `residential_proxy1`) y las mismas claves 2Captcha/CapSolver. Esto puede causar
  IP mismatch en DataDome. Usar solo uno a la vez.

---

## Troubleshooting

### El contenedor arranca pero curl /api/status no responde

```bash
docker logs realestate-trends   # buscar errores de import o de puerto
docker exec -it realestate-trends python -c "import playwright; print('ok')"
```

### Error "No such file or directory: province_urls_mapping.md"

El volumen no se montó correctamente. Verificar:
```bash
pwd    # debe ser /volume1/docker/RealEstateMaster
ls scraper/documentation/province_urls_mapping.md   # debe existir
```

### Error de Chromium al lanzar browser

```bash
docker exec -it realestate-trends playwright install chromium --with-deps
```

### El rastreo falla con muchos captchas seguidos

Normal si Idealista ha bloqueado el pool de IPs. Esperar 24h o cambiar el rango de IPs
en `shared/proxy_config.py` (quitar `-country-es` para usar IPs internacionales).

### La tarea DSM devuelve error 126 / "curl not found"

Ocurre si el script de la tarea usa `docker exec realestate-trends curl ...`.
`python:3.11-slim` no incluye `curl`. Cambiar el script a:
```bash
curl -s -X POST http://localhost:5005/api/start_tracker
```
(llamada directa al puerto expuesto, sin entrar al contenedor)

### Ver qué provincias se han completado

```bash
docker exec realestate-trends python -c "
import sqlite3
conn = sqlite3.connect('trends/data/market_trends.db')
rows = conn.execute('SELECT DISTINCT province, date_record FROM inventory_trends ORDER BY date_record DESC LIMIT 20').fetchall()
for r in rows: print(r)
"
```

### Ver grupos del usuario en Synology (ash shell)

```bash
id    # 'groups' no existe en ash; usar 'id' en su lugar
```

---

## Remotes Git de referencia

| Remote | URL | Uso |
|--------|-----|-----|
| `origin` | `https://github.com/alvguijarro/RealEstateMaster.git` | Repo original del usuario |
| `claudecode` | `https://github.com/alvguijarro/RealEstateMaster_ClaudeCode.git` | Workspace de Claude Code (siempre al día) |
