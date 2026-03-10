# Despliegue del Market Trends Tracker en Synology NAS

## Estado actual (sesión completada)

**Todo el trabajo de código ya está hecho y subido.** El commit `f389c523` en el remote
`claudecode` contiene los tres cambios necesarios para que el tracker funcione en Linux/Docker:

| Archivo | Cambio | Estado |
|---------|--------|--------|
| `trends/app.py` | `CREATE_NO_WINDOW` condicional a `win32` (×2) | ✅ Hecho |
| `trends/app.py` | `taskkill` → `SIGKILL` en Linux (×1) | ✅ Hecho |
| `trends/app.py` | Host binding `0.0.0.0` en Linux, `127.0.0.1` en Windows | ✅ Hecho |
| `Dockerfile` | Python 3.11-slim + Playwright + Chromium | ✅ Hecho |
| `docker-compose.yml` | Servicio con volumen, shm_size=256mb, restart=unless-stopped | ✅ Hecho |

El NAS ejecutará **únicamente** el servicio `trends` (puerto 5005). El resto de servicios
(analyzer, scraper, merger, dashboard) seguirán corriendo en el PC Windows.

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

## Despliegue inicial (paso a paso)

### 1. Habilitar SSH en el NAS

DSM → **Panel de Control** → **Terminal y SNMP** → activar **Habilitar servicio SSH**
(puerto 22) → Aplicar.

### 2. Conectar por SSH desde el PC

```bash
ssh admin@192.168.1.XXX    # reemplaza con la IP real del NAS
# Ver la IP en: DSM → Panel de Control → Red → Interfaz de red
```

### 3. Ir al volumen con más espacio y clonar el repo

```bash
df -h                      # ver espacio disponible por volumen
cd /volume1                # o /volume2 según el resultado anterior

git clone https://github.com/alvguijarro/RealEstateMaster_ClaudeCode.git RealEstateMaster
cd RealEstateMaster
```

> El repo `claudecode` (`RealEstateMaster_ClaudeCode`) es el workspace de Claude Code y
> siempre está al día. El repo `origin` (`RealEstateMaster`) es el del usuario.

### 4. (Opcional) Verificar que los archivos Docker existen

```bash
ls -la Dockerfile docker-compose.yml
cat Dockerfile
```

Deben existir con el contenido correcto (ver sección "Estado actual" arriba).

### 5. Construir la imagen Docker

```bash
docker build -t realestate-trends:latest .
```

- Primera vez: **5-15 minutos** (descarga ~300 MB de Chromium + deps del sistema)
- Solo hay que repetir este paso si cambia `requirements_master.txt` o el `Dockerfile`

Si hay errores de memoria durante el build, añadir `--memory=2g`:
```bash
docker build --memory=2g -t realestate-trends:latest .
```

### 6. Lanzar el contenedor

```bash
docker-compose up -d
```

O manualmente sin docker-compose:
```bash
docker run -d \
  --name realestate-trends \
  -p 5005:5005 \
  -v $(pwd):/app \
  --shm-size=256mb \
  --restart unless-stopped \
  realestate-trends:latest
```

### 7. Verificar que el servicio responde

```bash
# Ver logs del arranque (esperar ~5 segundos):
docker logs realestate-trends
# Debe aparecer: "Starting Trends Service on port 5005..."

# Comprobar endpoint de estado:
curl http://localhost:5005/api/status
# Debe responder: {"running": false, ...}
```

Desde el navegador en la red local: `http://192.168.1.XXX:5005`

### 8. Programar el rastreo diario en DSM

DSM → **Panel de Control** → **Programador de Tareas** → **Crear** →
**Tarea programada** → **Script de usuario**

| Campo | Valor |
|-------|-------|
| Nombre | `Market Trends Tracker` |
| Usuario | `root` |
| Horario | Diario, hora deseada (recomendado: 02:00 o 03:00) |
| Script | `docker exec realestate-trends curl -s -X POST http://localhost:5005/api/start_tracker` |

Pestaña **Configuración de la tarea** → activar **Enviar detalles de ejecución por email**
si se quieren notificaciones de fallo.

---

## Operación diaria

### Iniciar rastreo manualmente

```bash
# Nuevo rastreo desde el principio:
curl -X POST http://192.168.1.XXX:5005/api/start_tracker

# Reanudar desde el checkpoint (si se interrumpió a medias):
curl -X POST http://192.168.1.XXX:5005/api/resume_tracker
```

### Parar el rastreo

```bash
curl -X POST http://192.168.1.XXX:5005/api/stop_tracker
```

### Ver logs en tiempo real

```bash
docker logs -f realestate-trends
```

### Exportar datos a CSV

```bash
curl http://192.168.1.XXX:5005/api/export_csv -o trends_export.csv
```

---

## Actualizar el código en el futuro

```bash
ssh admin@192.168.1.XXX
cd /volume1/RealEstateMaster
git pull origin main          # desde el repo del usuario
# o:
git pull claudecode main       # desde el repo de Claude Code (si se añadió el remote)
```

**No hace falta reconstruir la imagen** salvo que cambien `requirements_master.txt` o
el `Dockerfile`. Si cambian, ejecutar:

```bash
docker-compose down
docker build -t realestate-trends:latest .
docker-compose up -d
```

### Añadir el remote claudecode al NAS (opcional pero recomendado)

```bash
git remote add claudecode https://github.com/alvguijarro/RealEstateMaster_ClaudeCode.git
git remote -v    # verificar ambos remotes
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
/volume1/RealEstateMaster/
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

El volumen no se montó correctamente o el path en el NAS no es `/app`.
Verificar que `$(pwd)` al hacer `docker run` apunta al directorio correcto:
```bash
pwd    # debe ser /volume1/RealEstateMaster
ls scraper/documentation/province_urls_mapping.md   # debe existir
```

### Error de Chromium al lanzar browser

```bash
docker exec -it realestate-trends playwright install chromium --with-deps
```

### El rastreo falla con muchos captchas seguidos

Normal si Idealista ha bloqueado el pool de IPs. Esperar 24h o cambiar el rango de IPs
en `shared/proxy_config.py` (quitar `-country-es` para usar IPs internacionales).

### Ver qué provincias se han completado

```bash
docker exec realestate-trends python -c "
import sqlite3
conn = sqlite3.connect('trends/data/market_trends.db')
rows = conn.execute('SELECT DISTINCT province, date_record FROM inventory_trends ORDER BY date_record DESC LIMIT 20').fetchall()
for r in rows: print(r)
"
```

---

## Remotes Git de referencia

| Remote | URL | Uso |
|--------|-----|-----|
| `origin` | `https://github.com/alvguijarro/RealEstateMaster.git` | Repo original del usuario |
| `claudecode` | `https://github.com/alvguijarro/RealEstateMaster_ClaudeCode.git` | Workspace de Claude Code (siempre al día) |
