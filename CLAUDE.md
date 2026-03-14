# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

RealEstateMaster is an automated Spanish real estate intelligence system targeting Idealista. It scrapes property listings (Venta/Alquiler) across all Spanish provinces, merges data, and provides investment analysis dashboards. The system runs on Windows with an embedded portable Python environment.

## Environment

- **Platform**: Windows 11, but shell commands use bash (Git Bash). Use Unix path separators and redirections.
- **Python**: Use `python_portable/python.exe` for scraper execution. System `python3` is not in PATH. For syntax checks and quick scripts, `python` (system) works.
- **GitHub CLI**: `gh` is NOT installed. Use `curl` with git credentials for GitHub API.
- **Git remotes**: `origin` = main repo (`alvguijarro/RealEstateMaster.git`), `claudecode` = workspace remote (`alvguijarro/RealEstateMaster_ClaudeCode.git`). Push workspace changes to `claudecode`.

## Running the Application

```bash
# Start everything (dashboard on :5000 launches all services as subprocesses)
./START.bat                        # or: python_portable/python.exe main.py

# Start automated 14-day province scraping cycle
./LAUNCH_AUTO_CYCLE.bat            # or: python_portable/python.exe scripts/automated_cycle.py

# Stop all services
./STOP_ALL.bat
```

Individual services can be started directly:
- Scraper server: `python_portable/python.exe scraper/app/server.py` (port 5003)
- Analyzer: `python_portable/python.exe analyzer/app.py` (port 5001)
- Trends: `python_portable/python.exe trends/app.py` (port 5005)

There are no automated tests. Verify changes by running the scraper against a live URL and checking logs.

## Architecture

### Microservices (Flask, all launched by `main.py`)

| Service | Port | Entry Point | Purpose |
|---------|------|-------------|---------|
| Dashboard | 5000 | `main.py` | Master launcher, spawns other services |
| Analyzer | 5001 | `analyzer/app.py` | Investment analysis: venta vs alquiler, yields, zone ranking |
| Merger | 5002 | `merger/app.py` | Excel file fusion |
| Scraper | 5003 | `scraper/app/server.py` | Playwright scraper with WebSocket status updates |
| Metrics | 5004 | `dashboard/app.py` | KPI dashboard |
| Trends | 5005 | `trends/app.py` | Price tracking over time (SQLite) |

Ports are centralized in `shared/config.py`.

### Scraper Internals (the most complex subsystem)

The scraper has two layers:

1. **`scraper/idealista_scraper/`** — Core scraping library
   - `scraper.py` (574 lines): Orchestration, pagination, session state
   - `utils.py` (~1450 lines): Stealth helpers, captcha solvers (DataDome/CapSolver only), identity rotation, human simulation
   - `extractors.py`: DOM field extraction (40+ property fields)
   - `config.py`: Timing presets (stealth/fast/extra-stealth), browser rotation pool, viewport/UA rotation
   - `regex_patterns.py`: Compiled regexes for property data parsing

2. **`scraper/app/`** — Web wrapper and server
   - `server.py`: Flask-SocketIO server with REST API and real-time WebSocket updates
   - `scraper_wrapper.py` (~4000 lines): The largest file. Wraps the core scraper with pause/stop, browser lifecycle (launch/identity rotation/profile management), proxy integration, block detection, and captcha orchestration. This is where Playwright browsers are launched and where `_goto_with_retry` handles navigation + captcha detection.

### Anti-Bot & Captcha Pipeline

Detection flow in `scraper_wrapper.py._goto_with_retry` → `utils.handle_captcha_v2`:
1. Navegación jerárquica (home → provincia → zona → URL objetivo) para esquivar DataDome
2. CapSolver DatadomeSliderTask ×3 (requires `CAPSOLVER_API_KEY` in env)
3. Para non-DataDome: local slider automático
4. Si falla: cooldown 15min (cerrar browser, regenerar proxy, relanzar)

Key constraint: The proxy (Bright Data residential, configured in `shared/proxy_config.py`) must use sticky sessions so browser and captcha solver share the same IP. The `t=bv` parameter in DataDome's captcha URL means the IP is permanently blocked — solver will skip and rotate identity.

### Browser Rotation

Definido en `scraper/idealista_scraper/config.py` como `WORKER_POOL` (5 workers: Chromium, Chrome, Edge, Opera, Iron). Worker 1 es visible, workers 2-5 son headless. Todos usan proxy residencial con sticky sessions diferentes. Non-standard channels (opera, iron, brave) requieren detección de ejecutable local — el `channel` de Playwright solo soporta `chrome` y `msedge`.

**Important**: `update_urls.py` and `trends_tracker.py` also launch browsers and must filter out non-standard channel values before passing to Playwright.

### Shared Configuration

- `shared/config.py`: Ports, API keys (RapidAPI, Gemini, CapSolver), project paths
- `shared/proxy_config.py`: Bright Data residential proxy with sticky session generation
- API keys have hardcoded fallbacks but can be overridden via environment variables

### Automation

`scripts/automated_cycle.py` runs a 14-day cycle scraping all provinces (Alquiler then Venta). State persisted in `scraper/cycle_state.json`. See `AUTOMATION_GUIDE.md` for Windows Task Scheduler setup.

### Despliegue en Synology NAS (Market Trends)

El servicio `trends` (puerto 5005) puede ejecutarse de forma autónoma en un Synology NAS (DS920+) vía Docker, permitiendo rastreo 24/7 sin depender del PC Windows.

- **NAS**: Synology DS920+, DSM 7.x, IP `192.168.1.2`
- **Ruta del repo en NAS**: `/volume1/docker/RealEstateMaster`
- **Usuario SSH**: `alvaro` (con alias `docker='sudo docker'` en `~/.bashrc`)
- **Archivos Docker**: `Dockerfile` (Python 3.11-slim + Playwright + Chromium) y `docker-compose.yml` (puerto 5005, volumen `.:/app`, `shm_size: 256mb`)
- **Tarea programada DSM**: ejecuta `curl -s -X POST http://localhost:5005/api/start_tracker` diariamente como `root`
- **Guía completa**: `docs/SYNOLOGY_NAS_DEPLOY.md`

Restricciones importantes:
- **No ejecutar el tracker en PC y NAS simultáneamente** — comparten proxy Bright Data y claves captcha, lo que causa IP mismatch en DataDome
- `docker-compose.yml` monta todo el proyecto como volumen (`.:/app`) porque `trends_tracker.py` importa módulos de `scraper/` y `shared/`
- `curl` no está disponible dentro del contenedor; las llamadas API se hacen desde el host del NAS al puerto expuesto

Operación básica desde el PC:
```bash
# Iniciar rastreo
curl -X POST http://192.168.1.2:5005/api/start_tracker

# Reanudar rastreo interrumpido
curl -X POST http://192.168.1.2:5005/api/resume_tracker

# Parar rastreo
curl -X POST http://192.168.1.2:5005/api/stop_tracker

# Ver logs
ssh alvaro@192.168.1.2 "docker logs -f realestate-trends"

# Actualizar código en el NAS
ssh alvaro@192.168.1.2 "cd /volume1/docker/RealEstateMaster && git pull"
```

## Key Files by Change Frequency

When modifying captcha/anti-bot logic: `scraper/idealista_scraper/utils.py`, `scraper/app/scraper_wrapper.py`
When modifying scraping behavior: `scraper/idealista_scraper/scraper.py`, `scraper/idealista_scraper/config.py`
When modifying analysis: `analyzer/analysis.py`
When modifying browser launch: `scraper/app/scraper_wrapper.py`, `scraper/update_urls.py`, `trends/trends_tracker.py`

## Tools & MCP Servers

- **Context7 MCP**: Always use Context7 MCP when needing library/API documentation, code generation, setup or configuration steps — without waiting for an explicit request.

## Conventions

- Commit messages and code comments are typically in Spanish
- Log messages use emoji prefixes for visual scanning in terminal
- The project uses no linter or formatter — match existing style
- Changes to scraper resilience should be verified by running against a real Idealista URL, not assumed correct
- Update `CHANGELOG.md` after structural changes

## Pushing Changes

After completing any set of code changes, push them to the `claudecode` remote (`https://github.com/alvguijarro/RealEstateMaster_ClaudeCode`):

```bash
git push claudecode main
```

The commit message **must be written in Spanish** and include a clear summary of what was changed and why. Follow this structure:

```
<tipo>(<ámbito>): <descripción breve en español>

- Detalle 1 de los cambios realizados
- Detalle 2 de los cambios realizados
- Motivo o contexto del cambio
```

Examples of valid commit types: `feat`, `fix`, `refactor`, `docs`, `chore`.
