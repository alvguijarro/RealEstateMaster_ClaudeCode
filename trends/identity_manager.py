"""
Gestión de identidad (rotación de perfiles de navegador) para el Trends Tracker.

Versión aislada de las funciones de rotación de identidad de scraper_wrapper.py /
update_urls.py, con rutas propias bajo trends/ para no interferir con el scraper
principal.

Ficheros propios (no compartidos con el scraper principal):
  - trends/data/identity_state.json  (estado de rotación y cooldowns)
  - trends/stealth_profile_<N>/      (perfiles persistentes de Playwright)
"""
import os
import sys
import time
import json
from pathlib import Path

# Asegurar que el proyecto root está en sys.path para importar idealista_scraper
_TRENDS_DIR = Path(__file__).parent
_PROJECT_ROOT = _TRENDS_DIR.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))
_SCRAPER_DIR = _PROJECT_ROOT / "scraper"
if str(_SCRAPER_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRAPER_DIR))

from idealista_scraper.config import BROWSER_ROTATION_POOL, PROFILE_COOLDOWN_MINUTES

# Fichero de estado propio del Trends Tracker (no compartido con el scraper principal)
_DATA_DIR = _TRENDS_DIR / "data"
IDENTITY_STATE_FILE = str(_DATA_DIR / "identity_state.json")


def load_identity_state() -> dict:
    """Carga el estado de identidad (índice actual y cooldowns)."""
    if not os.path.exists(IDENTITY_STATE_FILE):
        return {"current_index": 0, "cooldowns": {}}
    try:
        with open(IDENTITY_STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError):
        return {"current_index": 0, "cooldowns": {}}


def save_identity_state(state: dict) -> None:
    """Guarda el estado de identidad."""
    try:
        os.makedirs(os.path.dirname(IDENTITY_STATE_FILE), exist_ok=True)
        with open(IDENTITY_STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2)
    except IOError:
        pass


def get_profile_dir(profile_index: int) -> str:
    """Devuelve el directorio de perfil persistente propio del Trends Tracker."""
    return str(_TRENDS_DIR / f"stealth_profile_{profile_index}")


def mark_current_profile_blocked() -> None:
    """Marca el perfil actual como bloqueado e inicia su cooldown."""
    state = load_identity_state()
    current_idx = state.get("current_index", 0)
    if current_idx >= len(BROWSER_ROTATION_POOL) or current_idx < 0:
        current_idx = 0
    config = BROWSER_ROTATION_POOL[current_idx]
    pool_id = str(config["index"])
    state["cooldowns"][pool_id] = time.time()
    save_identity_state(state)
    print(f"🚫 Trends profile {pool_id} ({config['name']}) marcado como BLOQUEADO a las {time.ctime()}")


def rotate_identity():
    """
    Rota al siguiente perfil disponible en round-robin.
    Si todos están en cooldown, devuelve el que menos tiempo de espera tenga.
    Retorna (profile_config, wait_seconds).
    """
    state = load_identity_state()
    current_idx = state.get("current_index", 0)
    pool_size = len(BROWSER_ROTATION_POOL)

    cooldown_seconds = PROFILE_COOLDOWN_MINUTES * 60
    now = time.time()

    # Limpiar cooldowns expirados
    for pid in list(state["cooldowns"].keys()):
        if now - state["cooldowns"][pid] >= cooldown_seconds:
            del state["cooldowns"][pid]

    # Buscar el siguiente disponible en round-robin
    available_indices = []
    for i in range(pool_size):
        idx = (current_idx + 1 + i) % pool_size
        pid = str(BROWSER_ROTATION_POOL[idx]["index"])
        if pid not in state["cooldowns"]:
            available_indices.append(idx)

    if available_indices:
        next_idx = available_indices[0]
        state["current_index"] = next_idx
        save_identity_state(state)
        return BROWSER_ROTATION_POOL[next_idx], 0

    # Todos en cooldown: esperar al de menor tiempo restante
    wait_info = []
    for i in range(pool_size):
        config = BROWSER_ROTATION_POOL[i]
        pid = str(config["index"])
        blocked_time = state["cooldowns"].get(pid, now)
        remaining = max(1, cooldown_seconds - (now - blocked_time))
        wait_info.append((remaining, i))

    wait_info.sort()
    min_wait, next_idx = wait_info[0]
    state["current_index"] = next_idx
    save_identity_state(state)
    return BROWSER_ROTATION_POOL[next_idx], min_wait
