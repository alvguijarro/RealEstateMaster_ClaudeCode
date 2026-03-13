"""
Parallel worker launcher for automated scraping cycles.

Launches N independent scraper server processes (each with its own IP, profiles
and state files) and distributes provinces across them concurrently.

Usage:
    python scripts/parallel_worker_launcher.py [--workers N] [--resume] [--timeout-hours H]

Arguments:
    --workers N       Number of parallel workers (default=5)
    --resume          Resume from saved parallel_cycle_state.json
    --timeout-hours H Per-task timeout in hours (default=4)
"""
from __future__ import annotations

import argparse
import json
import os
import random
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import requests

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).parent.parent
SCRAPER_DIR = BASE_DIR / "scraper"
APP_DIR = SCRAPER_DIR / "app"
SCRIPTS_DIR = BASE_DIR / "scripts"
STATE_FILE = SCRAPER_DIR / "parallel_cycle_state.json"
VENTA_FILE = SCRAPER_DIR / "documentation" / "idealista_urls_venta.md"
ALQUILER_FILE = SCRAPER_DIR / "documentation" / "idealista_urls_alquiler.md"

# Port base: worker 1 -> 5003, worker 2 -> 5013, worker 3 -> 5023, ...
BASE_PORT = 5003
PORT_STEP = 10

MAX_RETRIES = 2  # max task retries before marking as failed


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
def log(msg: str, level: str = "INFO") -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] [{level}] {msg}", flush=True)


# ---------------------------------------------------------------------------
# Province list parsing (same format as automated_cycle.py)
# ---------------------------------------------------------------------------
def parse_markdown_table(file_path: Path) -> list[dict]:
    """Extract province/url pairs from the markdown table files."""
    urls: list[dict] = []
    if not file_path.exists():
        log(f"File not found: {file_path}", "ERR")
        return urls
    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            if "|" in line and "http" in line:
                parts = [p.strip() for p in line.split("|")]
                if len(parts) >= 4:
                    prov = parts[2]
                    url_raw = parts[3]
                    if "(" in url_raw and ")" in url_raw:
                        url = url_raw.split("(")[1].split(")")[0]
                    else:
                        url = url_raw
                    urls.append({"province": prov, "url": url})
    return urls


def build_task_queue() -> list[dict]:
    """Build the full task queue: all alquiler tasks then all venta tasks."""
    alquiler = parse_markdown_table(ALQUILER_FILE)
    venta = parse_markdown_table(VENTA_FILE)
    tasks: list[dict] = []
    for item in alquiler:
        tasks.append({"province": item["province"], "url": item["url"], "operation": "alquiler"})
    for item in venta:
        tasks.append({"province": item["province"], "url": item["url"], "operation": "venta"})
    return tasks


# ---------------------------------------------------------------------------
# State persistence
# ---------------------------------------------------------------------------
def load_state() -> dict:
    if STATE_FILE.exists():
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            log(f"Error loading state: {e}", "WARN")
    return {}


def save_state(state: dict) -> None:
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, ensure_ascii=False)


# ---------------------------------------------------------------------------
# Worker process management
# ---------------------------------------------------------------------------
def worker_port(worker_id: int) -> int:
    return BASE_PORT + (worker_id - 1) * PORT_STEP


def worker_url(worker_id: int) -> str:
    return f"http://127.0.0.1:{worker_port(worker_id)}"


def is_worker_ready(worker_id: int) -> bool:
    try:
        resp = requests.get(f"{worker_url(worker_id)}/health", timeout=3)
        return resp.status_code == 200
    except Exception:
        return False


def launch_worker(worker_id: int) -> subprocess.Popen:
    """Launch a scraper server process for the given worker ID."""
    server_script = APP_DIR / "server.py"
    env = os.environ.copy()
    env["SCRAPER_WORKER_ID"] = str(worker_id)
    env["SCRAPER_PORT"] = str(worker_port(worker_id))
    proc = subprocess.Popen(
        [sys.executable, str(server_script)],
        cwd=str(SCRAPER_DIR),
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
    )
    log(f"Worker {worker_id} launched (PID={proc.pid}, port={worker_port(worker_id)})")
    return proc


def wait_for_worker(worker_id: int, timeout: int = 60) -> bool:
    """Poll until the worker's HTTP server responds or timeout."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if is_worker_ready(worker_id):
            log(f"Worker {worker_id} is UP on port {worker_port(worker_id)}")
            return True
        time.sleep(2)
    log(f"Worker {worker_id} did NOT become ready within {timeout}s", "ERR")
    return False


def stop_all_workers(procs: dict[int, subprocess.Popen]) -> None:
    log("Stopping all worker processes...")
    for wid, proc in procs.items():
        try:
            proc.terminate()
            proc.wait(timeout=10)
            log(f"Worker {wid} stopped (PID={proc.pid})")
        except Exception as e:
            log(f"Error stopping worker {wid}: {e}", "WARN")
            try:
                proc.kill()
            except Exception:
                pass


# ---------------------------------------------------------------------------
# Task API interaction
# ---------------------------------------------------------------------------
def start_task(worker_id: int, task: dict) -> bool:
    """Send start-batch request to a worker. Returns True if accepted."""
    payload = {
        "urls": [task["url"]],
        "mode": "stealth",
        "use_vpn": True,
        "smart_enrichment": True,
        "parallel_enrichment": True,
    }
    try:
        resp = requests.post(
            f"{worker_url(worker_id)}/api/start-batch",
            json=payload,
            timeout=10,
        )
        return resp.status_code == 200
    except Exception as e:
        log(f"Worker {worker_id}: error starting task: {e}", "WARN")
        return False


def get_worker_status(worker_id: int) -> str | None:
    """Return the scraper status string for a worker, or None on error."""
    try:
        resp = requests.get(f"{worker_url(worker_id)}/api/status", timeout=5)
        if resp.status_code == 200:
            data = resp.json()
            internal = data.get("internal_status") or data.get("status", "")
            return internal
    except Exception:
        pass
    return None


def get_worker_progress(worker_id: int) -> str:
    """Return a short progress string for the status table."""
    try:
        resp = requests.get(f"{worker_url(worker_id)}/api/status", timeout=5)
        if resp.status_code == 200:
            data = resp.json()
            page = data.get("current_page", "?")
            total = data.get("total_pages", "?")
            return f"pag {page}/{total}"
    except Exception:
        pass
    return ""


# ---------------------------------------------------------------------------
# Main coordinator
# ---------------------------------------------------------------------------
def print_status_table(
    workers: int,
    worker_tasks: dict[int, dict | None],
    worker_statuses: dict[int, str],
    completed: int,
    total: int,
    failed: int,
    pending: int,
) -> None:
    now = datetime.now().strftime("%H:%M")
    print(f"\n[{now}] === STATUS ===")
    for wid in range(1, workers + 1):
        task = worker_tasks.get(wid)
        status = worker_statuses.get(wid, "UNKNOWN")
        if task:
            desc = f"{task['province']} {task['operation']}"
        else:
            desc = "Esperando tarea"
        print(f"  Worker {wid} ({worker_port(wid)}): {status.upper():<12} — {desc}")
    print(f"  Completadas: {completed}/{total} | Fallidas: {failed} | Pendientes: {pending}")
    print()


def run(num_workers: int, resume: bool, timeout_hours: float) -> None:
    log(f"=== PARALLEL CYCLE LAUNCHER ({num_workers} workers) ===")

    task_timeout = timedelta(hours=timeout_hours)

    # Build or restore task queue
    all_tasks = build_task_queue()
    total_tasks = len(all_tasks)

    if resume and STATE_FILE.exists():
        state = load_state()
        log(f"Resuming from saved state ({STATE_FILE})")
    else:
        state = {
            "started_at": datetime.now().isoformat(),
            "tasks": [
                {**t, "status": "pending", "retries": 0, "worker": None, "started_at": None}
                for t in all_tasks
            ],
        }
        save_state(state)

    tasks: list[dict] = state["tasks"]

    # ---------------------------------------------------------------------------
    # Launch workers with staggered starts
    # ---------------------------------------------------------------------------
    procs: dict[int, subprocess.Popen] = {}
    for wid in range(1, num_workers + 1):
        if wid > 1:
            delay = random.randint(30, 90)
            log(f"Staggering worker {wid} start by {delay}s...")
            time.sleep(delay)
        procs[wid] = launch_worker(wid)

    # Wait for all workers to be ready
    ready: dict[int, bool] = {}
    for wid in range(1, num_workers + 1):
        ready[wid] = wait_for_worker(wid, timeout=90)
        if not ready[wid]:
            log(f"Worker {wid} failed to start. Aborting.", "ERR")
            stop_all_workers(procs)
            sys.exit(1)

    # ---------------------------------------------------------------------------
    # Coordinator loop
    # ---------------------------------------------------------------------------
    worker_tasks: dict[int, dict | None] = {wid: None for wid in range(1, num_workers + 1)}
    worker_statuses: dict[int, str] = {wid: "idle" for wid in range(1, num_workers + 1)}
    worker_task_started: dict[int, datetime | None] = {wid: None for wid in range(1, num_workers + 1)}

    def pending_tasks() -> list[dict]:
        return [t for t in tasks if t["status"] == "pending"]

    def running_tasks() -> list[dict]:
        return [t for t in tasks if t["status"] == "running"]

    def completed_count() -> int:
        return sum(1 for t in tasks if t["status"] == "completed")

    def failed_count() -> int:
        return sum(1 for t in tasks if t["status"] == "failed")

    try:
        while True:
            # Check if all tasks are done
            if not pending_tasks() and not running_tasks():
                log(f"=== ALL TASKS DONE. Completed: {completed_count()}, Failed: {failed_count()} ===")
                break

            # Poll each worker
            for wid in range(1, num_workers + 1):
                current_task = worker_tasks[wid]

                if current_task is not None:
                    # Check for timeout
                    if worker_task_started[wid] and datetime.now() - worker_task_started[wid] > task_timeout:
                        log(f"Worker {wid}: task TIMED OUT ({task_timeout}) — {current_task['province']} {current_task['operation']}", "WARN")
                        current_task["status"] = "pending"  # requeue
                        current_task["retries"] = current_task.get("retries", 0) + 1
                        if current_task["retries"] > MAX_RETRIES:
                            log(f"Worker {wid}: max retries reached for {current_task['province']} {current_task['operation']}", "ERR")
                            current_task["status"] = "failed"
                        worker_tasks[wid] = None
                        worker_task_started[wid] = None
                        worker_statuses[wid] = "idle"
                        save_state(state)
                        continue

                    # Check if worker process is still alive
                    if procs[wid].poll() is not None:
                        log(f"Worker {wid}: process died unexpectedly — restarting...", "WARN")
                        procs[wid] = launch_worker(wid)
                        if not wait_for_worker(wid, timeout=90):
                            log(f"Worker {wid}: failed to restart", "ERR")
                            current_task["status"] = "pending"
                            current_task["retries"] = current_task.get("retries", 0) + 1
                            if current_task["retries"] > MAX_RETRIES:
                                current_task["status"] = "failed"
                            worker_tasks[wid] = None
                            worker_task_started[wid] = None
                            worker_statuses[wid] = "idle"
                            save_state(state)
                            continue

                    status = get_worker_status(wid)
                    worker_statuses[wid] = status or "unknown"

                    if status in ("completed", "stopped", "error", None):
                        if status == "completed":
                            log(f"Worker {wid}: completed — {current_task['province']} {current_task['operation']}")
                            current_task["status"] = "completed"
                        else:
                            log(f"Worker {wid}: task ended with status={status} — {current_task['province']} {current_task['operation']}", "WARN")
                            current_task["retries"] = current_task.get("retries", 0) + 1
                            if current_task["retries"] > MAX_RETRIES:
                                log(f"Max retries reached for {current_task['province']} {current_task['operation']}", "ERR")
                                current_task["status"] = "failed"
                            else:
                                current_task["status"] = "pending"

                        worker_tasks[wid] = None
                        worker_task_started[wid] = None
                        worker_statuses[wid] = "idle"
                        save_state(state)

                        # Inter-task pause for this worker
                        delay = random.randint(45, 120)
                        log(f"Worker {wid}: resting {delay}s before next task...")
                        time.sleep(delay)

                else:
                    # Worker is idle — assign next pending task
                    pending = pending_tasks()
                    if pending:
                        next_task = pending[0]
                        log(f"Worker {wid}: assigning — {next_task['province']} {next_task['operation']}")
                        if start_task(wid, next_task):
                            next_task["status"] = "running"
                            next_task["worker"] = wid
                            next_task["started_at"] = datetime.now().isoformat()
                            worker_tasks[wid] = next_task
                            worker_task_started[wid] = datetime.now()
                            worker_statuses[wid] = "running"
                            save_state(state)
                        else:
                            log(f"Worker {wid}: failed to start task — will retry on next poll", "WARN")

            # Status table
            print_status_table(
                num_workers,
                worker_tasks,
                worker_statuses,
                completed=completed_count(),
                total=total_tasks,
                failed=failed_count(),
                pending=len(pending_tasks()),
            )

            time.sleep(30)

    except KeyboardInterrupt:
        log("Interrupted by user. Saving state...")
        save_state(state)

    finally:
        stop_all_workers(procs)

    # Final summary
    log(f"=== CYCLE COMPLETE ===")
    log(f"Total: {total_tasks} | Completed: {completed_count()} | Failed: {failed_count()}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(description="Parallel scraper worker launcher")
    parser.add_argument("--workers", type=int, default=5, help="Number of parallel workers (default=5)")
    parser.add_argument("--resume", action="store_true", help="Resume from saved state")
    parser.add_argument("--timeout-hours", type=float, default=4.0, help="Per-task timeout in hours (default=4)")
    args = parser.parse_args()

    if args.workers < 1:
        print("--workers must be >= 1")
        sys.exit(1)
    if args.workers > 8:
        print("Warning: more than 8 workers is not recommended (high block risk)")

    run(num_workers=args.workers, resume=args.resume, timeout_hours=args.timeout_hours)


if __name__ == "__main__":
    main()
