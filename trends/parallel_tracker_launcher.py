#!/usr/bin/env python3
"""Lanza N instancias paralelas de trends_tracker.py, cada una con su propio proxy."""
import subprocess
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
PYTHON  = PROJECT_ROOT / "python_portable" / "python.exe"
TRACKER = PROJECT_ROOT / "trends" / "trends_tracker.py"


def main():
    import argparse
    p = argparse.ArgumentParser(description="Lanzador paralelo para trends_tracker.py")
    p.add_argument("--workers", type=int, default=5, help="Número de workers paralelos (default: 5)")
    p.add_argument("--headless", action="store_true", default=True, help="Ejecutar en modo headless")
    p.add_argument("--resume", action="store_true", default=True, help="Reanudar desde checkpoint")
    args = p.parse_args()

    print(f"[+] Iniciando trends paralelo con {args.workers} workers...")
    processes = []
    for worker_id in range(1, args.workers + 1):
        env = os.environ.copy()
        env["SCRAPER_WORKER_ID"]   = str(worker_id)
        env["SCRAPER_NUM_WORKERS"] = str(args.workers)
        cmd = [str(PYTHON), str(TRACKER)]
        if args.resume:
            cmd.append("--resume")
        if args.headless:
            cmd.append("--headless")
        proc = subprocess.Popen(cmd, env=env)
        print(f"[+] Trends Worker {worker_id} iniciado (PID {proc.pid})")
        processes.append((worker_id, proc))

    for worker_id, proc in processes:
        proc.wait()
        print(f"[✓] Trends Worker {worker_id} terminado (código {proc.returncode})")

    print("[✓] Todos los workers de trends han terminado.")


if __name__ == "__main__":
    main()
