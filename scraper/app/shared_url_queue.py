"""SharedURLQueue — cola asyncio compartida entre workers paralelos.

Soporta dos modos:
  1. Lista fija (enrichment): se pasa una lista de URLs al constructor.
  2. Productor-consumidor (scraping paralelo): se crea vacía, el productor
     llama a put() para añadir URLs, y los consumidores llaman a claim().
     Cuando el productor ha terminado, llama a close().

Cada URL solo la reclama un worker. Segura para asyncio (hilo único).
"""
from __future__ import annotations

import asyncio
from typing import Optional


class SharedURLQueue:
    """Cola de URLs compartida entre múltiples coroutines asyncio."""

    def __init__(self, urls: list = None) -> None:
        self._pending: asyncio.Queue = asyncio.Queue()
        self._claimed: set = set()
        self._lock: asyncio.Lock = asyncio.Lock()
        self._total: int = 0
        self._closed: bool = False
        if urls:
            for u in urls:
                self._pending.put_nowait(u)
                self._total += 1

    async def put(self, url: str) -> None:
        """Añade una URL a la cola (lado productor)."""
        self._pending.put_nowait(url)
        self._total += 1

    def close(self) -> None:
        """Señala que no se añadirán más URLs. Los workers que esperen
        recibirán None en su próximo claim() cuando la cola se vacíe."""
        self._closed = True

    @property
    def is_closed(self) -> bool:
        return self._closed

    async def claim(self) -> Optional[str]:
        """Obtiene y reserva la siguiente URL pendiente.

        - Si hay URLs en la cola, devuelve una inmediatamente.
        - Si la cola está vacía pero NO cerrada, espera 0.5s y reintenta
          (el productor puede seguir añadiendo URLs).
        - Si la cola está vacía Y cerrada, devuelve None (fin).
        """
        while True:
            async with self._lock:
                if not self._pending.empty():
                    url = self._pending.get_nowait()
                    self._claimed.add(url)
                    return url
                if self._closed:
                    return None
            # Cola vacía pero no cerrada — esperar por nuevas URLs
            await asyncio.sleep(0.5)

    @property
    def total(self) -> int:
        """Número total de URLs registradas en la cola."""
        return self._total

    def remaining(self) -> int:
        """Número de URLs pendientes todavía sin reclamar."""
        return self._pending.qsize()

    def claimed_count(self) -> int:
        """Número de URLs ya reclamadas (en proceso o completadas)."""
        return len(self._claimed)

    def pending_count(self) -> int:
        """Número de URLs pendientes todavía en la cola (sin reclamar)."""
        return self._pending.qsize()

    async def release(self, url: str) -> None:
        """Devuelve a la cola una URL reclamada pero no completada."""
        async with self._lock:
            if url in self._claimed:
                self._claimed.discard(url)
                await self._pending.put(url)
