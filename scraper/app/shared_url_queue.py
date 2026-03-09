"""SharedURLQueue — cola asyncio compartida entre workers de enriquecimiento paralelo.

Permite que dos coroutines (worker principal con proxy + worker WebKit sin proxy)
procesen la misma lista de URLs sin solapamientos: cada URL solo la reclama un worker.
"""
from __future__ import annotations

import asyncio
from typing import Optional


class SharedURLQueue:
    """Cola de URLs compartida entre multiples coroutines asyncio.

    Garantiza semantica claim-once: una vez que un worker reclama una URL,
    ningun otro worker la recibira aunque el primero falle.

    Segura para uso dentro del mismo event loop (asyncio, hilo unico).
    """

    def __init__(self, urls: list) -> None:
        self._pending: asyncio.Queue = asyncio.Queue()
        self._claimed: set = set()
        self._lock: asyncio.Lock = asyncio.Lock()
        self._total: int = 0
        for u in urls:
            self._pending.put_nowait(u)
            self._total += 1

    async def claim(self) -> Optional[str]:
        """Obtiene y reserva la siguiente URL pendiente.

        Devuelve None cuando la cola esta vacia.
        """
        async with self._lock:
            if self._pending.empty():
                return None
            url = self._pending.get_nowait()
            self._claimed.add(url)
            return url

    @property
    def total(self) -> int:
        """Numero total de URLs registradas en la cola."""
        return self._total

    def remaining(self) -> int:
        """Numero de URLs pendientes todavia sin reclamar."""
        return self._pending.qsize()

    def claimed_count(self) -> int:
        """Numero de URLs ya reclamadas (en proceso o completadas)."""
        return len(self._claimed)

    def pending_count(self) -> int:
        """Numero de URLs pendientes todavia en la cola (sin reclamar)."""
        return self._pending.qsize()

    async def release(self, url: str) -> None:
        """Devuelve a la cola una URL reclamada pero no completada.

        Permite que otro worker activo la procese en la misma sesion,
        en lugar de esperar al proximo reinicio del bucle externo.
        Solo devuelve la URL si realmente estaba en el conjunto claimed.
        """
        async with self._lock:
            if url in self._claimed:
                self._claimed.discard(url)
                await self._pending.put(url)
