import asyncio
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from pyscheduler.modifier import Modifier
from pyscheduler.protocols.cleaning import CleaningStrategy
from pyscheduler.protocols.lock import Lock


class Cleaner:
    """Cleans scheduler state."""

    def __init__(
        self, lock: Lock, strategy: CleaningStrategy, modifier: Modifier
    ) -> None:
        self._lock = lock
        self._strategy = strategy
        self._modifier = modifier

    async def _clean(self) -> None:
        async with self._lock:
            await self._modifier.remove_stale_tasks(self._strategy.evaluate)

    async def _loop(self) -> None:
        try:
            async for _ in self._strategy.cycle():
                await self._clean()
        except asyncio.CancelledError:
            pass

    @asynccontextmanager
    async def run(self) -> AsyncGenerator[None, None]:
        """Run in the context."""

        task = asyncio.create_task(self._loop())

        try:
            yield
        finally:
            task.cancel()
            await task
