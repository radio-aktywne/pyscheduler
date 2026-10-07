import asyncio
from uuid import UUID

from pyscheduler.models.data import runtime as r, storage as s
from pyscheduler.modifier import Modifier
from pyscheduler.protocols.lock import Lock
from pyscheduler.protocols.queue import Queue
from pyscheduler.protocols.store import Store
from pyscheduler.time import awareutcnow


class Recoverer:
    """Handles recovery of sleeping tasks."""

    def __init__(
        self, store: Store[s.State], lock: Lock, queue: Queue[UUID], modifier: Modifier
    ) -> None:
        self._store = store
        self._lock = lock
        self._queue = queue
        self._modifier = modifier

    async def recover(self) -> None:
        """Recover tasks."""
        while True:
            async with self._lock:
                state = await self._store.get()
                state = r.State.deserialize(state)

                if not state.tasks.sleeping:
                    break

                task_id = next(iter(state.tasks.sleeping.keys()))
                await self._modifier.move_task_to_queued(task_id, awareutcnow())

                try:
                    await self._queue.put(task_id)
                except asyncio.CancelledError, Exception:
                    await self._modifier.move_task_to_sleeping(task_id, awareutcnow())
                    raise
