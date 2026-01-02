from pyscheduler.events import EventCache
from pyscheduler.models import transfer as t
from pyscheduler.modifier import Modifier
from pyscheduler.protocols.lock import Lock
from pyscheduler.time import utcnow


class Canceller:
    """Handles cancelling of tasks."""

    def __init__(self, lock: Lock, cache: EventCache, modifier: Modifier) -> None:
        self._lock = lock
        self._cache = cache
        self._modifier = modifier

    async def cancel(self, request: t.CancelRequest) -> t.CancelledTask:
        """Cancel a task."""
        task_id = request.id

        async with self._lock:
            task = await self._modifier.move_task_to_cancelled(task_id, utcnow())
            cancelled = await self._cache.get(f"cancelled:{task_id}")
            await cancelled.notify()
            finished = await self._cache.get(f"finished:{task_id}")
            await finished.notify()

        return t.CancelledTask(
            task=t.Task(
                id=task_id,
                operation=t.Specification(
                    type=task.task.operation.type,
                    parameters=task.task.operation.parameters,
                ),
                condition=t.Specification(
                    type=task.task.condition.type,
                    parameters=task.task.condition.parameters,
                ),
                dependencies=task.task.dependencies,
            ),
            scheduled=task.scheduled,
            started=task.started,
            cancelled=task.cancelled,
        )
