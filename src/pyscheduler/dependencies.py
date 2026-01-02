import asyncio
from dataclasses import dataclass
from typing import Literal
from uuid import UUID

from pyscheduler.events import EventCache
from pyscheduler.models import enums as e
from pyscheduler.models import types as t
from pyscheduler.models.data import runtime as r
from pyscheduler.models.data import storage as s
from pyscheduler.protocols.lock import Lock
from pyscheduler.protocols.store import Store


@dataclass(kw_only=True)
class CancelledTaskResult:
    """Result of a cancelled task."""

    status: Literal[e.Status.CANCELLED]


@dataclass(kw_only=True)
class FailedTaskResult:
    """Result of a failed task."""

    status: Literal[e.Status.FAILED]
    error: str


@dataclass(kw_only=True)
class CompletedTaskResult:
    """Result of a completed task."""

    status: Literal[e.Status.COMPLETED]
    result: t.JSON


TaskResult = CancelledTaskResult | FailedTaskResult | CompletedTaskResult


class ResultResolver:
    """Resolves results of tasks."""

    def __init__(self, store: Store[s.State], lock: Lock, cache: EventCache) -> None:
        self._store = store
        self._lock = lock
        self._cache = cache

    async def _wait_until_finished(self, task_id: UUID) -> None:
        try:
            finished = await self._cache.get(f"finished:{task_id}")
            await finished.wait()
        except asyncio.CancelledError:
            pass

    async def _get_state(self) -> r.State:
        async with self._lock:
            state = await self._store.get()

        return r.State.deserialize(state)

    def _resolve_from_status(
        self, status: e.Status, task_id: UUID, state: r.State
    ) -> TaskResult | None:
        match status:
            case e.Status.CANCELLED:
                return CancelledTaskResult(status=status)
            case e.Status.FAILED:
                failed = state.tasks.failed[task_id]
                return FailedTaskResult(status=status, error=failed.error)
            case e.Status.COMPLETED:
                completed = state.tasks.completed[task_id]
                return CompletedTaskResult(status=status, result=completed.result)
            case _:
                return None

    async def resolve(self, task_id: UUID) -> TaskResult | None:
        """Resolve the result of a task."""
        wait_until_finished = asyncio.create_task(self._wait_until_finished(task_id))

        try:
            state = await self._get_state()
            status = state.statuses.get(task_id)

            if status is None:
                return None

            if result := self._resolve_from_status(status, task_id, state):
                return result

            await wait_until_finished
        finally:
            wait_until_finished.cancel()
            await wait_until_finished

        state = await self._get_state()
        status = state.statuses.get(task_id)

        if status is None:
            return None

        return self._resolve_from_status(status, task_id, state)
