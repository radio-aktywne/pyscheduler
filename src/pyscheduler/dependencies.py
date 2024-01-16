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


@dataclass
class CancelledTaskResult:
    """Result of a cancelled task."""

    status: Literal[e.Status.CANCELLED]


@dataclass
class FailedTaskResult:
    """Result of a failed task."""

    status: Literal[e.Status.FAILED]
    error: str


@dataclass
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

    async def _wait_until_finished(self, id: UUID) -> None:
        try:
            finished = await self._cache.get(f"finished:{id}")
            await finished.wait()
        except asyncio.CancelledError:
            pass

    async def _get_state(self) -> r.State:
        async with self._lock:
            state = await self._store.get()

        return r.State.deserialize(state)

    async def resolve(self, id: UUID) -> TaskResult | None:
        """Resolve the result of a task."""

        try:
            wait_until_finished = asyncio.create_task(self._wait_until_finished(id))

            state = await self._get_state()
            status = state.statuses.get(id)

            if status is None:
                return None

            if status == e.Status.CANCELLED:
                return CancelledTaskResult(status=status)

            if status == e.Status.FAILED:
                failed = state.tasks.failed[id]
                return FailedTaskResult(status=status, error=failed.error)

            if status == e.Status.COMPLETED:
                completed = state.tasks.completed[id]
                return CompletedTaskResult(status=status, result=completed.result)

            await wait_until_finished
        finally:
            wait_until_finished.cancel()
            await wait_until_finished

        state = await self._get_state()
        status = state.statuses.get(id)

        if status is None:
            return None

        if status == e.Status.CANCELLED:
            return CancelledTaskResult(status=status)

        if status == e.Status.FAILED:
            failed = state.tasks.failed[id]
            return FailedTaskResult(status=status, error=failed.error)

        if status == e.Status.COMPLETED:
            completed = state.tasks.completed[id]
            return CompletedTaskResult(status=status, result=completed.result)

        return None
