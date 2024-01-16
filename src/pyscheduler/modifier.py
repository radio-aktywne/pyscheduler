from abc import abstractmethod
from datetime import datetime
from typing import Protocol
from uuid import UUID

from pyscheduler.errors import (
    DependencyNotFoundError,
    TaskNotFoundError,
    TaskStatusError,
)
from pyscheduler.models import enums as e
from pyscheduler.models import transfer as t
from pyscheduler.models import types
from pyscheduler.models.data import runtime as r
from pyscheduler.models.data import storage as s
from pyscheduler.protocols.store import Store


class RemovePredicate(Protocol):
    @abstractmethod
    async def __call__(self, task: t.FinishedTask) -> bool:
        pass


class Modifier:
    """Utility for common state modifications."""

    def __init__(self, store: Store[s.State]) -> None:
        self._store = store

    async def _get_state(self) -> r.State:
        state = await self._store.get()
        return r.State.deserialize(state)

    async def _save_state(self, state: r.State) -> None:
        state = state.serialize()
        await self._store.set(state)

    async def add_pending_task(
        self, id: UUID, task: r.Task, scheduled: datetime
    ) -> r.PendingTask:
        """Add a task to the state."""

        state = await self._get_state()

        for dependency in task.dependencies.values():
            if dependency not in state.statuses:
                raise DependencyNotFoundError(id)

        task = r.PendingTask(task=task, scheduled=scheduled)
        state.tasks.pending[id] = task
        state.statuses[id] = e.Status.PENDING

        dependencies = set(task.task.dependencies.values())

        if dependencies:
            state.relationships.dependencies[id] = dependencies

        for dependency in dependencies:
            if dependency not in state.relationships.dependents:
                state.relationships.dependents[dependency] = set()

            state.relationships.dependents[dependency].add(id)

        await self._save_state(state)

        return task

    async def move_task_to_running(self, id: UUID, started: datetime) -> r.RunningTask:
        """Move a task to the running state."""

        state = await self._get_state()
        status = state.statuses.get(id)

        if status is None:
            raise TaskNotFoundError(id)

        if status != e.Status.PENDING:
            raise TaskStatusError(id, status)

        task = state.tasks.pending.pop(id, None)

        if task is None:
            raise TaskNotFoundError(id)

        task = r.RunningTask(task=task.task, scheduled=task.scheduled, started=started)
        state.tasks.running[id] = task
        state.statuses[id] = e.Status.RUNNING

        await self._save_state(state)

        return task

    async def move_task_to_cancelled(
        self, id: UUID, cancelled: datetime
    ) -> r.CancelledTask:
        """Move a task to the cancelled state."""

        state = await self._get_state()
        status = state.statuses.get(id)

        if status is None:
            raise TaskNotFoundError(id)

        match status:
            case e.Status.PENDING:
                task = state.tasks.pending.pop(id, None)
                started = None
            case e.Status.RUNNING:
                task = state.tasks.running.pop(id, None)
                started = task.started
            case _:
                raise TaskStatusError(id, status)

        if task is None:
            raise TaskNotFoundError(id)

        task = r.CancelledTask(
            task=task.task,
            scheduled=task.scheduled,
            started=started,
            cancelled=cancelled,
        )
        state.tasks.cancelled[id] = task
        state.statuses[id] = e.Status.CANCELLED

        await self._save_state(state)

        return task

    async def move_task_to_failed(
        self, id: UUID, failed: datetime, error: str
    ) -> r.FailedTask:
        """Move a task to the failed state."""

        state = await self._get_state()
        status = state.statuses.get(id)

        if status is None:
            raise TaskNotFoundError(id)

        if status != e.Status.RUNNING:
            raise TaskStatusError(id, status)

        task = state.tasks.running.pop(id, None)

        if task is None:
            raise TaskNotFoundError(id)

        task = r.FailedTask(
            task=task.task,
            scheduled=task.scheduled,
            started=task.started,
            failed=failed,
            error=error,
        )
        state.tasks.failed[id] = task
        state.statuses[id] = e.Status.FAILED

        await self._save_state(state)

        return task

    async def move_task_to_completed(
        self, id: UUID, completed: datetime, result: types.JSON
    ) -> r.CompletedTask:
        """Move a task to the completed state."""

        state = await self._get_state()
        status = state.statuses.get(id)

        if status is None:
            raise TaskNotFoundError(id)

        if status != e.Status.RUNNING:
            raise TaskStatusError(id, status)

        task = state.tasks.running.pop(id, None)

        if task is None:
            raise TaskNotFoundError(id)

        task = r.CompletedTask(
            task=task.task,
            scheduled=task.scheduled,
            started=task.started,
            completed=completed,
            result=result,
        )
        state.tasks.completed[id] = task
        state.statuses[id] = e.Status.COMPLETED

        await self._save_state(state)

        return task

    def _build_finished_task(self, id: UUID, state: r.State) -> t.FinishedTask:
        status = state.statuses[id]

        match status:
            case e.Status.CANCELLED:
                task = state.tasks.cancelled[id]
                return t.CancelledTask(
                    task=t.Task(
                        id=id,
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
            case e.Status.FAILED:
                task = state.tasks.failed[id]
                return t.FailedTask(
                    task=t.Task(
                        id=id,
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
                    failed=task.failed,
                    error=task.error,
                )
            case e.Status.COMPLETED:
                task = state.tasks.completed[id]
                return t.CompletedTask(
                    task=t.Task(
                        id=id,
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
                    completed=task.completed,
                    result=task.result,
                )
            case _:
                raise TaskStatusError(id, status)

    async def remove_stale_tasks(
        self,
        predicate: RemovePredicate | None = None,
    ) -> set[UUID]:
        """Remove finished tasks that are no longer needed."""

        async def _default_predicate(task: t.FinishedTask) -> bool:
            return True

        predicate = predicate or _default_predicate

        state = await self._get_state()

        pool = {
            id
            for id, status in state.statuses.items()
            if status in (e.Status.CANCELLED, e.Status.FAILED, e.Status.COMPLETED)
        }

        removed = set[UUID]()

        size = 0

        while len(pool) != size:
            size = len(pool)

            for id in pool.copy():
                if id in state.relationships.dependents:
                    continue

                if not await predicate(self._build_finished_task(id, state)):
                    continue

                status = state.statuses[id]

                match status:
                    case e.Status.CANCELLED:
                        state.tasks.cancelled.pop(id)
                    case e.Status.FAILED:
                        state.tasks.failed.pop(id)
                    case e.Status.COMPLETED:
                        state.tasks.completed.pop(id)

                state.statuses.pop(id)

                dependencies = state.relationships.dependencies.pop(id, set())
                for dependency in dependencies:
                    if dependency in state.relationships.dependents:
                        state.relationships.dependents[dependency].remove(id)
                        if not state.relationships.dependents[dependency]:
                            state.relationships.dependents.pop(dependency)

                pool.remove(id)
                removed.add(id)

        await self._save_state(state)

        return removed
