from abc import abstractmethod
from datetime import datetime
from typing import Protocol
from uuid import UUID

from pyscheduler.errors import (
    DependencyNotFoundError,
    TaskNotFoundError,
    TaskStatusError,
)
from pyscheduler.models import enums as e, transfer as t, types
from pyscheduler.models.data import runtime as r, storage as s
from pyscheduler.protocols.store import Store


class RemovePredicate(Protocol):
    """Predicate for checking if a task can be removed."""

    @abstractmethod
    async def __call__(self, task: t.FinishedTask) -> bool:
        """Check if a task can be removed."""


class Modifier:
    """Utility for common state modifications."""

    def __init__(self, store: Store[s.State]) -> None:
        self._store = store

    async def _get_state(self) -> r.State:
        state = await self._store.get()
        return r.State.deserialize(state)

    async def _save_state(self, state: r.State) -> None:
        serialized_state = state.serialize()
        await self._store.set(serialized_state)

    async def add_queued_task(
        self, task_id: UUID, task: r.Task, enqueued: datetime
    ) -> r.QueuedTask:
        """Add a queued task to the state."""
        state = await self._get_state()

        for dependency in task.dependencies.values():
            if dependency not in state.statuses:
                raise DependencyNotFoundError(dependency)

        queued_task = r.QueuedTask(task=task, enqueued=enqueued)
        state.tasks.queued[task_id] = queued_task
        state.statuses[task_id] = e.Status.QUEUED

        dependencies = set(queued_task.task.dependencies.values())

        if dependencies:
            state.relationships.dependencies[task_id] = dependencies

        for dependency in dependencies:
            if dependency not in state.relationships.dependents:
                state.relationships.dependents[dependency] = set()

            state.relationships.dependents[dependency].add(task_id)

        await self._save_state(state)

        return queued_task

    async def move_task_to_waiting(
        self, task_id: UUID, dequeued: datetime
    ) -> r.WaitingTask:
        """Move a task to the waiting state."""
        state = await self._get_state()
        status = state.statuses.get(task_id)

        if status is None:
            raise TaskNotFoundError(task_id)

        match status:
            case e.Status.QUEUED:
                task = state.tasks.queued.pop(task_id, None)
            case _:
                raise TaskStatusError(task_id, status)

        if task is None:
            raise TaskNotFoundError(task_id)

        task = r.WaitingTask(task=task.task, enqueued=task.enqueued, dequeued=dequeued)
        state.tasks.waiting[task_id] = task
        state.statuses[task_id] = e.Status.WAITING

        await self._save_state(state)

        return task

    async def move_task_to_sleeping(
        self, task_id: UUID, slept: datetime
    ) -> r.SleepingTask:
        """Move a task to the sleeping state."""
        state = await self._get_state()
        status = state.statuses.get(task_id)

        if status is None:
            raise TaskNotFoundError(task_id)

        match status:
            case e.Status.QUEUED:
                task = state.tasks.queued.pop(task_id, None)
                dequeued = None
            case e.Status.WAITING:
                task = state.tasks.waiting.pop(task_id, None)
                dequeued = task.dequeued if task is not None else None
            case _:
                raise TaskStatusError(task_id, status)

        if task is None:
            raise TaskNotFoundError(task_id)

        task = r.SleepingTask(
            task=task.task, enqueued=task.enqueued, dequeued=dequeued, slept=slept
        )
        state.tasks.sleeping[task_id] = task
        state.statuses[task_id] = e.Status.SLEEPING

        await self._save_state(state)

        return task

    async def move_task_to_queued(
        self, task_id: UUID, enqueued: datetime
    ) -> r.QueuedTask:
        """Move a task to the queued state."""
        state = await self._get_state()
        status = state.statuses.get(task_id)

        if status is None:
            raise TaskNotFoundError(task_id)

        match status:
            case e.Status.SLEEPING:
                task = state.tasks.sleeping.pop(task_id, None)
            case _:
                raise TaskStatusError(task_id, status)

        if task is None:
            raise TaskNotFoundError(task_id)

        task = r.QueuedTask(task=task.task, enqueued=enqueued)
        state.tasks.queued[task_id] = task
        state.statuses[task_id] = e.Status.QUEUED

        await self._save_state(state)

        return task

    async def move_task_to_running(
        self, task_id: UUID, started: datetime
    ) -> r.RunningTask:
        """Move a task to the running state."""
        state = await self._get_state()
        status = state.statuses.get(task_id)

        if status is None:
            raise TaskNotFoundError(task_id)

        match status:
            case e.Status.WAITING:
                task = state.tasks.waiting.pop(task_id, None)
            case _:
                raise TaskStatusError(task_id, status)

        if task is None:
            raise TaskNotFoundError(task_id)

        task = r.RunningTask(
            task=task.task,
            enqueued=task.enqueued,
            dequeued=task.dequeued,
            started=started,
        )
        state.tasks.running[task_id] = task
        state.statuses[task_id] = e.Status.RUNNING

        await self._save_state(state)

        return task

    async def move_task_to_cancelled(
        self, task_id: UUID, cancelled: datetime
    ) -> r.CancelledTask:
        """Move a task to the cancelled state."""
        state = await self._get_state()
        status = state.statuses.get(task_id)

        if status is None:
            raise TaskNotFoundError(task_id)

        match status:
            case e.Status.WAITING:
                task = state.tasks.waiting.pop(task_id, None)
                started = None
            case e.Status.RUNNING:
                task = state.tasks.running.pop(task_id, None)
                started = task.started if task is not None else None
            case _:
                raise TaskStatusError(task_id, status)

        if task is None:
            raise TaskNotFoundError(task_id)

        task = r.CancelledTask(
            task=task.task,
            enqueued=task.enqueued,
            dequeued=task.dequeued,
            started=started,
            cancelled=cancelled,
        )
        state.tasks.cancelled[task_id] = task
        state.statuses[task_id] = e.Status.CANCELLED

        await self._save_state(state)

        return task

    async def move_task_to_failed(
        self, task_id: UUID, failed: datetime, error: str
    ) -> r.FailedTask:
        """Move a task to the failed state."""
        state = await self._get_state()
        status = state.statuses.get(task_id)

        if status is None:
            raise TaskNotFoundError(task_id)

        match status:
            case e.Status.WAITING:
                task = state.tasks.waiting.pop(task_id, None)
                started = None
            case e.Status.RUNNING:
                task = state.tasks.running.pop(task_id, None)
                started = task.started if task is not None else None
            case _:
                raise TaskStatusError(task_id, status)

        if task is None:
            raise TaskNotFoundError(task_id)

        task = r.FailedTask(
            task=task.task,
            enqueued=task.enqueued,
            dequeued=task.dequeued,
            started=started,
            failed=failed,
            error=error,
        )
        state.tasks.failed[task_id] = task
        state.statuses[task_id] = e.Status.FAILED

        await self._save_state(state)

        return task

    async def move_task_to_completed(
        self, task_id: UUID, completed: datetime, result: types.JSON
    ) -> r.CompletedTask:
        """Move a task to the completed state."""
        state = await self._get_state()
        status = state.statuses.get(task_id)

        if status is None:
            raise TaskNotFoundError(task_id)

        match status:
            case e.Status.RUNNING:
                task = state.tasks.running.pop(task_id, None)
            case _:
                raise TaskStatusError(task_id, status)

        if task is None:
            raise TaskNotFoundError(task_id)

        task = r.CompletedTask(
            task=task.task,
            enqueued=task.enqueued,
            dequeued=task.dequeued,
            started=task.started,
            completed=completed,
            result=result,
        )
        state.tasks.completed[task_id] = task
        state.statuses[task_id] = e.Status.COMPLETED

        await self._save_state(state)

        return task

    def _build_finished_task(self, task_id: UUID, state: r.State) -> t.FinishedTask:
        status = state.statuses[task_id]

        match status:
            case e.Status.CANCELLED:
                task = state.tasks.cancelled[task_id]
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
                    enqueued=task.enqueued,
                    dequeued=task.dequeued,
                    started=task.started,
                    cancelled=task.cancelled,
                )
            case e.Status.FAILED:
                task = state.tasks.failed[task_id]
                return t.FailedTask(
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
                    enqueued=task.enqueued,
                    dequeued=task.dequeued,
                    started=task.started,
                    failed=task.failed,
                    error=task.error,
                )
            case e.Status.COMPLETED:
                task = state.tasks.completed[task_id]
                return t.CompletedTask(
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
                    enqueued=task.enqueued,
                    dequeued=task.dequeued,
                    started=task.started,
                    completed=task.completed,
                    result=task.result,
                )
            case _:
                raise TaskStatusError(task_id, status)

    async def remove_stale_tasks(  # noqa: C901
        self, predicate: RemovePredicate | None = None
    ) -> set[UUID]:
        """Remove finished tasks that are no longer needed."""

        async def _default_predicate(task: t.FinishedTask) -> bool:
            return True

        predicate = predicate or _default_predicate

        state = await self._get_state()

        pool = {
            task_id
            for task_id, status in state.statuses.items()
            if status in (e.Status.CANCELLED, e.Status.FAILED, e.Status.COMPLETED)
        }

        removed = set[UUID]()

        size = 0

        while len(pool) != size:
            size = len(pool)

            for task_id in pool.copy():
                if task_id in state.relationships.dependents:
                    continue

                if not await predicate(self._build_finished_task(task_id, state)):
                    continue

                status = state.statuses[task_id]

                match status:
                    case e.Status.CANCELLED:
                        state.tasks.cancelled.pop(task_id)
                    case e.Status.FAILED:
                        state.tasks.failed.pop(task_id)
                    case e.Status.COMPLETED:
                        state.tasks.completed.pop(task_id)

                state.statuses.pop(task_id)

                dependencies = state.relationships.dependencies.pop(task_id, set())
                for dependency in dependencies:
                    if dependency in state.relationships.dependents:
                        state.relationships.dependents[dependency].remove(task_id)
                        if not state.relationships.dependents[dependency]:
                            state.relationships.dependents.pop(dependency)

                pool.remove(task_id)
                removed.add(task_id)

        await self._save_state(state)

        return removed
