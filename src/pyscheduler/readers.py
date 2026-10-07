from uuid import UUID

from pyscheduler.models import enums as e, transfer as t
from pyscheduler.models.data import runtime as r, storage as s
from pyscheduler.protocols.lock import Lock
from pyscheduler.protocols.store import Store


class BaseReader:
    """Base class for readers."""

    def __init__(self, store: Store[s.State], lock: Lock) -> None:
        self._store = store
        self._lock = lock

    async def _get_state(self) -> r.State:
        """Get the current state."""
        async with self._lock:
            state = await self._store.get()

        return r.State.deserialize(state)


class QueuedTasksReader(BaseReader):
    """Reader for queued tasks."""

    async def get(self, task_id: UUID) -> t.QueuedTask | None:
        """Get a queued task by id."""
        state = await self._get_state()
        task = state.tasks.queued.get(task_id)

        if task is None:
            return None

        return t.QueuedTask(
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
        )


class WaitingTasksReader(BaseReader):
    """Reader for waiting tasks."""

    async def get(self, task_id: UUID) -> t.WaitingTask | None:
        """Get a waiting task by id."""
        state = await self._get_state()
        task = state.tasks.waiting.get(task_id)

        if task is None:
            return None

        return t.WaitingTask(
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
        )


class SleepingTasksReader(BaseReader):
    """Reader for sleeping tasks."""

    async def get(self, task_id: UUID) -> t.SleepingTask | None:
        """Get a sleeping task by id."""
        state = await self._get_state()
        task = state.tasks.sleeping.get(task_id)

        if task is None:
            return None

        return t.SleepingTask(
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
            slept=task.slept,
        )


class RunningTasksReader(BaseReader):
    """Reader for running tasks."""

    async def get(self, task_id: UUID) -> t.RunningTask | None:
        """Get a running task by id."""
        state = await self._get_state()
        task = state.tasks.running.get(task_id)

        if task is None:
            return None

        return t.RunningTask(
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
        )


class CancelledTasksReader(BaseReader):
    """Reader for cancelled tasks."""

    async def get(self, task_id: UUID) -> t.CancelledTask | None:
        """Get a cancelled task by id."""
        state = await self._get_state()
        task = state.tasks.cancelled.get(task_id)

        if task is None:
            return None

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


class FailedTasksReader(BaseReader):
    """Reader for failed tasks."""

    async def get(self, task_id: UUID) -> t.FailedTask | None:
        """Get a failed task by id."""
        state = await self._get_state()
        task = state.tasks.failed.get(task_id)

        if task is None:
            return None

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


class CompletedTasksReader(BaseReader):
    """Reader for completed tasks."""

    async def get(self, task_id: UUID) -> t.CompletedTask | None:
        """Get a completed task by id."""
        state = await self._get_state()
        task = state.tasks.completed.get(task_id)

        if task is None:
            return None

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


class Reader(BaseReader):
    """Reader for tasks."""

    def __init__(self, store: Store[s.State], lock: Lock) -> None:
        super().__init__(store, lock)
        self._queued = QueuedTasksReader(store, lock)
        self._waiting = WaitingTasksReader(store, lock)
        self._sleeping = SleepingTasksReader(store, lock)
        self._running = RunningTasksReader(store, lock)
        self._cancelled = CancelledTasksReader(store, lock)
        self._failed = FailedTasksReader(store, lock)
        self._completed = CompletedTasksReader(store, lock)

    @property
    def queued(self) -> QueuedTasksReader:
        """Reader for queued tasks."""
        return self._queued

    @property
    def waiting(self) -> WaitingTasksReader:
        """Reader for waiting tasks."""
        return self._waiting

    @property
    def sleeping(self) -> SleepingTasksReader:
        """Reader for sleeping tasks."""
        return self._sleeping

    @property
    def running(self) -> RunningTasksReader:
        """Reader for running tasks."""
        return self._running

    @property
    def cancelled(self) -> CancelledTasksReader:
        """Reader for cancelled tasks."""
        return self._cancelled

    @property
    def failed(self) -> FailedTasksReader:
        """Reader for failed tasks."""
        return self._failed

    @property
    def completed(self) -> CompletedTasksReader:
        """Reader for completed tasks."""
        return self._completed

    async def list(self) -> t.TaskIndex:
        """List all tasks by status."""
        state = await self._get_state()

        return t.TaskIndex(
            queued=set(state.tasks.queued.keys()),
            waiting=set(state.tasks.waiting.keys()),
            sleeping=set(state.tasks.sleeping.keys()),
            running=set(state.tasks.running.keys()),
            cancelled=set(state.tasks.cancelled.keys()),
            failed=set(state.tasks.failed.keys()),
            completed=set(state.tasks.completed.keys()),
        )

    async def get(self, task_id: UUID) -> t.GenericTask | None:  # noqa: PLR0911
        """Get a task by id."""
        state = await self._get_state()
        status = state.statuses.get(task_id)

        match status:
            case e.Status.QUEUED:
                task = await self._queued.get(task_id)
                return t.GenericTask(task=task.task, status=status) if task else None
            case e.Status.WAITING:
                task = await self._waiting.get(task_id)
                return t.GenericTask(task=task.task, status=status) if task else None
            case e.Status.SLEEPING:
                task = await self._sleeping.get(task_id)
                return t.GenericTask(task=task.task, status=status) if task else None
            case e.Status.RUNNING:
                task = await self._running.get(task_id)
                return t.GenericTask(task=task.task, status=status) if task else None
            case e.Status.CANCELLED:
                task = await self._cancelled.get(task_id)
                return t.GenericTask(task=task.task, status=status) if task else None
            case e.Status.FAILED:
                task = await self._failed.get(task_id)
                return t.GenericTask(task=task.task, status=status) if task else None
            case e.Status.COMPLETED:
                task = await self._completed.get(task_id)
                return t.GenericTask(task=task.task, status=status) if task else None
            case _:
                return None
