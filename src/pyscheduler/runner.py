import asyncio
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager, suppress
from uuid import UUID

from pyscheduler.dependencies import ResultResolver
from pyscheduler.errors import (
    DependencyNotFoundError,
    InvalidConditionError,
    InvalidOperationError,
    SchedulerError,
    TaskNotFoundError,
    UnexpectedTaskStatusError,
    UnsuccessfulDependencyError,
)
from pyscheduler.events import EventCache
from pyscheduler.models import enums as e, types
from pyscheduler.models.data import runtime as r, storage as s
from pyscheduler.modifier import Modifier
from pyscheduler.protocols.condition import Condition, ConditionFactory
from pyscheduler.protocols.event import Event
from pyscheduler.protocols.lock import Lock
from pyscheduler.protocols.operation import Operation, OperationFactory
from pyscheduler.protocols.queue import Queue
from pyscheduler.protocols.store import Store
from pyscheduler.time import awareutcnow


class Runner:
    """Manages lifecycle of scheduled tasks."""

    def __init__(
        self,
        store: Store[s.State],
        lock: Lock,
        cache: EventCache,
        queue: Queue[UUID],
        modifier: Modifier,
        operations: OperationFactory,
        conditions: ConditionFactory,
    ) -> None:
        self._store = store
        self._lock = lock
        self._cache = cache
        self._queue = queue
        self._modifier = modifier
        self._operations = operations
        self._conditions = conditions
        self._resolver = ResultResolver(store, lock, cache)

    @asynccontextmanager
    async def _manage_finished_event(self, task_id: UUID) -> AsyncGenerator[Event]:
        finished = await self._cache.get(f"finished:{task_id}")

        try:
            yield finished
        finally:
            await self._cache.delete(f"finished:{task_id}")

    @asynccontextmanager
    async def _sleep_on_interruptions(self, task_id: UUID) -> AsyncGenerator[None]:
        try:
            yield
        except asyncio.CancelledError:
            with suppress(SchedulerError):
                await self._set_task_as_sleeping(task_id)
            raise

    @asynccontextmanager
    async def _fail_on_interruptions(
        self, task_id: UUID, error: str, finished: Event
    ) -> AsyncGenerator[None]:
        try:
            yield
        except asyncio.CancelledError:
            with suppress(SchedulerError):
                await self._set_task_as_failed(task_id, error, finished)
            raise

    @asynccontextmanager
    async def _complete_on_interruptions(
        self, task_id: UUID, result: types.JSON, finished: Event
    ) -> AsyncGenerator[None]:
        try:
            yield
        except asyncio.CancelledError:
            with suppress(SchedulerError):
                await self._set_task_as_completed(task_id, result, finished)
            raise

    async def _get_task(self, task_id: UUID) -> r.Task:
        async with self._lock:
            state = await self._store.get()

        state = r.State.deserialize(state)
        status = state.statuses.get(task_id)

        if status is None:
            raise TaskNotFoundError(task_id)

        if status != e.Status.QUEUED:
            raise UnexpectedTaskStatusError(task_id, status)

        task = state.tasks.queued.get(task_id)

        if task is None:
            raise TaskNotFoundError(task_id)

        return task.task

    async def _create_operation(self, operation_type: str) -> Operation:
        operation = await self._operations.create(operation_type)

        if operation is None:
            raise InvalidOperationError(operation_type)

        return operation

    async def _create_condition(self, condition_type: str) -> Condition:
        condition = await self._conditions.create(condition_type)

        if condition is None:
            raise InvalidConditionError(condition_type)

        return condition

    async def _resolve_dependencies(
        self, dependencies: dict[str, UUID]
    ) -> dict[str, types.JSON]:
        deps = {}

        for parameter, dependency in dependencies.items():
            result = await self._resolver.resolve(dependency)

            if result is None:
                raise DependencyNotFoundError(dependency)

            match result.status:
                case e.Status.CANCELLED | e.Status.FAILED:
                    raise UnsuccessfulDependencyError(dependency, result.status)
                case _:
                    deps[parameter] = result.result

        return deps

    async def _set_task_as_waiting(self, task_id: UUID) -> None:
        async with self._lock:
            await self._modifier.move_task_to_waiting(task_id, awareutcnow())

    async def _set_task_as_sleeping(self, task_id: UUID) -> None:
        async with self._lock:
            await self._modifier.move_task_to_sleeping(task_id, awareutcnow())

    async def _set_task_as_running(self, task_id: UUID) -> None:
        async with self._lock:
            await self._modifier.move_task_to_running(task_id, awareutcnow())

    async def _set_task_as_failed(
        self, task_id: UUID, error: str, finished: Event
    ) -> None:
        async with self._lock:
            await self._modifier.move_task_to_failed(task_id, awareutcnow(), error)
            await finished.notify()

    async def _set_task_as_completed(
        self, task_id: UUID, result: types.JSON, finished: Event
    ) -> None:
        async with self._lock:
            await self._modifier.move_task_to_completed(task_id, awareutcnow(), result)
            await finished.notify()

    async def _monitor_cancellation(self, task_id: UUID) -> None:
        try:
            cancelled = await self._cache.get(f"cancelled:{task_id}")
            await cancelled.wait()
        except asyncio.CancelledError:
            pass

    async def _run_task(self, task_id: UUID) -> None:
        try:
            async with self._manage_finished_event(task_id) as finished:
                async with self._sleep_on_interruptions(task_id):
                    task = await self._get_task(task_id)
                    await self._set_task_as_waiting(task_id)

                    try:
                        operation = await self._create_operation(task.operation.type)
                    except InvalidOperationError as ex:
                        await self._set_task_as_failed(
                            task_id, f"Operation {ex.type} is not supported.", finished
                        )
                        return

                    try:
                        condition = await self._create_condition(task.condition.type)
                    except InvalidConditionError as ex:
                        await self._set_task_as_failed(
                            task_id, f"Condition {ex.type} is not supported.", finished
                        )
                        return

                    try:
                        dependencies = task.dependencies
                        dependencies = await self._resolve_dependencies(dependencies)
                    except UnsuccessfulDependencyError as ex:
                        await self._set_task_as_failed(
                            task_id,
                            f"Dependency {ex.id} finished with status {ex.status}.",
                            finished,
                        )
                        return

                    try:
                        await condition.wait(task.condition.parameters)
                    except asyncio.CancelledError:
                        raise
                    except Exception as ex:  # noqa: BLE001
                        await self._set_task_as_failed(
                            task_id,
                            f"Condition {task.condition.type} failed: {ex}.",
                            finished,
                        )
                        return

                async with self._fail_on_interruptions(
                    task_id,
                    f"Operation {task.operation.type} was interrupted.",
                    finished,
                ):
                    await self._set_task_as_running(task_id)

                    try:
                        parameters = task.operation.parameters
                        result = await operation.run(parameters, dependencies)
                    except asyncio.CancelledError:
                        raise
                    except Exception as ex:  # noqa: BLE001
                        await self._set_task_as_failed(
                            task_id,
                            f"Operation {task.operation.type} failed: {ex}.",
                            finished,
                        )
                        return

                async with self._complete_on_interruptions(task_id, result, finished):
                    await self._set_task_as_completed(task_id, result, finished)
        except asyncio.CancelledError:
            pass

    async def _handle_task_dequeued(self, task_id: UUID) -> None:
        run = asyncio.create_task(self._run_task(task_id))
        monitor = asyncio.create_task(self._monitor_cancellation(task_id))

        try:
            await asyncio.wait([run, monitor], return_when=asyncio.FIRST_COMPLETED)
        except asyncio.CancelledError:
            pass
        finally:
            if not run.done():
                run.cancel()

            if not monitor.done():
                monitor.cancel()

            await asyncio.wait([run, monitor])

    async def _process_queue(self) -> None:
        handlers = []

        try:
            while True:
                task_id = await self._queue.get()
                handler = asyncio.create_task(self._handle_task_dequeued(task_id))
                handlers.append(handler)
        except asyncio.CancelledError:
            pass
        finally:
            if handlers:
                for handler in handlers:
                    handler.cancel()

                await asyncio.wait(handlers)

    @asynccontextmanager
    async def run(self) -> AsyncGenerator[None]:
        """Run in the context."""
        task = asyncio.create_task(self._process_queue())

        try:
            yield
        finally:
            task.cancel()
            await task
