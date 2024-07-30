import asyncio
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from uuid import UUID, uuid4

from pyscheduler.dependencies import ResultResolver
from pyscheduler.errors import (
    DependencyNotFoundError,
    InvalidConditionError,
    InvalidOperationError,
    TaskNotFoundError,
    UnexpectedTaskStatusError,
    UnsuccessfulDependencyError,
)
from pyscheduler.events import EventCache
from pyscheduler.models import enums as e
from pyscheduler.models import types
from pyscheduler.models.data import runtime as r
from pyscheduler.models.data import storage as s
from pyscheduler.modifier import Modifier
from pyscheduler.protocols.condition import Condition, ConditionFactory
from pyscheduler.protocols.event import Event
from pyscheduler.protocols.lock import Lock
from pyscheduler.protocols.operation import Operation, OperationFactory
from pyscheduler.protocols.queue import Queue
from pyscheduler.protocols.store import Store
from pyscheduler.time import utcnow


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
    async def _manage_finished_event(self, id: uuid4) -> AsyncGenerator[Event]:
        finished = await self._cache.get(f"finished:{id}")

        try:
            yield finished
        finally:
            await self._cache.delete(f"finished:{id}")

    async def _get_task(self, id: UUID) -> r.Task:
        async with self._lock:
            state = await self._store.get()

        state = r.State.deserialize(state)
        status = state.statuses.get(id)

        if status is None:
            raise TaskNotFoundError(id)

        if status != e.Status.PENDING:
            raise UnexpectedTaskStatusError(id, status)

        task = state.tasks.pending.get(id)

        if task is None:
            raise TaskNotFoundError(id)

        return task.task

    async def _create_operation(self, type: str) -> Operation:
        operation = await self._operations.create(type)

        if operation is None:
            raise InvalidOperationError(type)

        return operation

    async def _create_condition(self, type: str) -> Condition:
        condition = await self._conditions.create(type)

        if condition is None:
            raise InvalidConditionError(type)

        return condition

    async def _resolve_dependencies(
        self, dependencies: dict[str, UUID]
    ) -> dict[str, types.JSON]:
        deps = {}

        for parameter, dependency in dependencies.items():
            result = await self._resolver.resolve(dependency)

            if result is None:
                raise DependencyNotFoundError(dependency)

            if result.status in (e.Status.CANCELLED, e.Status.FAILED):
                raise UnsuccessfulDependencyError(dependency, result.status)

            deps[parameter] = result.result

        return deps

    async def _set_task_as_running(self, id: UUID) -> None:
        async with self._lock:
            await self._modifier.move_task_to_running(id, utcnow())

    async def _set_task_as_failed(self, id: UUID, error: str, finished: Event) -> None:
        async with self._lock:
            await self._modifier.move_task_to_failed(id, utcnow(), error)
            await finished.notify()

    async def _set_task_as_completed(
        self, id: UUID, result: types.JSON, finished: Event
    ) -> None:
        async with self._lock:
            await self._modifier.move_task_to_completed(id, utcnow(), result)
            await finished.notify()

    async def _monitor_cancellation(self, id: UUID) -> None:
        try:
            cancelled = await self._cache.get(f"cancelled:{id}")
            await cancelled.wait()
        except asyncio.CancelledError:
            pass

    async def _run_task(self, id: UUID) -> None:
        try:
            task = await self._get_task(id)

            async with self._manage_finished_event(id) as finished:
                try:
                    operation = await self._create_operation(task.operation.type)
                except InvalidOperationError as ex:
                    error = f"Operation {ex.type} is not supported."
                    await self._set_task_as_failed(id, error, finished)
                    return

                try:
                    condition = await self._create_condition(task.condition.type)
                except InvalidConditionError as ex:
                    error = f"Condition {ex.type} is not supported."
                    await self._set_task_as_failed(id, error, finished)
                    return

                try:
                    dependencies = await self._resolve_dependencies(task.dependencies)
                except UnsuccessfulDependencyError as ex:
                    error = f"Dependency {ex.id} finished with status {ex.status}."
                    await self._set_task_as_failed(id, error, finished)
                    return

                try:
                    await condition.wait(task.condition.parameters)
                except asyncio.CancelledError:
                    raise
                except Exception as ex:
                    error = f"Condition {task.condition.type} failed: {ex}."
                    await self._set_task_as_failed(id, error, finished)
                    return

                await self._set_task_as_running(id)

                try:
                    parameters = task.operation.parameters
                    result = await operation.run(parameters, dependencies)
                except asyncio.CancelledError:
                    raise
                except Exception as ex:
                    error = f"Operation {task.operation.type} failed: {ex}."
                    await self._set_task_as_failed(id, error, finished)
                    return

                await self._set_task_as_completed(id, result, finished)
        except asyncio.CancelledError:
            pass

    async def _handle_task_added(self, id: UUID) -> None:
        run = asyncio.create_task(self._run_task(id))
        monitor = asyncio.create_task(self._monitor_cancellation(id))

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
                id = await self._queue.get()
                handler = asyncio.create_task(self._handle_task_added(id))
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
