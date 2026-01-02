from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from uuid import UUID

from pyscheduler.adder import Adder
from pyscheduler.canceller import Canceller
from pyscheduler.cleaner import Cleaner
from pyscheduler.events import EventCache, EventFactory
from pyscheduler.models import transfer as t
from pyscheduler.models.data import storage as s
from pyscheduler.modifier import Modifier
from pyscheduler.protocols.cleaning import CleaningStrategyFactory
from pyscheduler.protocols.condition import ConditionFactory
from pyscheduler.protocols.lock import Lock
from pyscheduler.protocols.operation import OperationFactory
from pyscheduler.protocols.queue import Queue
from pyscheduler.protocols.store import Store
from pyscheduler.readers import Reader
from pyscheduler.runner import Runner


class Scheduler:
    """Scheduler that manages the lifecycle of scheduled tasks."""

    def __init__(  # noqa: PLR0913
        self,
        store: Store[s.State],
        lock: Lock,
        events: EventFactory,
        queue: Queue[UUID],
        operations: OperationFactory,
        conditions: ConditionFactory,
        cleaning: CleaningStrategyFactory,
    ) -> None:
        tasks = Reader(store, lock)
        cache = EventCache(events)
        modifier = Modifier(store)
        runner = Runner(store, lock, cache, queue, modifier, operations, conditions)
        adder = Adder(lock, queue, modifier, operations, conditions)
        canceller = Canceller(lock, cache, modifier)
        cleaner = Cleaner(lock, modifier, cleaning)

        self._tasks = tasks
        self._runner = runner
        self._adder = adder
        self._canceller = canceller
        self._cleaner = cleaner

    @property
    def tasks(self) -> Reader:
        """Reader for tasks."""
        return self._tasks

    async def schedule(self, request: t.ScheduleRequest) -> t.PendingTask:
        """Schedule a task."""
        return await self._adder.add(request)

    async def cancel(self, request: t.CancelRequest) -> t.CancelledTask:
        """Cancel a task."""
        return await self._canceller.cancel(request)

    async def clean(self, request: t.CleanRequest) -> t.CleaningResult:
        """Clean tasks."""
        return await self._cleaner.clean(request)

    @asynccontextmanager
    async def run(self) -> AsyncGenerator[None]:
        """Run in the context."""
        async with self._runner.run():
            yield
