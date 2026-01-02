from pyscheduler.errors import InvalidCleaningStrategyError
from pyscheduler.models import transfer as t
from pyscheduler.modifier import Modifier
from pyscheduler.protocols.cleaning import CleaningStrategyFactory
from pyscheduler.protocols.lock import Lock


class Cleaner:
    """Cleans scheduler state."""

    def __init__(
        self,
        lock: Lock,
        modifier: Modifier,
        cleaning: CleaningStrategyFactory,
    ) -> None:
        self._lock = lock
        self._modifier = modifier
        self._cleaning = cleaning

    async def clean(self, request: t.CleanRequest) -> t.CleaningResult:
        """Clean tasks."""
        strategy = await self._cleaning.create(request.strategy.type)
        if strategy is None:
            raise InvalidCleaningStrategyError(request.strategy.type)

        async def _predicate(task: t.FinishedTask) -> bool:
            return await strategy.evaluate(task, request.strategy.parameters)

        async with self._lock:
            removed = await self._modifier.remove_stale_tasks(_predicate)

        return t.CleaningResult(removed=removed)
