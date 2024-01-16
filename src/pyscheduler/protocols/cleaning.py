from abc import abstractmethod
from typing import Protocol

from pyscheduler.models import transfer as t


class CleaningStrategy(Protocol):
    """Strategy for cleaning."""

    @abstractmethod
    async def evaluate(
        self, task: t.FinishedTask, parameters: dict[str, t.JSON]
    ) -> bool:
        """Evaluate if a task should be cleaned."""

        pass


class CleaningStrategyFactory(Protocol):
    """Factory for creating cleaning strategies."""

    @abstractmethod
    async def create(self, type: str) -> CleaningStrategy | None:
        """Create a cleaning strategy."""

        pass
