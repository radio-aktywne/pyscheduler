from abc import abstractmethod
from collections.abc import AsyncGenerator
from typing import Protocol

from pyscheduler.models import transfer as t


class CleaningStrategy(Protocol):
    """Strategy for cleaning."""

    @abstractmethod
    async def cycle(self) -> AsyncGenerator[None, None]:
        """Yield when ready to clean."""

        pass

    @abstractmethod
    async def evaluate(self, task: t.FinishedTask) -> bool:
        """Evaluate if a task should be cleaned."""

        pass
