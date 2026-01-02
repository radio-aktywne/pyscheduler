from abc import abstractmethod
from typing import Protocol

from pyscheduler.models import types as t


class Condition(Protocol):
    """Condition handles waiting for a task to be ready to run."""

    @abstractmethod
    async def wait(self, parameters: dict[str, t.JSON]) -> None:
        """Wait for the condition to be met."""


class ConditionFactory(Protocol):
    """Factory for creating conditions."""

    @abstractmethod
    async def create(self, condition_type: str) -> Condition | None:
        """Create a condition."""
