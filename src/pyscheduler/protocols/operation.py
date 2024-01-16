from abc import abstractmethod
from typing import Protocol

from pyscheduler.models import types as t


class Operation(Protocol):
    """Operation handles running the logic of a task."""

    @abstractmethod
    async def run(
        self, parameters: dict[str, t.JSON], dependencies: dict[str, t.JSON]
    ) -> t.JSON:
        """Run the operation."""

        pass


class OperationFactory(Protocol):
    """Factory for creating operations."""

    @abstractmethod
    async def create(self, type: str) -> Operation | None:
        """Create an operation."""

        pass
