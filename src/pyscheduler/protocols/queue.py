from abc import abstractmethod
from typing import Protocol


class Queue[T](Protocol):
    """Supports getting and putting items."""

    @abstractmethod
    async def get(self) -> T:
        """Get an item from the queue."""

        pass

    @abstractmethod
    async def put(self, item: T) -> None:
        """Put an item into the queue."""

        pass
