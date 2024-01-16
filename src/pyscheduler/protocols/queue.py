from abc import abstractmethod
from typing import Protocol, TypeVar

T = TypeVar("T")


class Queue(Protocol[T]):
    """Supports getting and putting items."""

    @abstractmethod
    async def get(self) -> T:
        """Get an item from the queue."""

        pass

    @abstractmethod
    async def put(self, item: T) -> None:
        """Put an item into the queue."""

        pass
