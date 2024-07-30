from abc import abstractmethod
from typing import Protocol


class Store[T](Protocol):
    """Supports getting and setting a value."""

    @abstractmethod
    async def get(self) -> T:
        """Return the stored value."""

        pass

    @abstractmethod
    async def set(self, value: T) -> None:
        """Set the stored value."""

        pass
