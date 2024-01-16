from abc import abstractmethod
from typing import Protocol


class Lock(Protocol):
    """Acquires and releases a lock in a context."""

    @abstractmethod
    async def __aenter__(self) -> None:
        pass

    @abstractmethod
    async def __aexit__(self, *args, **kwargs) -> None:
        pass
