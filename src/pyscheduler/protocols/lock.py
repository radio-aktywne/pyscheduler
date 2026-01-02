from abc import abstractmethod
from types import TracebackType
from typing import Protocol


class Lock(Protocol):
    """Acquires and releases a lock in a context."""

    @abstractmethod
    async def __aenter__(self) -> None:
        pass

    @abstractmethod
    async def __aexit__(
        self,
        exception_type: type[BaseException] | None,
        exception: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        pass
