from abc import abstractmethod
from typing import Protocol


class Event(Protocol):
    """Supports waiting for an event to happen and notifying that it has happened."""

    @abstractmethod
    async def wait(self) -> None:
        """Wait for the event to happen."""

        pass

    @abstractmethod
    async def notify(self) -> None:
        """Notify that the event has happened."""

        pass


class EventFactory(Protocol):
    """Factory for creating events."""

    @abstractmethod
    async def create(self, topic: str) -> Event:
        """Create an event for the given topic."""

        pass
