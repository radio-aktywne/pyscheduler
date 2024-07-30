import asyncio

from pyscheduler.protocols.event import Event, EventFactory


class EventCache:
    """Cache for events."""

    _factory: EventFactory
    _cache: dict[str, Event]
    _lock: asyncio.Lock

    def __init__(self, factory: EventFactory) -> None:
        self._factory = factory
        self._cache = {}
        self._lock = asyncio.Lock()

    async def get(self, topic: str) -> Event:
        """Get an event for the given topic."""

        async with self._lock:
            if topic not in self._cache:
                self._cache[topic] = await self._factory.create(topic)

            return self._cache[topic]

    async def delete(self, topic: str) -> None:
        """Delete the event for the given topic from the cache."""

        async with self._lock:
            self._cache.pop(topic, None)

    async def clear(self) -> None:
        """Clear the cache."""

        async with self._lock:
            self._cache.clear()
