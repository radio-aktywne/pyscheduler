from datetime import datetime, timezone


def utcnow() -> datetime:
    """Return the current time in UTC with no timezone information."""

    return datetime.now(tz=timezone.utc).replace(tzinfo=None)
