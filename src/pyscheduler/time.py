from datetime import UTC, datetime


def utcnow() -> datetime:
    """Return the current time in UTC with no timezone information."""
    return datetime.now(tz=UTC).replace(tzinfo=None)
