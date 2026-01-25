from datetime import UTC, datetime


def awareutcnow() -> datetime:
    """Return the current datetime in UTC with timezone information."""
    return datetime.now(UTC)


def naiveutcnow() -> datetime:
    """Return the current datetime in UTC without timezone information."""
    return awareutcnow().replace(tzinfo=None)


def isostringify(dt: datetime) -> str:
    """Convert a datetime to a string in ISO 8601 format."""
    return dt.isoformat().replace("+00:00", "Z")


def isoparse(value: str) -> datetime:
    """Parse a string in ISO 8601 format to a datetime."""
    return datetime.fromisoformat(value)
