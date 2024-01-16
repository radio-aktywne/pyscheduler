from enum import StrEnum


class Status(StrEnum):
    """Status of a task."""

    PENDING = "pending"
    RUNNING = "running"
    CANCELLED = "cancelled"
    FAILED = "failed"
    COMPLETED = "completed"
