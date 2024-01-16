from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

from pyscheduler.models.enums import Status
from pyscheduler.models.types import JSON


@dataclass
class Specification:
    """Generic specification for type-based implementation."""

    type: str
    parameters: dict[str, JSON]


@dataclass
class ScheduleRequest:
    """Request to schedule a task."""

    operation: Specification
    condition: Specification
    dependencies: dict[str, UUID]


@dataclass
class CancelRequest:
    """Request to cancel a task."""

    id: UUID


@dataclass
class CleanRequest:
    """Request to clean tasks."""

    strategy: Specification


@dataclass
class Task:
    """Core task data."""

    id: UUID
    operation: Specification
    condition: Specification
    dependencies: dict[str, UUID]


@dataclass
class TaskIndex:
    """Index of tasks by status."""

    pending: set[UUID]
    running: set[UUID]
    cancelled: set[UUID]
    failed: set[UUID]
    completed: set[UUID]


@dataclass
class GenericTask:
    """Data of a task of any status."""

    task: Task
    status: Status


@dataclass
class PendingTask:
    """Data of a pending task."""

    task: Task
    scheduled: datetime


@dataclass
class RunningTask:
    """Data of a running task."""

    task: Task
    scheduled: datetime
    started: datetime


@dataclass
class CancelledTask:
    """Data of a cancelled task."""

    task: Task
    scheduled: datetime
    started: datetime | None
    cancelled: datetime


@dataclass
class FailedTask:
    """Data of a failed task."""

    task: Task
    scheduled: datetime
    started: datetime
    failed: datetime
    error: str


@dataclass
class CompletedTask:
    """Data of a completed task."""

    task: Task
    scheduled: datetime
    started: datetime
    completed: datetime
    result: JSON


FinishedTask = CancelledTask | FailedTask | CompletedTask


@dataclass
class CleaningResult:
    """Result of cleaning."""

    removed: set[UUID]
