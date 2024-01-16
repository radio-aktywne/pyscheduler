from typing import TypedDict

from pyscheduler.models import types as t


class Specification(TypedDict):
    """Generic specification for type-based implementation."""

    type: str
    parameters: dict[str, t.JSON]


class Task(TypedDict):
    """Core task data."""

    operation: Specification
    condition: Specification
    dependencies: dict[str, str]


class PendingTask(TypedDict):
    """Data of a pending task."""

    task: Task
    scheduled: str


class RunningTask(TypedDict):
    """Data of a running task."""

    task: Task
    scheduled: str
    started: str


class CancelledTask(TypedDict):
    """Data of a cancelled task."""

    task: Task
    scheduled: str
    started: str | None
    cancelled: str


class FailedTask(TypedDict):
    """Data of a failed task."""

    task: Task
    scheduled: str
    started: str
    failed: str
    error: str


class CompletedTask(TypedDict):
    """Data of a completed task."""

    task: Task
    scheduled: str
    started: str
    completed: str
    result: t.JSON


class Tasks(TypedDict):
    """Tasks data organized by status."""

    pending: dict[str, PendingTask]
    running: dict[str, RunningTask]
    cancelled: dict[str, CancelledTask]
    failed: dict[str, FailedTask]
    completed: dict[str, CompletedTask]


class Relationships(TypedDict):
    """Relationships between tasks."""

    dependents: dict[str, list[str]]
    dependencies: dict[str, list[str]]


class State(TypedDict):
    """State of the scheduler."""

    tasks: Tasks
    statuses: dict[str, t.Status]
    relationships: Relationships
