from uuid import UUID

from pyscheduler.models import enums as e


class SchedulerError(Exception):
    """Base class for scheduler errors."""

    def __init__(self, message: str | None = None) -> None:
        self._message = message

        args = (message,) if message else ()
        super().__init__(*args)

    @property
    def message(self) -> str | None:
        return self._message


class InvalidOperationError(SchedulerError):
    """Raised when an operation is invalid."""

    def __init__(self, type: str) -> None:
        super().__init__(f"Invalid operation: {type}.")
        self._type = type

    @property
    def type(self) -> str:
        return self._type


class InvalidConditionError(SchedulerError):
    """Raised when a condition is invalid."""

    def __init__(self, type: str) -> None:
        super().__init__(f"Invalid condition: {type}.")
        self._type = type

    @property
    def type(self) -> str:
        return self._type


class DependencyNotFoundError(SchedulerError):
    """Raised when a dependency is not found."""

    def __init__(self, id: UUID) -> None:
        super().__init__(f"Dependency not found: {id}.")
        self._id = id

    @property
    def id(self) -> UUID:
        return self._id


class TaskNotFoundError(SchedulerError):
    """Raised when a task is not found."""

    def __init__(self, id: UUID) -> None:
        super().__init__(f"Task not found: {id}.")
        self._id = id

    @property
    def id(self) -> UUID:
        return self._id


class TaskStatusError(SchedulerError):
    """Raised when a task status is invalid."""

    def __init__(self, id: UUID, status: e.Status) -> None:
        super().__init__(f"Task {id} has invalid status: {status}.")
        self._id = id
        self._status = status

    @property
    def id(self) -> UUID:
        return self._id

    @property
    def status(self) -> e.Status:
        return self._status


class UnsuccessfulDependencyError(SchedulerError):
    """Raised when a dependency is not successful."""

    def __init__(self, id: UUID, status: e.Status) -> None:
        super().__init__(f"Dependency {id} finished with status {status}.")
        self._id = id
        self._status = status

    @property
    def id(self) -> UUID:
        return self._id

    @property
    def status(self) -> e.Status:
        return self._status


class UnexpectedTaskStatusError(SchedulerError):
    """Raised when a task status is unexpected."""

    def __init__(self, id: UUID, status: e.Status) -> None:
        super().__init__(f"Task {id} has unexpected status: {status}.")
        self._id = id
        self._status = status

    @property
    def id(self) -> UUID:
        return self._id

    @property
    def status(self) -> e.Status:
        return self._status


class InvalidCleaningStrategyError(SchedulerError):
    """Raised when a cleaning strategy is invalid."""

    def __init__(self, type: str) -> None:
        super().__init__(f"Invalid cleaning strategy: {type}.")
        self._type = type

    @property
    def type(self) -> str:
        return self._type
