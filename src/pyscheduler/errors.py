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
        """Error message."""
        return self._message


class InvalidOperationError(SchedulerError):
    """Raised when an operation is invalid."""

    def __init__(self, operation_type: str) -> None:
        super().__init__(f"Invalid operation: {operation_type}.")
        self._operation_type = operation_type

    @property
    def type(self) -> str:
        """Type of the operation."""
        return self._operation_type


class InvalidConditionError(SchedulerError):
    """Raised when a condition is invalid."""

    def __init__(self, condition_type: str) -> None:
        super().__init__(f"Invalid condition: {condition_type}.")
        self._condition_type = condition_type

    @property
    def type(self) -> str:
        """Type of the condition."""
        return self._condition_type


class DependencyNotFoundError(SchedulerError):
    """Raised when a dependency is not found."""

    def __init__(self, dependency_id: UUID) -> None:
        super().__init__(f"Dependency not found: {dependency_id}.")
        self._dependency_id = dependency_id

    @property
    def id(self) -> UUID:
        """Identifier of the dependency."""
        return self._dependency_id


class TaskNotFoundError(SchedulerError):
    """Raised when a task is not found."""

    def __init__(self, task_id: UUID) -> None:
        super().__init__(f"Task not found: {task_id}.")
        self._task_id = task_id

    @property
    def id(self) -> UUID:
        """Identifier of the task."""
        return self._task_id


class TaskStatusError(SchedulerError):
    """Raised when a task status is invalid."""

    def __init__(self, task_id: UUID, status: e.Status) -> None:
        super().__init__(f"Task {task_id} has invalid status: {status}.")
        self._task_id = task_id
        self._status = status

    @property
    def id(self) -> UUID:
        """Identifier of the task."""
        return self._task_id

    @property
    def status(self) -> e.Status:
        """Status of the task."""
        return self._status


class UnsuccessfulDependencyError(SchedulerError):
    """Raised when a dependency is not successful."""

    def __init__(self, dependency_id: UUID, status: e.Status) -> None:
        super().__init__(f"Dependency {dependency_id} finished with status {status}.")
        self._dependency_id = dependency_id
        self._status = status

    @property
    def id(self) -> UUID:
        """Identifier of the dependency."""
        return self._dependency_id

    @property
    def status(self) -> e.Status:
        """Status of the dependency."""
        return self._status


class UnexpectedTaskStatusError(SchedulerError):
    """Raised when a task status is unexpected."""

    def __init__(self, task_id: UUID, status: e.Status) -> None:
        super().__init__(f"Task {task_id} has unexpected status: {status}.")
        self._task_id = task_id
        self._status = status

    @property
    def id(self) -> UUID:
        """Identifier of the task."""
        return self._task_id

    @property
    def status(self) -> e.Status:
        """Status of the task."""
        return self._status


class InvalidCleaningStrategyError(SchedulerError):
    """Raised when a cleaning strategy is invalid."""

    def __init__(self, strategy_type: str) -> None:
        super().__init__(f"Invalid cleaning strategy: {strategy_type}.")
        self._strategy_type = strategy_type

    @property
    def type(self) -> str:
        """Type of the cleaning strategy."""
        return self._strategy_type
