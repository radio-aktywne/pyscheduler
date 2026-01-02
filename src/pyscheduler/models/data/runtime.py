from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Self, override
from uuid import UUID

from pyscheduler.models import enums as e
from pyscheduler.models import types as t
from pyscheduler.models.data import storage as s


class BaseModel[S](ABC):
    """Base class for runtime data models."""

    @abstractmethod
    def serialize(self) -> S:
        """Serialize the model to a storage model."""

    @classmethod
    @abstractmethod
    def deserialize(cls, data: S) -> Self:
        """Deserialize the model from a storage model."""


@dataclass(kw_only=True)
class Specification(BaseModel[s.Specification]):
    """Generic specification for type-based implementation."""

    type: str
    parameters: dict[str, t.JSON]

    @override
    def serialize(self) -> s.Specification:
        return {
            "type": self.type,
            "parameters": self.parameters,
        }

    @classmethod
    @override
    def deserialize(cls, data: s.Specification) -> Self:
        return cls(
            type=data["type"],
            parameters=data["parameters"],
        )


@dataclass(kw_only=True)
class Task(BaseModel[s.Task]):
    """Core task data."""

    operation: Specification
    condition: Specification
    dependencies: dict[str, UUID]

    @override
    def serialize(self) -> s.Task:
        return {
            "operation": self.operation.serialize(),
            "condition": self.condition.serialize(),
            "dependencies": {
                key: str(value) for key, value in self.dependencies.items()
            },
        }

    @classmethod
    @override
    def deserialize(cls, data: s.Task) -> Self:
        return cls(
            operation=Specification.deserialize(data["operation"]),
            condition=Specification.deserialize(data["condition"]),
            dependencies={
                key: UUID(value) for key, value in data["dependencies"].items()
            },
        )


@dataclass(kw_only=True)
class PendingTask(BaseModel[s.PendingTask]):
    """Data of a pending task."""

    task: Task
    scheduled: datetime

    @override
    def serialize(self) -> s.PendingTask:
        return {
            "task": self.task.serialize(),
            "scheduled": self.scheduled.isoformat(),
        }

    @classmethod
    @override
    def deserialize(cls, data: s.PendingTask) -> Self:
        return cls(
            task=Task.deserialize(data["task"]),
            scheduled=datetime.fromisoformat(data["scheduled"]),
        )


@dataclass(kw_only=True)
class RunningTask(BaseModel[s.RunningTask]):
    """Data of a running task."""

    task: Task
    scheduled: datetime
    started: datetime

    @override
    def serialize(self) -> s.RunningTask:
        return {
            "task": self.task.serialize(),
            "scheduled": self.scheduled.isoformat(),
            "started": self.started.isoformat(),
        }

    @classmethod
    @override
    def deserialize(cls, data: s.RunningTask) -> Self:
        return cls(
            task=Task.deserialize(data["task"]),
            scheduled=datetime.fromisoformat(data["scheduled"]),
            started=datetime.fromisoformat(data["started"]),
        )


@dataclass(kw_only=True)
class CancelledTask(BaseModel[s.CancelledTask]):
    """Data of a cancelled task."""

    task: Task
    scheduled: datetime
    started: datetime | None
    cancelled: datetime

    @override
    def serialize(self) -> s.CancelledTask:
        return {
            "task": self.task.serialize(),
            "scheduled": self.scheduled.isoformat(),
            "started": self.started.isoformat() if self.started is not None else None,
            "cancelled": self.cancelled.isoformat(),
        }

    @classmethod
    @override
    def deserialize(cls, data: s.CancelledTask) -> Self:
        return cls(
            task=Task.deserialize(data["task"]),
            scheduled=datetime.fromisoformat(data["scheduled"]),
            started=(
                datetime.fromisoformat(data["started"])
                if data["started"] is not None
                else None
            ),
            cancelled=datetime.fromisoformat(data["cancelled"]),
        )


@dataclass(kw_only=True)
class FailedTask(BaseModel[s.FailedTask]):
    """Data of a failed task."""

    task: Task
    scheduled: datetime
    started: datetime
    failed: datetime
    error: str

    @override
    def serialize(self) -> s.FailedTask:
        return {
            "task": self.task.serialize(),
            "scheduled": self.scheduled.isoformat(),
            "started": self.started.isoformat(),
            "failed": self.failed.isoformat(),
            "error": self.error,
        }

    @classmethod
    @override
    def deserialize(cls, data: s.FailedTask) -> Self:
        return cls(
            task=Task.deserialize(data["task"]),
            scheduled=datetime.fromisoformat(data["scheduled"]),
            started=datetime.fromisoformat(data["started"]),
            failed=datetime.fromisoformat(data["failed"]),
            error=data["error"],
        )


@dataclass(kw_only=True)
class CompletedTask(BaseModel[s.CompletedTask]):
    """Data of a completed task."""

    task: Task
    scheduled: datetime
    started: datetime
    completed: datetime
    result: t.JSON

    @override
    def serialize(self) -> s.CompletedTask:
        return {
            "task": self.task.serialize(),
            "scheduled": self.scheduled.isoformat(),
            "started": self.started.isoformat(),
            "completed": self.completed.isoformat(),
            "result": self.result,
        }

    @classmethod
    @override
    def deserialize(cls, data: s.CompletedTask) -> Self:
        return cls(
            task=Task.deserialize(data["task"]),
            scheduled=datetime.fromisoformat(data["scheduled"]),
            started=datetime.fromisoformat(data["started"]),
            completed=datetime.fromisoformat(data["completed"]),
            result=data["result"],
        )


@dataclass(kw_only=True)
class Tasks(BaseModel[s.Tasks]):
    """Tasks data organized by status."""

    pending: dict[UUID, PendingTask]
    running: dict[UUID, RunningTask]
    cancelled: dict[UUID, CancelledTask]
    failed: dict[UUID, FailedTask]
    completed: dict[UUID, CompletedTask]

    @override
    def serialize(self) -> s.Tasks:
        class Serializer[R: BaseModel, S]:
            def serialize(self, data: dict[UUID, R]) -> dict[str, S]:
                return {str(key): value.serialize() for key, value in data.items()}

        return {
            "pending": Serializer[PendingTask, s.PendingTask]().serialize(
                self.pending,
            ),
            "running": Serializer[RunningTask, s.RunningTask]().serialize(
                self.running,
            ),
            "cancelled": Serializer[CancelledTask, s.CancelledTask]().serialize(
                self.cancelled,
            ),
            "failed": Serializer[FailedTask, s.FailedTask]().serialize(
                self.failed,
            ),
            "completed": Serializer[CompletedTask, s.CompletedTask]().serialize(
                self.completed,
            ),
        }

    @classmethod
    @override
    def deserialize(cls, data: s.Tasks) -> Self:
        class Deserializer[R: BaseModel, S]:
            def __init__(self, model: type[R]) -> None:
                self._model = model

            def deserialize(self, data: dict[str, S]) -> dict[UUID, R]:
                return {
                    UUID(key): self._model.deserialize(value)
                    for key, value in data.items()
                }

        return cls(
            pending=Deserializer[PendingTask, s.PendingTask](
                PendingTask,
            ).deserialize(
                data["pending"],
            ),
            running=Deserializer[RunningTask, s.RunningTask](
                RunningTask,
            ).deserialize(
                data["running"],
            ),
            cancelled=Deserializer[CancelledTask, s.CancelledTask](
                CancelledTask,
            ).deserialize(
                data["cancelled"],
            ),
            failed=Deserializer[FailedTask, s.FailedTask](
                FailedTask,
            ).deserialize(
                data["failed"],
            ),
            completed=Deserializer[CompletedTask, s.CompletedTask](
                CompletedTask,
            ).deserialize(
                data["completed"],
            ),
        )


@dataclass(kw_only=True)
class Relationships(BaseModel[s.Relationships]):
    """Relationships between tasks."""

    dependents: dict[UUID, set[UUID]]
    dependencies: dict[UUID, set[UUID]]

    @override
    def serialize(self) -> s.Relationships:
        class Serializer:
            def serialize(self, data: dict[UUID, set[UUID]]) -> dict[str, list[str]]:
                return {
                    str(key): [str(value) for value in values]
                    for key, values in data.items()
                }

        return {
            "dependents": Serializer().serialize(self.dependents),
            "dependencies": Serializer().serialize(self.dependencies),
        }

    @classmethod
    @override
    def deserialize(cls, data: s.Relationships) -> Self:
        class Deserializer:
            def deserialize(self, data: dict[str, list[str]]) -> dict[UUID, set[UUID]]:
                return {
                    UUID(key): {UUID(value) for value in values}
                    for key, values in data.items()
                }

        return cls(
            dependents=Deserializer().deserialize(data["dependents"]),
            dependencies=Deserializer().deserialize(data["dependencies"]),
        )


@dataclass(kw_only=True)
class State(BaseModel[s.State]):
    """State of the scheduler."""

    tasks: Tasks
    statuses: dict[UUID, e.Status]
    relationships: Relationships

    @override
    def serialize(self) -> s.State:
        return {
            "tasks": self.tasks.serialize(),
            "statuses": {str(key): value.value for key, value in self.statuses.items()},
            "relationships": self.relationships.serialize(),
        }

    @classmethod
    @override
    def deserialize(cls, data: s.State) -> Self:
        return cls(
            tasks=Tasks.deserialize(data["tasks"]),
            statuses={
                UUID(key): e.Status(value) for key, value in data["statuses"].items()
            },
            relationships=Relationships.deserialize(data["relationships"]),
        )
