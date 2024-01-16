from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Generic, TypedDict, TypeVar
from uuid import UUID

from pyscheduler.models import enums as e
from pyscheduler.models import types as t
from pyscheduler.models.data import storage as s

StorageModelType = TypeVar("StorageModelType", bound=TypedDict)
RuntimeModelType = TypeVar("RuntimeModelType", bound="BaseModel")
StorageType = TypeVar("StorageType")
RuntimeType = TypeVar("RuntimeType")


class BaseModel(Generic[StorageModelType], ABC):
    """Base class for runtime data models."""

    @abstractmethod
    def serialize(self) -> StorageModelType:
        """Serialize the model to a storage model."""
        pass

    @classmethod
    @abstractmethod
    def deserialize(
        cls: type[RuntimeModelType], data: StorageModelType
    ) -> RuntimeModelType:
        """Deserialize the model from a storage model."""
        pass


@dataclass
class Specification(BaseModel[s.Specification]):
    """Generic specification for type-based implementation."""

    type: str
    parameters: dict[str, t.JSON]

    def serialize(self) -> s.Specification:
        return {
            "type": self.type,
            "parameters": self.parameters,
        }

    @classmethod
    def deserialize(
        cls: type["Specification"], data: s.Specification
    ) -> "Specification":
        return cls(
            type=data["type"],
            parameters=data["parameters"],
        )


@dataclass
class Task(BaseModel[s.Task]):
    """Core task data."""

    operation: Specification
    condition: Specification
    dependencies: dict[str, UUID]

    def serialize(self) -> s.Task:
        return {
            "operation": self.operation.serialize(),
            "condition": self.condition.serialize(),
            "dependencies": {
                key: str(value) for key, value in self.dependencies.items()
            },
        }

    @classmethod
    def deserialize(cls: type["Task"], data: s.Task) -> "Task":
        return cls(
            operation=Specification.deserialize(data["operation"]),
            condition=Specification.deserialize(data["condition"]),
            dependencies={
                key: UUID(value) for key, value in data["dependencies"].items()
            },
        )


@dataclass
class PendingTask(BaseModel[s.PendingTask]):
    """Data of a pending task."""

    task: Task
    scheduled: datetime

    def serialize(self) -> s.PendingTask:
        return {
            "task": self.task.serialize(),
            "scheduled": self.scheduled.isoformat(),
        }

    @classmethod
    def deserialize(cls: type["PendingTask"], data: s.PendingTask) -> "PendingTask":
        return cls(
            task=Task.deserialize(data["task"]),
            scheduled=datetime.fromisoformat(data["scheduled"]),
        )


@dataclass
class RunningTask(BaseModel[s.RunningTask]):
    """Data of a running task."""

    task: Task
    scheduled: datetime
    started: datetime

    def serialize(self) -> s.RunningTask:
        return {
            "task": self.task.serialize(),
            "scheduled": self.scheduled.isoformat(),
            "started": self.started.isoformat(),
        }

    @classmethod
    def deserialize(cls: type["RunningTask"], data: s.RunningTask) -> "RunningTask":
        return cls(
            task=Task.deserialize(data["task"]),
            scheduled=datetime.fromisoformat(data["scheduled"]),
            started=datetime.fromisoformat(data["started"]),
        )


@dataclass
class CancelledTask(BaseModel[s.CancelledTask]):
    """Data of a cancelled task."""

    task: Task
    scheduled: datetime
    started: datetime | None
    cancelled: datetime

    def serialize(self) -> s.CancelledTask:
        return {
            "task": self.task.serialize(),
            "scheduled": self.scheduled.isoformat(),
            "started": self.started.isoformat() if self.started is not None else None,
            "cancelled": self.cancelled.isoformat(),
        }

    @classmethod
    def deserialize(
        cls: type["CancelledTask"], data: s.CancelledTask
    ) -> "CancelledTask":
        return cls(
            task=Task.deserialize(data["task"]),
            scheduled=datetime.fromisoformat(data["scheduled"]),
            started=datetime.fromisoformat(data["started"])
            if data["started"] is not None
            else None,
            cancelled=datetime.fromisoformat(data["cancelled"]),
        )


@dataclass
class FailedTask(BaseModel[s.FailedTask]):
    """Data of a failed task."""

    task: Task
    scheduled: datetime
    started: datetime
    failed: datetime
    error: str

    def serialize(self) -> s.FailedTask:
        return {
            "task": self.task.serialize(),
            "scheduled": self.scheduled.isoformat(),
            "started": self.started.isoformat(),
            "failed": self.failed.isoformat(),
            "error": self.error,
        }

    @classmethod
    def deserialize(cls: type["FailedTask"], data: s.FailedTask) -> "FailedTask":
        return cls(
            task=Task.deserialize(data["task"]),
            scheduled=datetime.fromisoformat(data["scheduled"]),
            started=datetime.fromisoformat(data["started"]),
            failed=datetime.fromisoformat(data["failed"]),
            error=data["error"],
        )


@dataclass
class CompletedTask(BaseModel[s.CompletedTask]):
    """Data of a completed task."""

    task: Task
    scheduled: datetime
    started: datetime
    completed: datetime
    result: t.JSON

    def serialize(self) -> s.CompletedTask:
        return {
            "task": self.task.serialize(),
            "scheduled": self.scheduled.isoformat(),
            "started": self.started.isoformat(),
            "completed": self.completed.isoformat(),
            "result": self.result,
        }

    @classmethod
    def deserialize(
        cls: type["CompletedTask"], data: s.CompletedTask
    ) -> "CompletedTask":
        return cls(
            task=Task.deserialize(data["task"]),
            scheduled=datetime.fromisoformat(data["scheduled"]),
            started=datetime.fromisoformat(data["started"]),
            completed=datetime.fromisoformat(data["completed"]),
            result=data["result"],
        )


@dataclass
class Tasks(BaseModel[s.Tasks]):
    """Tasks data organized by status."""

    pending: dict[UUID, PendingTask]
    running: dict[UUID, RunningTask]
    cancelled: dict[UUID, CancelledTask]
    failed: dict[UUID, FailedTask]
    completed: dict[UUID, CompletedTask]

    def serialize(self) -> s.Tasks:
        class Serializer(Generic[RuntimeModelType, StorageModelType]):
            def serialize(
                self, data: dict[UUID, RuntimeModelType]
            ) -> dict[str, StorageModelType]:
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
    def deserialize(cls: type["Tasks"], data: s.Tasks) -> "Tasks":
        class Deserializer(Generic[StorageModelType, RuntimeModelType]):
            def __init__(self, model: type[RuntimeModelType]) -> None:
                self._model = model

            def deserialize(
                self, data: dict[str, StorageModelType]
            ) -> dict[UUID, RuntimeModelType]:
                return {
                    UUID(key): self._model.deserialize(value)
                    for key, value in data.items()
                }

        return cls(
            pending=Deserializer[s.PendingTask, PendingTask](
                PendingTask,
            ).deserialize(
                data["pending"],
            ),
            running=Deserializer[s.RunningTask, RunningTask](
                RunningTask,
            ).deserialize(
                data["running"],
            ),
            cancelled=Deserializer[s.CancelledTask, CancelledTask](
                CancelledTask,
            ).deserialize(
                data["cancelled"],
            ),
            failed=Deserializer[s.FailedTask, FailedTask](
                FailedTask,
            ).deserialize(
                data["failed"],
            ),
            completed=Deserializer[s.CompletedTask, CompletedTask](
                CompletedTask,
            ).deserialize(
                data["completed"],
            ),
        )


@dataclass
class Relationships(BaseModel[s.Relationships]):
    """Relationships between tasks."""

    dependents: dict[UUID, set[UUID]]
    dependencies: dict[UUID, set[UUID]]

    def serialize(self) -> s.Relationships:
        class Serializer(Generic[RuntimeType, StorageType]):
            def serialize(
                self, data: dict[UUID, set[RuntimeType]]
            ) -> dict[str, list[StorageType]]:
                return {
                    str(key): [str(value) for value in values]
                    for key, values in data.items()
                }

        return {
            "dependents": Serializer[UUID, str]().serialize(self.dependents),
            "dependencies": Serializer[UUID, str]().serialize(self.dependencies),
        }

    @classmethod
    def deserialize(
        cls: type["Relationships"], data: s.Relationships
    ) -> "Relationships":
        class Deserializer(Generic[StorageType, RuntimeType]):
            def deserialize(
                self, data: dict[str, list[StorageType]]
            ) -> dict[UUID, set[RuntimeType]]:
                return {
                    UUID(key): {UUID(value) for value in values}
                    for key, values in data.items()
                }

        return cls(
            dependents=Deserializer[str, UUID]().deserialize(data["dependents"]),
            dependencies=Deserializer[str, UUID]().deserialize(data["dependencies"]),
        )


@dataclass
class State(BaseModel[s.State]):
    """State of the scheduler."""

    tasks: Tasks
    statuses: dict[UUID, e.Status]
    relationships: Relationships

    def serialize(self) -> s.State:
        return {
            "tasks": self.tasks.serialize(),
            "statuses": {str(key): value.value for key, value in self.statuses.items()},
            "relationships": self.relationships.serialize(),
        }

    @classmethod
    def deserialize(cls: type["State"], data: s.State) -> "State":
        return cls(
            tasks=Tasks.deserialize(data["tasks"]),
            statuses={
                UUID(key): e.Status(value) for key, value in data["statuses"].items()
            },
            relationships=Relationships.deserialize(data["relationships"]),
        )
