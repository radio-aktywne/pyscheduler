from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Self, override
from uuid import UUID

from pyscheduler.models import enums as e, types as t
from pyscheduler.models.data import storage as s
from pyscheduler.time import isoparse, isostringify


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
class QueuedTask(BaseModel[s.QueuedTask]):
    """Data of a queued task."""

    task: Task
    enqueued: datetime

    @override
    def serialize(self) -> s.QueuedTask:
        return {
            "task": self.task.serialize(),
            "enqueued": isostringify(self.enqueued),
        }

    @classmethod
    @override
    def deserialize(cls, data: s.QueuedTask) -> Self:
        return cls(
            task=Task.deserialize(data["task"]),
            enqueued=isoparse(data["enqueued"]),
        )


@dataclass(kw_only=True)
class WaitingTask(BaseModel[s.WaitingTask]):
    """Data of a waiting task."""

    task: Task
    enqueued: datetime
    dequeued: datetime

    @override
    def serialize(self) -> s.WaitingTask:
        return {
            "task": self.task.serialize(),
            "enqueued": isostringify(self.enqueued),
            "dequeued": isostringify(self.dequeued),
        }

    @classmethod
    @override
    def deserialize(cls, data: s.WaitingTask) -> Self:
        return cls(
            task=Task.deserialize(data["task"]),
            enqueued=isoparse(data["enqueued"]),
            dequeued=isoparse(data["dequeued"]),
        )


@dataclass(kw_only=True)
class SleepingTask(BaseModel[s.SleepingTask]):
    """Data of a sleeping task."""

    task: Task
    enqueued: datetime
    dequeued: datetime | None
    slept: datetime

    @override
    def serialize(self) -> s.SleepingTask:
        return {
            "task": self.task.serialize(),
            "enqueued": isostringify(self.enqueued),
            "dequeued": isostringify(self.dequeued)
            if self.dequeued is not None
            else None,
            "slept": isostringify(self.slept),
        }

    @classmethod
    @override
    def deserialize(cls, data: s.SleepingTask) -> Self:
        return cls(
            task=Task.deserialize(data["task"]),
            enqueued=isoparse(data["enqueued"]),
            dequeued=isoparse(data["dequeued"])
            if data["dequeued"] is not None
            else None,
            slept=isoparse(data["slept"]),
        )


@dataclass(kw_only=True)
class RunningTask(BaseModel[s.RunningTask]):
    """Data of a running task."""

    task: Task
    enqueued: datetime
    dequeued: datetime
    started: datetime

    @override
    def serialize(self) -> s.RunningTask:
        return {
            "task": self.task.serialize(),
            "enqueued": isostringify(self.enqueued),
            "dequeued": isostringify(self.dequeued),
            "started": isostringify(self.started),
        }

    @classmethod
    @override
    def deserialize(cls, data: s.RunningTask) -> Self:
        return cls(
            task=Task.deserialize(data["task"]),
            enqueued=isoparse(data["enqueued"]),
            dequeued=isoparse(data["dequeued"]),
            started=isoparse(data["started"]),
        )


@dataclass(kw_only=True)
class CancelledTask(BaseModel[s.CancelledTask]):
    """Data of a cancelled task."""

    task: Task
    enqueued: datetime
    dequeued: datetime
    started: datetime | None
    cancelled: datetime

    @override
    def serialize(self) -> s.CancelledTask:
        return {
            "task": self.task.serialize(),
            "enqueued": isostringify(self.enqueued),
            "dequeued": isostringify(self.dequeued),
            "started": isostringify(self.started) if self.started is not None else None,
            "cancelled": isostringify(self.cancelled),
        }

    @classmethod
    @override
    def deserialize(cls, data: s.CancelledTask) -> Self:
        return cls(
            task=Task.deserialize(data["task"]),
            enqueued=isoparse(data["enqueued"]),
            dequeued=isoparse(data["dequeued"]),
            started=(
                isoparse(data["started"]) if data["started"] is not None else None
            ),
            cancelled=isoparse(data["cancelled"]),
        )


@dataclass(kw_only=True)
class FailedTask(BaseModel[s.FailedTask]):
    """Data of a failed task."""

    task: Task
    enqueued: datetime
    dequeued: datetime
    started: datetime | None
    failed: datetime
    error: str

    @override
    def serialize(self) -> s.FailedTask:
        return {
            "task": self.task.serialize(),
            "enqueued": isostringify(self.enqueued),
            "dequeued": isostringify(self.dequeued),
            "started": isostringify(self.started) if self.started is not None else None,
            "failed": isostringify(self.failed),
            "error": self.error,
        }

    @classmethod
    @override
    def deserialize(cls, data: s.FailedTask) -> Self:
        return cls(
            task=Task.deserialize(data["task"]),
            enqueued=isoparse(data["enqueued"]),
            dequeued=isoparse(data["dequeued"]),
            started=isoparse(data["started"]) if data["started"] is not None else None,
            failed=isoparse(data["failed"]),
            error=data["error"],
        )


@dataclass(kw_only=True)
class CompletedTask(BaseModel[s.CompletedTask]):
    """Data of a completed task."""

    task: Task
    enqueued: datetime
    dequeued: datetime
    started: datetime
    completed: datetime
    result: t.JSON

    @override
    def serialize(self) -> s.CompletedTask:
        return {
            "task": self.task.serialize(),
            "enqueued": isostringify(self.enqueued),
            "dequeued": isostringify(self.dequeued),
            "started": isostringify(self.started),
            "completed": isostringify(self.completed),
            "result": self.result,
        }

    @classmethod
    @override
    def deserialize(cls, data: s.CompletedTask) -> Self:
        return cls(
            task=Task.deserialize(data["task"]),
            enqueued=isoparse(data["enqueued"]),
            dequeued=isoparse(data["dequeued"]),
            started=isoparse(data["started"]),
            completed=isoparse(data["completed"]),
            result=data["result"],
        )


@dataclass(kw_only=True)
class Tasks(BaseModel[s.Tasks]):
    """Tasks data organized by status."""

    queued: dict[UUID, QueuedTask]
    waiting: dict[UUID, WaitingTask]
    sleeping: dict[UUID, SleepingTask]
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
            "queued": Serializer[QueuedTask, s.QueuedTask]().serialize(
                self.queued,
            ),
            "waiting": Serializer[WaitingTask, s.WaitingTask]().serialize(
                self.waiting,
            ),
            "sleeping": Serializer[SleepingTask, s.SleepingTask]().serialize(
                self.sleeping,
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
            queued=Deserializer[QueuedTask, s.QueuedTask](
                QueuedTask,
            ).deserialize(data["queued"]),
            waiting=Deserializer[WaitingTask, s.WaitingTask](
                WaitingTask,
            ).deserialize(data["waiting"]),
            sleeping=Deserializer[SleepingTask, s.SleepingTask](
                SleepingTask,
            ).deserialize(data["sleeping"]),
            running=Deserializer[RunningTask, s.RunningTask](
                RunningTask,
            ).deserialize(data["running"]),
            cancelled=Deserializer[CancelledTask, s.CancelledTask](
                CancelledTask,
            ).deserialize(data["cancelled"]),
            failed=Deserializer[FailedTask, s.FailedTask](
                FailedTask,
            ).deserialize(data["failed"]),
            completed=Deserializer[CompletedTask, s.CompletedTask](
                CompletedTask,
            ).deserialize(data["completed"]),
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
