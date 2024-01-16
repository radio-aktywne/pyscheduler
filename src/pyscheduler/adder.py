from uuid import UUID, uuid4

from pyscheduler.errors import InvalidConditionError, InvalidOperationError
from pyscheduler.models import transfer as t
from pyscheduler.models.data import runtime as r
from pyscheduler.modifier import Modifier
from pyscheduler.protocols.condition import ConditionFactory
from pyscheduler.protocols.lock import Lock
from pyscheduler.protocols.operation import OperationFactory
from pyscheduler.protocols.queue import Queue
from pyscheduler.time import utcnow


class Adder:
    """Handles adding new tasks."""

    def __init__(
        self,
        lock: Lock,
        queue: Queue[UUID],
        modifier: Modifier,
        operations: OperationFactory,
        conditions: ConditionFactory,
    ) -> None:
        self._lock = lock
        self._queue = queue
        self._modifier = modifier
        self._operations = operations
        self._conditions = conditions

    async def add(self, request: t.ScheduleRequest) -> t.PendingTask:
        """Add a task."""

        operation = await self._operations.create(request.operation.type)
        if operation is None:
            raise InvalidOperationError(request.operation.type)

        condition = await self._conditions.create(request.condition.type)
        if condition is None:
            raise InvalidConditionError(request.condition.type)

        id = uuid4()

        task = r.Task(
            operation=r.Specification(
                type=request.operation.type,
                parameters=request.operation.parameters,
            ),
            condition=r.Specification(
                type=request.condition.type,
                parameters=request.condition.parameters,
            ),
            dependencies=request.dependencies,
        )

        async with self._lock:
            task = await self._modifier.add_pending_task(id, task, utcnow())
            await self._queue.put(id)

        return t.PendingTask(
            task=t.Task(
                id=id,
                operation=t.Specification(
                    type=task.task.operation.type,
                    parameters=task.task.operation.parameters,
                ),
                condition=t.Specification(
                    type=task.task.condition.type,
                    parameters=task.task.condition.parameters,
                ),
                dependencies=task.task.dependencies,
            ),
            scheduled=task.scheduled,
        )
