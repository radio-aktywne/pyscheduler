from typing import Any, Literal

JSON = dict[str, Any] | list[Any] | str | int | float | bool | None
Status = Literal["pending", "running", "cancelled", "failed", "completed"]
