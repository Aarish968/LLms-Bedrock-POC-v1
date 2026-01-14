from enum import Enum


class EventStage(str, Enum):
    start = "start"
    success = "success"
    failure = "failure"
    requested = "requested"

    def __str__(self) -> str:
        return str.__str__(self)
