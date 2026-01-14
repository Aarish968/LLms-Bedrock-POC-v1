from enum import Enum


class WorkQueueNames(str, Enum):
    dev = "dev"
    prod = "prod"
    thoughtspot_dev = "thoughtspot-dev"
    thoughtspot_prod = "thoughtspot-prod"

    def __str__(self) -> str:
        return str.__str__(self)
