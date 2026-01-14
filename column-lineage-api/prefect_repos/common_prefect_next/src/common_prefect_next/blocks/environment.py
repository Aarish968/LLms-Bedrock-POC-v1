from enum import Enum
from typing import Literal


class Env(str, Enum):
    dev = "dev"
    prod = "prod"

    def __str__(self) -> str:
        return str.__str__(self)


TEnv = Literal["dev", "prod"]
