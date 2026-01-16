from enum import Enum


class LiveboardType(str, Enum):
    CANVAS = "canvas"
    ENGAGEMENT = "engagement"
    USER = "user"
