from enum import Enum


class ThoughtSpotSecretNames(str, Enum):
    generic_user = "thoughtspot/generic_user"

    def __str__(self) -> str:
        return str.__str__(self)


class AwsThoughtSpotBlockNames(str, Enum):
    generic_user = "thoughtspot-generic-user"

    def __str__(self) -> str:
        return str.__str__(self)
