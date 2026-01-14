from functools import lru_cache
from common_serial_resolution.models.settings import CommonResolutionSettings
from .steps.resolve import run_serial_resolution


@lru_cache(maxsize=1)
def get_settings_() -> CommonResolutionSettings:
    return CommonResolutionSettings()


def get_settings() -> CommonResolutionSettings:
    return get_settings_()
