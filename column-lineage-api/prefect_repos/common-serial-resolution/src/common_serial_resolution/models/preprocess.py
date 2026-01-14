from typing import Iterable

from . import SerialNumber


def preprocess_serial_number(s: str) -> SerialNumber | None:
    s = s.strip()
    if 1 < len(s) < 24:
        return SerialNumber(s)
    return None


def preprocess_serial_numbers(serial_numbers: Iterable[str]) -> set[SerialNumber]:
    result = {
        preprocess_serial_number(serial_number) for serial_number in serial_numbers
    }
    result.discard(None)
    return result


__all__ = ["preprocess_serial_numbers"]
