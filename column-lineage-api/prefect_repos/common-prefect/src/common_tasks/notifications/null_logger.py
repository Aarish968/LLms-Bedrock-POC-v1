from typing import Any


class NullNotificationHandler():
    
    def __init__(self, **kwargs):
        ...
    
    def _null_method(self, *arg, **kwargs):
        return None
    
    def __getattr__(self, name: str) -> Any:
        return self._null_method