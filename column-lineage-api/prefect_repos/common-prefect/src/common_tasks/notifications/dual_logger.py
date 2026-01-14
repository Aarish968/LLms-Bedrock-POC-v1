from typing import TYPE_CHECKING, Optional

from common_tasks.notifications.null_logger import NullNotificationHandler

if TYPE_CHECKING:
    from logging import Logger

    from common_tasks.notifications import ApiNotificationHandler
    from common_tasks.notifications.api_logger import Status


class DualLogger:
    
    """
    Handles logging to two different logging mechanisms.

    This class facilitates the process of logging messages to both a Prefect logger and an
    API logger. This can be useful for ensuring that log messages are consistently recorded
    in multiple places for reliability and redundancy.

    Attributes
    ----------
    prefect_logger : Optional[Logger]
        Logger instance for logging messages within the Prefect framework.
    api_logger : Optional[ApiNotificationHandler]
        Logger instance for logging messages to an external API.
    """
    
    def __init__(self,
                 prefect_logger: Optional["Logger"],
                 api_logger: Optional["ApiNotificationHandler"]
                 ):
        self.prefect_logger=prefect_logger if prefect_logger else NullNotificationHandler()
        self.api_logger=api_logger if api_logger else NullNotificationHandler()
        
    def debug(self, msg: str):
        self.prefect_logger.debug(msg)
    
    def info(self, msg: str):
        self.prefect_logger.info(msg)
        self.api_logger.send_text(msg)
    
    def send_text(self, msg: str):
        self.prefect_logger.info(msg)
        try:
            self.api_logger.send_text(msg)
        except:
            self.prefect_logger.warning(f"Failed to notify '{msg}'")
    
    def error(self, msg):
        self.prefect_logger.error(msg)
        try:
            self.api_logger.send_text(msg, status='error')
        except:
            self.prefect_logger.warning(f"Failed to notify '{msg}'")
            
    def exception(self, msg, *, exception: Optional[Exception]=None):
        self.prefect_logger.exception(msg)
        try:
            self.api_logger.send_exception(msg, exception=exception)
        except:
            self.prefect_logger.warning(f"Failed to notify '{msg}'")
    
    def send_exception(self, msg, *, exception: Optional[Exception]=None):
        self.prefect_logger.exception(msg)
        try:
            self.api_logger.send_exception(msg, exception=exception)
        except:
            self.prefect_logger.warning(f"Failed to notify '{msg}'")
    
    def send_download_link(self, url: str, *, label: Optional[str]=None, status: Optional["Status"]="result"):
        self.prefect_logger.info(f"Sending download link... {url=}")
        try:
            self.api_logger.send_download_link(url, label=label, status=status)
        except:
            self.prefect_logger.warning(f"Failed to notify '{url}'")
            
    def send_table(self, table: dict, *, status: Optional["Status"]=None):
        self.prefect_logger.info(f"Sending table... {table=}")
        try:
            self.api_logger.send_table(table, status=status)
        except Exception:
            self.prefect_logger.warning(f"Failed to notify '{table}'")
            
    def mark_successful(self, *, message: Optional[str]=None):
        self.prefect_logger.info(f"Marking successful... {message=}")
        try:
            self.api_logger.mark_successful(message=message)
        except:
            self.prefect_logger.warning(f"Failed to notify '{message}'")