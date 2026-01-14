from typing import Callable, Optional

from prefect import Task
from prefect.utilities.tasks import defaults_from_attrs
from sqlalchemy import table, VARCHAR, column, update
from sqlalchemy.engine import Engine
from sqlalchemy.sql import quoted_name

T_ENG_FACTORY = Callable[[], Engine]


class UpdateDataCanvasTable(Task):
    """
    Task that handles updating the Data Canvas Request Table with Flow Results
    """

    def __init__(
        self,
        table_name: str,
        engine_factory: T_ENG_FACTORY,
        status: Optional[str] = None,
        error_message: Optional[str] = None,
        output_path: Optional[str] = None,
        request_id: Optional[str] = None,
    ):
        """

        Parameters
        ----------
        table_name : str
            The fully qualified name of the table to update
        engine_factory: T_ENG_FACTORY
            A function that returns an SQLAlchemy Engine
        status : str
            Typically "Success" or "Failed", but any string is allowed. Setting this value here provide a default value
            for the run method.
        error_message: Optional[str]
            Appears as a red error message on UI. Setting this value here provide a default value for the run method.
            Setting this value here provide a default value for the run method.
        output_path : Optional[str]
            The path to the output file. Setting this value here provide a default value for the run method.
        request_id : Optional[str]
            The request id from the Data Canvas Request Table. Setting this value here provide a default value for the
            run method.
        """
        super().__init__()
        self.table_name = table_name
        self.engine_factory = engine_factory
        self.status = status
        self.error_message = error_message
        self.output_path = output_path
        self.request_id = request_id

    @defaults_from_attrs("status", "error_message", "output_path", "request_id")
    def run(
        self,
        status: Optional[str] = None,
        error_message: Optional[str] = None,
        output_path: Optional[str] = None,
        request_id: Optional[str] = None,
    ):
        """
        Update the Data Canvas Request Table

        Parameters
        ----------
        status : Optional[str]
            Typically "Success" or "Failed", but any string is allowed
        error_message: Optional[str]
            Appears as a red error message on UI
        output_path : Optional[str]
            The path to the output file.
        request_id : Optional[str]
            The request id from the Data Canvas Request Table

        Returns
        -------
        """

        dc_table = table(
            quoted_name(self.table_name, quote=False),
            column("status", VARCHAR),
            column("output_file_path", VARCHAR),
            column("error_message", VARCHAR),
            column("request_id", VARCHAR),
        )

        dc_values = {
            "status": status,
            "request_id": request_id,
        }
        if output_path:
            dc_values["output_file_path"] = output_path
        if error_message:
            dc_values["error_message"] = error_message

        update_stmt = (
            update(dc_table)
            .where(dc_table.c.request_id == request_id)
            .values(dc_values)
        )

        engine = self.engine_factory()
        with engine.begin() as conn:
            conn.execute(update_stmt)

        return dc_values
