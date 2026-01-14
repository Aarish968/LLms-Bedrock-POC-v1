import logging
from typing import TYPE_CHECKING

from botocore.exceptions import ValidationError
from sqlalchemy import Connection, text

from common_serial_resolution.models.sql_models import (
    SerialResolutionResponse,
)

if TYPE_CHECKING:
    from common_serial_resolution.models.sql_models import (
        SerialResolutionProcedureParams,
    )


logger = logging.getLogger(__name__)


def call_serial_tagging_procedure(
    params: "SerialResolutionProcedureParams",
    conn: "Connection",
) -> SerialResolutionResponse:
    """
    Call the serial tagging procedure.

    The serial numbers should be preprocessed and staged in S3 (see `stage_serial_numbers` function).

    If successful, this function will return two tables:
    - `sn_serial_resolved_<request_id>_tmp`: Contains resolved serial numbers.
    - `sn_serial_ranked_<request_id>_tmp`: Contains ranked serial numbers.

    The process of assigning the 1411 tag is handled by the procedure.
    """
    invalue = params.model_dump_json(by_alias=True)
    logged_user = params.cisco_cco_id

    stmt = text(
        """
        CALL SERIAL_RESOLUTION_V2(:invalue, :logged_user)
        """
    ).bindparams(invalue=invalue, logged_user=logged_user)

    try:
        response = conn.execute(stmt).scalar()
    except Exception:
        logger.exception("Error calling procedure")
        raise

    if response is None:
        msg = "Procedure returned None - This usually indicates an error in the procedure execution"
        logger.error(msg)
        raise ValueError(msg)

    try:
        response = SerialResolutionResponse.validate_json(response)
    except ValidationError as e:
        logger.exception("Validation error in procedure response")
        msg = (
            "Validation error in procedure response. "
            "Ensure the procedure returns a valid JSON response."
        )
        raise ValueError(msg) from e

    return response
