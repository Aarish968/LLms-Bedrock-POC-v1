import logging
from typing import Any

from sqlalchemy import Connection, TextClause, text

from common_prefect_next.utils import json_dumps

logger = logging.getLogger(__name__)


def make_wf_background_stmt(
    request_id: int,
    parameters: dict,
    workflow_data: Any,
    job_id: str,
    run_id: str,
    updated_by: str,
) -> "TextClause":
    workflow_data = workflow_data or {}

    stmt = text(
        """
        UPDATE DC_WF_BACKGROUND_JOB
        SET EXTERNAL_JOB_ID = :job_id,
            EXTERNAL_RUN_ID = :run_id,
            PARAMETERS = :parameters,
            WORKFLOW_DATA = :workflow_data,
            UPDATED_BY = :updated_by,
            UPDATE_DTM = SYSDATE()
        WHERE REQUEST_ID = :request_id
        AND IS_DELETED = 'F'
        """
    ).bindparams(
        job_id=job_id,
        run_id=run_id,
        parameters=json_dumps(parameters),
        workflow_data=json_dumps(workflow_data),
        updated_by=updated_by,
        request_id=request_id,
    )

    return stmt


def track_wf_background_job(conn: "Connection") -> None:
    """
    A flow could be called directly or triggered as a result of an event and automation.

    For cases where a flow is triggered by an event, the flow is responsible for updating the
    `DC_WF_BACKGROUND_JOB` table with:
    -   EXTERNAL_JOB_ID - UUID of the deployment
    -   EXTERNAL_RUN_ID - Status of the deployment
    -   PARAMETERS - Parameters of the flow run

    This is not critical so we just log the error and continue

    """

    from prefect.context import get_run_context

    def _track_wf_background_job() -> None:
        ctx = get_run_context()
        flow_run = ctx.flow_run
        if not flow_run:
            logger.warning("Flow run not found - skipping tracking")
            return
        job_id = str(flow_run.flow_id)
        run_id = str(flow_run.id)
        parameters = flow_run.parameters
        updated_by = flow_run.name
        request_id = int(parameters["request_id"])
        stmt = make_wf_background_stmt(
            request_id=request_id,
            parameters=parameters,
            workflow_data=None,
            job_id=job_id,
            run_id=run_id,
            updated_by=updated_by,
        )
        conn.execute(stmt)

    try:
        _track_wf_background_job()
    except Exception:
        logger.exception("Failed to track workflow background job")
        return None
