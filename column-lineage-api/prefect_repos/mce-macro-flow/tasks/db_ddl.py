from __future__ import annotations

from prefect import task
from prefect.engine.signals import FAIL
from prefect.triggers import any_failed
from sqlalchemy import bindparam, Integer, text, create_engine, inspect
from sqlalchemy.sql import quoted_name

from common import sec
from common.config import RunSettings, DbConfig, FlowEnv
from common.repo import FlowParams


@task(log_stdout=True, tags=["snowflake_small"])
def create_mce_transient_table(params: FlowParams, settings: RunSettings) -> str:
    """
    Create a transient table in Snowflake to hold the MCE data.
    This is primarily done as we are joining engagement_id to instance_id, and then doing a self join.

    CSF_XXCCS_DS_INSTANCE_DETAIL is over 1.8 billion rows.
    BV_MCE_AM_ENGAGEMENT_DATA is over 12.8 billion rows.

    Parameters
    ----------
    params : FlowParams
    settings : RunSettings

    Notes
    -----
    Confusingly, BV_MCE_ENGAGEMENT_HDR has a column `ENGAGEMENT_NUMBER`, which is the MCE Engagement ID.
    However, we need another, internal column `ENGAGEMENT_ID` to join to other tables.
    """

    transient_tbl_fqn = f"{DbConfig.TRANSIENT_TABLE_CATALOG}.{DbConfig.TRANSIENT_TABLE_SCHEMA}.{params.transient_table_name}".lower()
    transient_tbl_param = bindparam(
        "transient_table_name", quoted_name(params.transient_table_name, False)
    )
    mce_engagement_id_param = bindparam(
        "mce_engagement_id", params.mce_engagement_id, type_=Integer
    )

    # region Transient Table DDL
    stmt = text(
        """
        CREATE OR REPLACE transient TABLE identifier(:transient_table_name) AS
SELECT EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_INSTANCE_DETAIL.bill_to_site_use_id             AS bill_to_site_use_id,
       EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_INSTANCE_DETAIL.covered_status                  AS covered_status,
       EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_INSTANCE_DETAIL.dup_serial_number               AS dup_serial_number,
       EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_INSTANCE_DETAIL.duplicate_ib_flag               AS duplicate_ib_flag,
       EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_INSTANCE_DETAIL.install_at_site_use_id          AS install_at_site_use_id,
       EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_INSTANCE_DETAIL.instance_status_desc            AS instance_status_desc,
       EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_INSTANCE_DETAIL.inventory_item_id               AS inventory_item_id,
       EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_INSTANCE_DETAIL.item_name                       AS item_name,
       EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_INSTANCE_DETAIL.item_type_flag                  AS item_type_flag,
       EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_INSTANCE_DETAIL.parent_instance_id              AS parent_instance_id,
       EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_INSTANCE_DETAIL.po_number                       AS po_number,
       EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_INSTANCE_DETAIL.quantity                        AS quantity,
       EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_INSTANCE_DETAIL.serial_number                   AS serial_number,
       EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_INSTANCE_DETAIL.ship_date                       AS ship_date,
       EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_INSTANCE_DETAIL.ship_to_site_use_id             AS ship_to_site_use_id,
       EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_INSTANCE_DETAIL.so_number                       AS so_number,
       EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_INSTANCE_DETAIL.instance_number                 AS instance_number,
       SERVICES_DB.SERVICES_ENT_FBV.BV_MCE_AM_ENGAGEMENT_DATA.collector_host_name         AS collector_host_name,
       NVL(SERVICES_DB.SERVICES_ENT_FBV.BV_MCE_AM_ENGAGEMENT_DATA.collector_matched, 'N') AS collector_matched,
       NVL(SERVICES_DB.SERVICES_ENT_FBV.BV_MCE_AM_ENGAGEMENT_DATA.c3_matched, 'N')        AS c3_matched,
       NVL(SERVICES_DB.SERVICES_ENT_FBV.BV_MCE_AM_ENGAGEMENT_DATA.customer_matched, 'N')  AS customer_matched,
       SERVICES_DB.SERVICES_ENT_FBV.BV_MCE_AM_ENGAGEMENT_DATA.instance_id                 AS instance_id,
       SERVICES_DB.SERVICES_ENT_FBV.BV_MCE_AM_ENGAGEMENT_DATA.engagement_id               AS engagement_id,
       SERVICES_DB.SERVICES_ENT_FBV.BV_MCE_AM_ENGAGEMENT_DATA.contract_id                 AS contract_id,
       SERVICES_DB.SERVICES_ENT_FBV.BV_MCE_AM_ENGAGEMENT_HDR.engagement_number            AS engagement_number
       FROM SERVICES_DB.SERVICES_ENT_FBV.BV_MCE_AM_ENGAGEMENT_HDR
         JOIN SERVICES_DB.SERVICES_ENT_FBV.BV_MCE_AM_ENGAGEMENT_DATA
              ON SERVICES_DB.SERVICES_ENT_FBV.BV_MCE_AM_ENGAGEMENT_HDR.engagement_id =
                    SERVICES_DB.SERVICES_ENT_FBV.BV_MCE_AM_ENGAGEMENT_DATA.engagement_id
            JOIN EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_INSTANCE_DETAIL
                ON EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_INSTANCE_DETAIL.instance_id =
                    SERVICES_DB.SERVICES_ENT_FBV.BV_MCE_AM_ENGAGEMENT_DATA.instance_id
         WHERE SERVICES_DB.SERVICES_ENT_FBV.BV_MCE_AM_ENGAGEMENT_HDR.engagement_number = :mce_engagement_id
  AND SERVICES_DB.SERVICES_ENT_FBV.BV_MCE_AM_ENGAGEMENT_DATA.operation_code IN ('I', 'U', 'N')
        """
    ).bindparams(transient_tbl_param, mce_engagement_id_param)
    # endregion
    engine = create_engine(
        sec.get_sf_pw(
            sec.check_env(settings.env), DbConfig.WAREHOUSE_SMALL, DbConfig.SCHEMA_API
        )
    )

    if settings.flow_env == FlowEnv.DEV:
        print(f"DEV: Checking for existing transient table : {transient_tbl_fqn}")
        inspector = inspect(engine)
        has_table = inspector.has_table(transient_tbl_fqn)
        if has_table:
            print(f"DEV: Found existing transient table {transient_tbl_fqn}")
            return params.transient_table_name
        print(
            f"DEV: Did not find existing transient table {transient_tbl_fqn}, proceeding to create transient table..."
        )

    print(f"Creating transient table {transient_tbl_fqn}...")
    with engine.connect() as conn:
        conn.execute(stmt)
    print(f"Created transient table {transient_tbl_fqn}")

    query = text(
        """
        SELECT COUNT(*) FROM identifier(:transient_table_name)
        """
    ).bindparams(transient_tbl_param)
    with engine.connect() as conn:
        count = conn.execute(query).scalar()
    if count == 0:
        drop_tbl_stmt = text(
            """
            DROP TABLE IF EXISTS identifier(:transient_table_name)
            """
        ).bindparams(transient_tbl_param)
        with engine.connect() as conn:
            conn.execute(drop_tbl_stmt)
        print(f"Transient table {transient_tbl_fqn} is empty, dropping table...")
        raise FAIL(message=f"Transient table {transient_tbl_fqn} is empty")
    print(f"{transient_tbl_fqn}: {count} Rows")

    return params.transient_table_name


@task(log_stdout=True, tags=["snowflake_xsmall"])
def drop_mce_transient_table(transient_table_name: str, settings: RunSettings) -> str:
    """
    Drop the Transient Table. Perform some basic safety checks to ensure we're referring to a transient table.

    Parameters
    ----------
    transient_table_name : str
        Name of table to drop
    settings : RunSettings

    Returns
    -------
    str
        The name of the transient table that was dropped
    """

    # If in dev, we don't want to drop the table
    if settings.flow_env == FlowEnv.DEV:
        print(
            f"Skipping drop of transient table {transient_table_name} in {settings.flow_env}"
        )
        return transient_table_name

    # Check that the table name uses the expected template convention
    is_templated_table = DbConfig.is_mce_transient_table(transient_table_name)
    if not is_templated_table:
        raise FAIL(
            f"Table name {transient_table_name} does not match expected template - Failing Flow"
        )

    info_schema = (
        f"{DbConfig.TRANSIENT_TABLE_CATALOG}.INFORMATION_SCHEMA.TABLES"
    ).lower()

    info_schema_param = bindparam("info", quoted_name(info_schema, False))
    # This doesn't require the quoted_name, because it's being used as a string value
    transient_table_param = bindparam("transient_table_name", transient_table_name)

    query_is_transient = text(
        """
        SELECT IS_TRANSIENT FROM identifier(:info)
        WHERE TABLE_NAME = :transient_table_name
        """
    ).bindparams(info_schema_param, transient_table_param)

    engine = create_engine(
        sec.get_sf_pw(
            sec.check_env(settings.env), DbConfig.WAREHOUSE_XSMALL, DbConfig.SCHEMA_API
        )
    )

    # Check that the table is a transient table
    with engine.connect() as conn:
        is_transient_result = conn.execute(
            query_is_transient,
        ).scalar()
    if is_transient_result is None:
        raise FAIL(f"Table {transient_table_name} not found - Failing Flow")
    if is_transient_result != "YES":
        raise FAIL(
            f"Table {transient_table_name} is not a transient table - Failing Flow"
        )

    transient_table_fqn = f"{DbConfig.TRANSIENT_TABLE_CATALOG}.{DbConfig.TRANSIENT_TABLE_SCHEMA}.{transient_table_name}".lower()
    transient_table_fqn_param = bindparam(
        "transient_table_name", quoted_name(transient_table_fqn, False)
    )

    # language=Snowflake
    stmt = text(f"DROP TABLE identifier(:transient_table_name)").bindparams(
        transient_table_fqn_param
    )
    print(f"Dropping transient table {transient_table_name}...")
    with engine.connect() as conn:
        conn.execute(stmt)
    print(f"Dropped transient table {transient_table_name}")
    return transient_table_name


@task(log_stdout=True, tags=["snowflake_xsmall"])
def update_generic_upload_log_table(
    params: FlowParams, settings: "RunSettings"
) -> bool:
    """
    Update the generic upload log table with the output file path
    Parameters
    ----------
    params: FlowParams
    settings: RunSettings

    Returns
    -------

    """

    gen_upload_table = (
        f"{DbConfig.CPS_DB}.{DbConfig.CPS_BIA_BR}.{DbConfig.TBL_GENERIC_UPLOAD}"
    ).lower()
    stmt = text(
        """
        UPDATE identifier(:gen_upload_table)
        set STATUS = 'Success', OUTPUT_FILE_PATH = :output_uri
        where REQUEST_ID = :request_id
        """
    ).bindparams(
        bindparam("gen_upload_table", quoted_name(gen_upload_table, False)),
        bindparam("output_uri", params.output_uri),
        bindparam("request_id", params.run_id),
    )

    if settings.flow_env != FlowEnv.PROD:
        print("Not updating table in non-prod environment")
        return True

    engine = create_engine(
        sec.get_sf_pw(
            sec.check_env(settings.env),
            DbConfig.WAREHOUSE_XSMALL,
            "prd_cps_dsci_etl_svc",
        )
    )
    try:
        with engine.connect() as conn:
            conn.execute(stmt)
    except Exception as e:
        print(f"Error updating {gen_upload_table}: {e}")
        return False
    return True


@task(log_stdout=True, trigger=any_failed, tags=["snowflake_xsmall"])
def update_generic_upload_log_table_as_failed(
    params: FlowParams, settings: RunSettings
) -> bool:
    """
    Update the generic upload log table with the failed status
    Parameters
    ----------
    params: FlowParams
    settings: RunSettings

    Returns
    -------
    """

    gen_upload_table = (
        f"{DbConfig.CPS_DB}.{DbConfig.CPS_BIA_BR}.{DbConfig.TBL_GENERIC_UPLOAD}"
    ).lower()
    stmt = text(
        """
        UPDATE identifier(:gen_upload_table)
        set STATUS = 'Failed'
        where REQUEST_ID = :request_id
        """
    ).bindparams(
        bindparam("gen_upload_table", quoted_name(gen_upload_table, False)),
        bindparam("request_id", params.run_id),
    )

    if settings.flow_env != FlowEnv.PROD:
        print("Not updating table in non-prod environment")
        return True
    engine = create_engine(
        sec.get_sf_pw(
            sec.check_env(settings.env),
            DbConfig.WAREHOUSE_XSMALL,
            "prd_cps_dsci_etl_svc",
        )
    )
    try:
        with engine.connect() as conn:
            conn.execute(stmt)
    except Exception as e:
        print(f"Error updating {gen_upload_table}: {e}")
        return False
    return True
