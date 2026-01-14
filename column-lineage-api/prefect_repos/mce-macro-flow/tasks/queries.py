from __future__ import annotations

from _operator import itemgetter
from pathlib import Path

import pandas as pd
from prefect import task
from prefect.engine.signals import FAIL
from sqlalchemy import bindparam, create_engine, text
from sqlalchemy.sql import quoted_name

from common import sec
from common.config import RunSettings, DbConfig, FlowEnv
from common.repo import FlowParams
from tasks.macro import replicate_macro


@task(log_stdout=True, tags=["snowflake_small"], checkpoint=False)
def query_mce_data(
    params: FlowParams, settings: RunSettings, e_grid_path: str | Path
) -> pd.DataFrame:
    """

    Parameters
    ----------
    params : FlowParams
    settings : RunSettings
    e_grid_path : str | Path
        Path to the e_grid json file

    Notes
    -------
    Using several CTEs, the elapsed time to run this query with Small Warehouse is 864 seconds, or 14.4 minutes!

    Avoiding CTE creation in favor of filtering
    """

    transient_table_fqn = (
        f"{DbConfig.TRANSIENT_TABLE_CATALOG}.{DbConfig.TRANSIENT_TABLE_SCHEMA}.{params.transient_table_name}"
    ).lower()
    transient_table_param = bindparam(
        "transient_table_name", quoted_name(transient_table_fqn, False)
    )

    engine = create_engine(
        sec.get_sf_pw(
            sec.check_env(settings.env), DbConfig.WAREHOUSE_SMALL, DbConfig.CPS_BIA_BR
        )
    )
    # region Query
    query = text(
        """WITH ISITE_CTE AS
         (SELECT EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_SITE_GU_DENORM.site_use_id               AS site_use_id,
                 EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_SITE_GU_DENORM.party_name                AS party_name,
                 concat(EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_SITE_GU_DENORM.address1, ' ',
                        NVL(EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_SITE_GU_DENORM.address2, '')) AS address,
                 EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_SITE_GU_DENORM.city                      AS city,
                 EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_SITE_GU_DENORM.state                     AS state,
                 EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_SITE_GU_DENORM.country_name              AS country_name,
                 EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_SITE_GU_DENORM.postal_code               AS postal_code,
                 EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_SITE_GU_DENORM.gu_id                     AS gu_id,
                 EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_SITE_GU_DENORM.gu_name                   AS gu_name,
                 EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_SITE_GU_DENORM.site_use_code             AS site_use_code
          FROM EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_SITE_GU_DENORM
          WHERE NVL(EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_SITE_GU_DENORM.edwsf_source_deleted_flag, 'N') = 'N'
            AND EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_SITE_GU_DENORM.site_use_code = 'SHIP_TO'),
     CVD_LINE_CTE AS
         (SELECT EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_CVDPRDLINE_DETAIL.contract_id           AS contract_id,
                 EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_CVDPRDLINE_DETAIL.covered_line_id       AS covered_line_id,
                 EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_CVDPRDLINE_DETAIL.date_terminated       AS date_terminated,
                 EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_CVDPRDLINE_DETAIL.dnr_flag              AS dnr_flag,
                 EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_CVDPRDLINE_DETAIL.end_date              AS end_date,
                 EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_CVDPRDLINE_DETAIL.instance_id           AS instance_id,
                 EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_CVDPRDLINE_DETAIL.line_number           AS line_number,
                 EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_CVDPRDLINE_DETAIL.maintenance_po_number AS maintenance_po_number,
                 EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_CVDPRDLINE_DETAIL.maintenance_so_number AS maintenance_so_number,
                 EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_CVDPRDLINE_DETAIL.price_negotiated      AS price_negotiated,
                 EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_CVDPRDLINE_DETAIL.price_unit            AS price_unit,
                 EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_CVDPRDLINE_DETAIL.service_line_id       AS service_line_id,
                 EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_CVDPRDLINE_DETAIL.start_date            AS start_date,
                 EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_CVDPRDLINE_DETAIL.sts_code              AS sts_code,
                 EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_CVDPRDLINE_DETAIL.usd_price_unit        AS usd_price_unit
          FROM EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_CVDPRDLINE_DETAIL
          WHERE NVL(EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_CVDPRDLINE_DETAIL.edwsf_source_deleted_flag, 'N') = 'N'),
     HDR_CORE_CTE AS
         (SELECT EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_SAHDR_CORE.contract_number       AS contract_number,
                 EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_SAHDR_CORE.contract_id           AS contract_id,
                 EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_SAHDR_CORE.bill_to_customer_name AS bill_to_customer_name,
                 EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_SAHDR_CORE.billto_cr_party_name  AS billto_cr_party_name,
                 EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_SAHDR_CORE.billto_gu_name        AS billto_gu_name,
                 EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_SAHDR_CORE.bill_to_country       AS bill_to_country,
                 EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_SAHDR_CORE.service_line_id       AS service_line_id,
                 EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_SAHDR_CORE.service_line_name     AS service_line_name,
                 EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_SAHDR_CORE.service_line_sts_code AS service_line_sts_code
          FROM EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_SAHDR_CORE
          WHERE NVL(EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_SAHDR_CORE.edwsf_source_deleted_flag, 'N') = 'N'),
     ITEM_CTE AS
         (SELECT EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_SAIB_ITEMS.business_entity_desc_top                               AS business_entity_desc_top,
                 EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_SAIB_ITEMS.business_entity_name_top                               AS business_entity_name_top,
                 EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_SAIB_ITEMS.description                                            AS description,
                 EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_SAIB_ITEMS.ib_product_type                                        AS ib_product_type,
                 EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_SAIB_ITEMS.inventory_item_id                                      AS inventory_item_id,
                 EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_SAIB_ITEMS.last_date_of_support                                   AS last_date_of_support,
                 IFF(EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_SAIB_ITEMS.mapped_to_service_flag = 'YES WITH SPM', 'T',
                     'F')                                                                                             AS mapped_to_service_flag,
                 EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_SAIB_ITEMS.product_family                                         AS product_family,
                 EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_SAIB_ITEMS.product_family_description                             AS product_family_description,
                 EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_SAIB_ITEMS.product_family_mfg_descr                               AS product_family_mfg_descr,
                 EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_SAIB_ITEMS.product_list_price                                     AS product_list_price,
                 EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_SAIB_ITEMS.product_list_price_gpl_us                              AS product_list_price_gpl_us,
                 EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_SAIB_ITEMS.service_list_price                                     AS service_list_price,
                 EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_SAIB_ITEMS.serviceable_product_flag                               AS serviceable_product_flag,
                 EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_SAIB_ITEMS.sub_business_entity_desc_top                           AS sub_business_entity_desc_top,
                 EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_SAIB_ITEMS.sub_business_entity_name_top                           AS sub_business_entity_name_top
          FROM EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_SAIB_ITEMS
          WHERE NVL(EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_SAIB_ITEMS.edwsf_source_deleted_flag, 'N') = 'N')
SELECT IB.instance_number                                                      AS instance_number,
       NVL(IB.serial_number, IB.dup_serial_number)                             AS serial_number,
       NVL(IB.duplicate_ib_flag, 'N')                                          AS duplicate_ib_flag,
       IB.instance_status_desc                                                 AS install_base_status,
       IB.item_name                                                            AS device_name,
       IB.so_number                                                            AS product_so,
       IB.po_number                                                            AS product_po,
       CPS_DB.CPS_DSCI_ARCHIVE.FIX_DATES(IB.ship_date)                         AS ship_date_header,
       IB.collector_host_name                                                  AS collector_host_name,
       IB.collector_matched                                                    AS collector_matched,
       IB.c3_matched                                                           AS c3_matched,
       IB.customer_matched                                                     AS customer_matched,
       IB.item_type_flag                                                       AS item_type_flag,
       IB.covered_status                                                       AS coverage_status,
       IB_PRNT.instance_number                                                 AS parent_instance_number,
       NVL(IB_PRNT.serial_number, IB_PRNT.dup_serial_number)                   AS parent_serial_number,
       CPS_DB.CPS_DSCI_ARCHIVE.FIX_DATES(ITEM_CTE.last_date_of_support)        AS product_last_date_of_support_ldos,
       ITEM_CTE.ib_product_type                                                AS product_type,
       ITEM_CTE.product_family_mfg_descr                                       AS product_family_mfg_descr,
       ITEM_CTE.product_family_description                                     AS product_family_description,
       ITEM_CTE.description                                                    AS product_description,
       ITEM_CTE.product_family                                                 AS product_family,
       ITEM_CTE.business_entity_name_top                                       AS architecture,
       ITEM_CTE.business_entity_desc_top                                       AS architecture_d,
       ITEM_CTE.sub_business_entity_desc_top                                   AS sub_architecture_d,
       ITEM_CTE.sub_business_entity_name_top                                   AS sub_architecture,
       ITEM_CTE.product_list_price                                             AS product_list_price,
       ITEM_CTE.product_list_price_gpl_us                                      AS global_product_list_price,
       ITEM_CTE.serviceable_product_flag                                       AS serviceable_product_flag,
       ITEM_CTE.service_list_price                                             AS service_list_price_raw,
       NVL(CVD_LINE_CTE.dnr_flag, 'N')                                         AS dnr_flag,
       CVD_LINE_CTE.maintenance_so_number                                      AS maintenance_so_number,
       CVD_LINE_CTE.maintenance_po_number                                      AS maintenance_po_number,
       CVD_LINE_CTE.price_negotiated                                           AS price_negotiated,
       CVD_LINE_CTE.line_number                                                AS product_coverage_line_number,
       CPS_DB.CPS_DSCI_ARCHIVE.FIX_DATES(CVD_LINE_CTE.start_date)              AS product_coverage_start_date,
       CPS_DB.CPS_DSCI_ARCHIVE.FIX_DATES(CVD_LINE_CTE.end_date)                AS product_coverage_end_date,
       CPS_DB.CPS_DSCI_ARCHIVE.FIX_DATES(CVD_LINE_CTE.date_terminated)         AS product_coverage_date_terminated,
       CVD_LINE_CTE.sts_code                                                   AS sts_code,
       NVL(CVD_LINE_CTE.usd_price_unit, CVD_LINE_CTE.price_unit)               AS usd_prorated_list_price,
       NVL(CVD_LINE_CTE.usd_price_unit, CVD_LINE_CTE.price_unit) * IB.quantity AS usd_extended_list_price,
       IB.quantity                                                             AS quantity,
       ISITE_CTE.site_use_id                                                   AS installed_at_site_id,
       ISITE_CTE.party_name                                                    AS installed_at_customer_name,
       ISITE_CTE.address                                                       AS installed_at_address_lines,
       ISITE_CTE.city                                                          AS installed_at_city,
       ISITE_CTE.country_name                                                  AS installed_at_country,
       ISITE_CTE.postal_code                                                   AS installed_at_postal_code,
       ISITE_CTE.state                                                         AS installed_at_state_province,
       ISITE_CTE.gu_id                                                         AS installed_at_gu_id,
       ISITE_CTE.gu_name                                                       AS installed_at_gu_name,
       IFF(ITEM_CTE.mapped_to_service_flag = 'YES WITH SPM', 'T', 'F')         AS mapped_to_service_flag,
       HDR_CORE_CTE.contract_number                                            AS contract_number,
       HDR_CORE_CTE.bill_to_customer_name                                      AS contract_bill_to_customer_name,
       HDR_CORE_CTE.billto_cr_party_name                                       AS bill_to_party_name,
       HDR_CORE_CTE.billto_gu_name                                             AS contract_bill_to_customer_gu_name,
       HDR_CORE_CTE.bill_to_country                                            AS contract_bill_to_country,
       HDR_CORE_CTE.service_line_name                                          AS service_level,
       HDR_CORE_CTE.service_line_sts_code                                      AS service_level_status,
       CASE
           WHEN (CVD_LINE_CTE.sts_code IS NOT NULL) THEN CVD_LINE_CTE.sts_code
           WHEN (CVD_LINE_CTE.sts_code IS NULL) THEN CASE
                                                         WHEN (IB.covered_status = 'A') THEN 'ACTIVE'
                                                         WHEN (IB.covered_status = 'I') THEN 'EXPIRED'
                                                         WHEN (IB.covered_status = 'N') THEN 'NEVER COVERED' END
           ELSE 'NEVER COVERED' END                                            AS product_coverage_status
FROM identifier (:transient_table_name) AS IB
         LEFT OUTER JOIN identifier (:transient_table_name) AS IB_PRNT
                         ON IB.parent_instance_id = IB_PRNT.instance_id AND IB.contract_id = IB_PRNT.contract_id
         LEFT OUTER JOIN ISITE_CTE ON IB.install_at_site_use_id = ISITE_CTE.site_use_id
         LEFT OUTER JOIN CVD_LINE_CTE
                         ON IB.instance_id = CVD_LINE_CTE.instance_id AND IB.contract_id = CVD_LINE_CTE.contract_id
         LEFT OUTER JOIN HDR_CORE_CTE ON CVD_LINE_CTE.contract_id = HDR_CORE_CTE.contract_id AND
                                         CVD_LINE_CTE.service_line_id = HDR_CORE_CTE.service_line_id
         LEFT OUTER JOIN ITEM_CTE ON IB.inventory_item_id = ITEM_CTE.inventory_item_id
         """
    ).bindparams(transient_table_param)
    # endregion
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
    print(f"Query result, shape: {df.shape}")

    if len(df) == 0:
        raise FAIL(message="Found 0 rows in the query result")

    # Ported some SQL functions to Python, for brevity
    match_getter = itemgetter("collector_matched", "c3_matched", "customer_matched")
    match_table = {
        ("Y", "Y", "Y"): "ZONE1.3",
        ("Y", "Y", "N"): "ZONE1.1",
        ("Y", "N", "Y"): "ZONE2.1",
        ("Y", "N", "N"): "ZONE2.0",
        ("N", "Y", "Y"): "ZONE1.2",
        ("N", "Y", "N"): "ZONE4.0",
        ("N", "N", "Y"): "ZONE3.0",
    }
    zone_desc_table = {
        "ZONE1.3": "Collector, C3, Customer",
        "ZONE1.1": "Collector, C3",
        "ZONE2.1": "Collector, Customer",
        "ZONE2.0": "Collector",
        "ZONE1.2": "C3, Customer",
        "ZONE4.0": "C3",
        "ZONE3.0": "Customer",
    }
    item_type_table = {"S": "Standalone", "P": "Major", "C": "Minor"}

    def assign_zone(row: pd.Series) -> str | pd.NA:
        # We can assume that the values are either 'Y' or 'N', thanks to the SQL query
        key: tuple[str, str, str] = match_getter(row)  # type: ignore
        return match_table.get(key, pd.NA)

    def assign_zone_desc(row: pd.Series) -> str | pd.NA:
        return zone_desc_table.get(row["zone_id"], pd.NA)

    def assign_config_type(row: pd.Series) -> str | pd.NA:
        return item_type_table.get(row["item_type_flag"], pd.NA)

    df["zone_id"] = df.apply(assign_zone, axis=1)
    df["config_type"] = df.apply(assign_config_type, axis=1)
    df["zone_description"] = df.apply(assign_zone_desc, axis=1)

    if settings.flow_env == FlowEnv.DEV and settings.store_query_output:
        fp_csv = Path(__file__).parent.parent / "tests" / "testdata" / "mce_data.csv"
        print(f"Writing query output to {fp_csv}")
        df.to_csv(fp_csv, index=False)
    df = replicate_macro(
        df=df, customer_name=params.customer_name, e_grid_path=e_grid_path
    )
    if settings.flow_env == FlowEnv.DEV and settings.store_query_output:
        fp_xlsx = Path(__file__).parent.parent / "tests" / "testdata" / "mce_data.xlsx"
        print(f"Writing query output to {fp_xlsx}")
        df.to_excel(fp_xlsx, index=False)
    return df
