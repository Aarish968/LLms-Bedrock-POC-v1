from sqlalchemy import table, column as col, select

from queries.func import NVL

"""
Contract Details
"""

TABLE_HDR_CORE = table(
    "EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_SAHDR_CORE",
    col("contract_number"),
    col("contract_id"),
    col("bill_to_customer_name"),
    col("billto_cr_party_name"),
    col("billto_gu_name"),
    col("bill_to_country"),
    col("service_line_id"),
    col("service_line_name"),
    col("service_line_sts_code"),
    col("edwsf_source_deleted_flag"),
)
HDR_CORE_CTE = (
    select(
        TABLE_HDR_CORE.c.contract_number.label("contract_number"),
        TABLE_HDR_CORE.c.contract_id.label("contract_id"),
        TABLE_HDR_CORE.c.bill_to_customer_name.label("bill_to_customer_name"),
        TABLE_HDR_CORE.c.billto_cr_party_name.label("billto_cr_party_name"),
        TABLE_HDR_CORE.c.billto_gu_name.label("billto_gu_name"),
        TABLE_HDR_CORE.c.bill_to_country.label("bill_to_country"),
        TABLE_HDR_CORE.c.service_line_id.label("service_line_id"),
        TABLE_HDR_CORE.c.service_line_name.label("service_line_name"),
        TABLE_HDR_CORE.c.service_line_sts_code.label("service_line_sts_code"),
    )
    .where(NVL(TABLE_HDR_CORE.c.edwsf_source_deleted_flag, "N") == "N")
    .cte("HDR_CORE_CTE")
)
