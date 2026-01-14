from sqlalchemy import table, column as col, select

from queries.func import NVL

"""
This table includes product coverage contracts. For a given instance_id, there can be multiple contracts.
Past, current, and future contracts are included.
"""

TABLE_CVD_LINE = table(
    "EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_CVDPRDLINE_DETAIL",
    col("contract_id"),
    col("covered_line_id"),
    col("date_terminated"),
    col("dnr_flag"),
    col("edwsf_source_deleted_flag"),
    col("end_date"),
    col("instance_id"),
    col("line_number"),
    col("maintenance_po_number"),
    col("maintenance_so_number"),
    col("price_negotiated"),
    col("price_unit"),
    col("service_line_id"),
    col("start_date"),
    col("sts_code"),
    col("usd_price_unit"),
)
CVD_LINE_CTE = (
    select(
        TABLE_CVD_LINE.c.contract_id.label("contract_id"),
        TABLE_CVD_LINE.c.covered_line_id.label("covered_line_id"),
        TABLE_CVD_LINE.c.date_terminated.label("date_terminated"),
        TABLE_CVD_LINE.c.dnr_flag.label("dnr_flag"),
        TABLE_CVD_LINE.c.end_date.label("end_date"),
        TABLE_CVD_LINE.c.instance_id.label("instance_id"),
        TABLE_CVD_LINE.c.line_number.label("line_number"),
        TABLE_CVD_LINE.c.maintenance_po_number.label("maintenance_po_number"),
        TABLE_CVD_LINE.c.maintenance_so_number.label("maintenance_so_number"),
        TABLE_CVD_LINE.c.price_negotiated.label("price_negotiated"),
        TABLE_CVD_LINE.c.price_unit.label("price_unit"),
        TABLE_CVD_LINE.c.service_line_id.label("service_line_id"),
        TABLE_CVD_LINE.c.start_date.label("start_date"),
        TABLE_CVD_LINE.c.sts_code.label("sts_code"),
        TABLE_CVD_LINE.c.usd_price_unit.label("usd_price_unit")
    )
    .where(NVL(TABLE_CVD_LINE.c.edwsf_source_deleted_flag, "N") == "N")
    .cte("CVD_LINE_CTE")
)
