from sqlalchemy import table, column as col, select
from sqlalchemy.sql.functions import concat

from queries.func import NVL

TABLE_ISITE = table(
    "EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_SITE_GU_DENORM",
    col("site_use_id"),
    col("party_name"),
    col("address1"),
    col("address2"),
    col("city"),
    col("state"),
    col("country_name"),
    col("postal_code"),
    col("gu_id"),
    col("gu_name"),
    col("edwsf_source_deleted_flag"),
    col("site_use_code"),
)
ISITE_CTE = (
    select(
        TABLE_ISITE.c.site_use_id.label("site_use_id"),
        TABLE_ISITE.c.party_name.label("party_name"),
        concat(TABLE_ISITE.c.address1, " ", NVL(TABLE_ISITE.c.address2, "")).label(
            "address"
        ),
        TABLE_ISITE.c.city.label("city"),
        TABLE_ISITE.c.state.label("state"),
        TABLE_ISITE.c.country_name.label("country_name"),
        TABLE_ISITE.c.postal_code.label("postal_code"),
        TABLE_ISITE.c.gu_id.label("gu_id"),
        TABLE_ISITE.c.gu_name.label("gu_name"),
        TABLE_ISITE.c.site_use_code.label("site_use_code"),
    )
    .where(NVL(TABLE_ISITE.c.edwsf_source_deleted_flag, "N") == "N")
    .where(TABLE_ISITE.c.site_use_code == "SHIP_TO")
    .cte("ISITE_CTE")
)
