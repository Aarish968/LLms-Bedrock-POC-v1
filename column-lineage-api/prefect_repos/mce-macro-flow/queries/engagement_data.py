from sqlalchemy import table, column as col

TABLE_ENG = table(
    "SERVICES_DB.SERVICES_ENT_FBV.BV_MCE_AM_ENGAGEMENT_DATA",
    col("c3_matched"),
    col("collector_host_name"),
    col("collector_matched"),
    col("customer_matched"),
    col("engagement_id"),
    col("instance_id"),
    col("operation_code"),
    col("contract_id")
)
