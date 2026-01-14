from sqlalchemy import table, column as col

TABLE_IB = table(
    "EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_INSTANCE_DETAIL",
    col("bill_to_site_use_id"),
    col("covered_status"),
    col("dup_serial_number"),
    col("duplicate_ib_flag"),
    col("edwsf_source_deleted_flag"),
    col("install_at_site_use_id"),
    col("instance_id"),
    col("instance_number"),
    col("instance_status_desc"),
    col("inventory_item_id"),
    col("item_name"),
    col("item_type_flag"),
    col("parent_instance_id"),
    col("po_number"),
    col("quantity"),
    col("serial_number"),
    col("ship_date"),
    col("ship_to_site_use_id"),
    col("so_number"),
)
