from sqlalchemy import table, column as col, select

from queries.func import NVL, IFF

TABLE_ITEM = table(
    "EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_SAIB_ITEMS",
    col("business_entity_desc_top"),
    col("business_entity_name_top"),
    col("description"),
    col("edwsf_source_deleted_flag"),
    col("ib_product_type"),
    col("inventory_item_id"),
    col("last_date_of_support"),
    col("mapped_to_service_flag"),
    col("product_family"),
    col("product_family_description"),
    col("product_family_mfg_descr"),
    col("product_list_price"),
    col("product_list_price_gpl_us"),
    col("service_list_price"),
    col("serviceable_product_flag"),
    col("sub_business_entity_desc_top"),
    col("sub_business_entity_name_top"),
)
ITEM_CTE = (
    select(
        TABLE_ITEM.c.business_entity_desc_top.label("business_entity_desc_top"),
        TABLE_ITEM.c.business_entity_name_top.label("business_entity_name_top"),
        TABLE_ITEM.c.description.label("description"),
        TABLE_ITEM.c.ib_product_type.label("ib_product_type"),
        TABLE_ITEM.c.inventory_item_id.label("inventory_item_id"),
        TABLE_ITEM.c.last_date_of_support.label("last_date_of_support"),
        IFF(TABLE_ITEM.c.mapped_to_service_flag == "YES WITH SPM", "T", "F").label("mapped_to_service_flag"),
        TABLE_ITEM.c.product_family.label("product_family"),
        TABLE_ITEM.c.product_family_description.label("product_family_description"),
        TABLE_ITEM.c.product_family_mfg_descr.label("product_family_mfg_descr"),
        TABLE_ITEM.c.product_list_price.label("product_list_price"),
        TABLE_ITEM.c.product_list_price_gpl_us.label("product_list_price_gpl_us"),
        TABLE_ITEM.c.service_list_price.label("service_list_price"),
        TABLE_ITEM.c.serviceable_product_flag.label("serviceable_product_flag"),
        TABLE_ITEM.c.sub_business_entity_desc_top.label("sub_business_entity_desc_top"),
        TABLE_ITEM.c.sub_business_entity_name_top.label("sub_business_entity_name_top"),
    )
    .where(NVL(TABLE_ITEM.c.edwsf_source_deleted_flag, "N") == "N")
    .cte("ITEM_CTE")
)
