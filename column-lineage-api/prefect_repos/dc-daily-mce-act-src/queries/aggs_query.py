aggs_query = """
create or replace table CPS_DSCI_ARCHIVE.device_level_aggs as
select
PARENT_INSTANCE_ID,
count(distinct INSTANCE_ID) as total_config_lines,
sum(case when ORDERV_CURRENT =1 then g.QUANTITY else 0 end ) as quantity_total,  -- bc we are not filtering to curent = 1 we woudl dbl count
sum(case when ORDERV_CURRENT =1 then g.USD_EXTENDED_LIST_PRICE else 0 end ) as USD_EXTENDED_LIST_PRICE_TOTAL,  -- bc we are not filtering to curent = 1 we woudl dbl count
sum(case when ORDERV_CURRENT =1 then g.PRICE_NEGOTIATED else 0 end ) as PRICE_NEGOTIATED_TOTAL,  -- bc we are not filtering to curent = 1 we woudl dbl count
sum(case when ORDERV_CURRENT =1 then g.USD_PRORATED_LIST_PRICE else 0 end ) as USD_PRORATED_LIST_PRICE_TOTAL,  -- bc we are not filtering to curent = 1 we woudl dbl count
sum(case when ORDERV_CURRENT =1 then g.PRODUCT_LIST_PRICE else 0 end ) as product_list_price_total,  -- bc we are not filtering to curent = 1 we woudl dbl count
sum(case when ORDERV_CURRENT =1 then g.SERVICE_LIST_PRICE_RAW else 0 end ) as SERVICE_LIST_PRICE_RAW_TOTAL,  -- bc we are not filtering to curent = 1 we woudl dbl count
sum( case when ORDERV_CURRENT =1  and g.PRODUCT_RELATIONSHIP in ('Parent', 'Standalone') then 1 else 0 end ) as total_parents,
-- business rule from Athul  no more that 1
least(1,sum( case when ORDERV_CURRENT =1 and g.real_product_type ='CHASSIS' then 1 else 0 end )) as total_chassis,
sum( case when ORDERV_CURRENT =1 and g.install_base_status ='Latest-INSTALLED' then 1 else 0 end ) as total_latest_installed,
sum( case when ORDERV_CURRENT =1 and g.install_base_status !='Latest-INSTALLED' then 1 else 0 end ) as not_total_latest_installed,
sum( case when ORDERV_CURRENT =1 and g.real_product_type ='SOFTWARE'then 1 else 0 end ) as total_sw_product_type,
sum( case when ORDERV_CURRENT =1 and g.real_product_type !='SOFTWARE'then 1 else 0 end ) as total_non_sw_product_type,
-- total in the current that are active or signed status
sum( case when ORDERV_CURRENT =1 and g.STS_CODE in ('ACTIVE', 'SIGNED') then 1 else 0 end ) as TOTAL_ACTIVE_OR_SIGNED_IN_CONFIG,
--todo   whEN PARENT IS NOT LDSO BUT cHILD IS
-- sERVICE LEVEL
-- sTATUS
-- MULTI CONTRACT IN CURRENT
-- sum( case when ORDERV_CURRENT =1 and g.is_ldos_today ='True' then 1 else 0 end ) as count_of_ldos_in_current_config,
-- now we want to span across ordev
count(distinct g.install_base_status) as install_base_status_length,
count(distinct g.CONTRACT_NUMBER_C) as contract_number_list_length,
count(distinct g.MAINTENANCE_SO_NUMBER) as maintenance_so_number_list_length,
count(distinct g.INSTALLED_AT_SITE_ID) as installed_at_site_id_list_length,
count(distinct g.SERVICE_LEVEL_C) as  total_service_levels_all_current_contracts
from CPS_DSCI_ARCHIVE.configs_n_coverage g
--where  PARENT_INSTANCE_ID   = 5360869353
      --PARENT_INSTANCE_ID = 5149466997 and
      -- ORDERV_CURRENT =1  dont use for aggregation or you lose the multi covered contracts
group by PARENT_INSTANCE_ID
-- having count(0) > 1INS
;
"""