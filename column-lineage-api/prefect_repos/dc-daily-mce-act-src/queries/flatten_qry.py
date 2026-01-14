flatten_qry = """create or replace table CPS_DSCI_ARCHIVE.configs_n_coverage as
    select ib.INSTANCE_ID,
           ib.PARENT_INSTANCE_ID,
           CASE WHEN ib.INSTANCE_ID = ib.PARENT_INSTANCE_ID THEN 'Y' ELSE NULL END                      AS is_actual_parent,
           ib.LAST_COVERED_LINE_ID,
           nvl(cvd_line.COVERED_LINE_ID, -1)                                                            as this_covered_line_id,
           rank() over ( partition by ib.INSTANCE_ID order by nvl(cvd_line.COVERED_LINE_ID, -1) desc)   as orderv_current,
           ib.INSTANCE_STATUS_DESC                                                                      as install_base_status,
           CASE
               WHEN IB.item_type_flag = 'S' THEN 'Standalone'
               WHEN IB.item_type_flag = 'P' THEN 'Parent'
               WHEN IB.item_type_flag = 'C' THEN 'Child'
               ELSE NULL
               END                                                                                         product_relationship,
           cvd_line.MAINTENANCE_SO_NUMBER,
           item.service_list_price                                                                      as service_list_price_raw,--130 , 342
           cvd_line.PRICE_NEGOTIATED,                                                                                               --495  637 alt location vs nasty cte
           item.product_list_price,
           ib.install_at_site_use_id                                                                    as installed_at_site_id,
           hdr_core_c.contract_number                                                                   as contract_number_c,       --38
           hdr_core_c.service_line_name                                                                 as service_level_c,         --128
           cvd_line.STS_CODE                                                                            as sts_code  , -- the product status code
           ib.QUANTITY,
           nvl(cvd_line.USD_PRICE_UNIT, cvd_line.PRICE_UNIT)                                            as usd_prorated_list_price, --504
           nvl(cvd_line.USD_PRICE_UNIT, cvd_line.PRICE_UNIT) * ib.QUANTITY                              as usd_extended_list_price , -- 505
           nvl(cp.FIXED_PRODUCT_TYPE,nvl(item.ib_product_type,'Unknown')) as real_product_type,
            array_agg(DISTINCT hdr_core_c.service_line_name) OVER ( PARTITION BY ib.PARENT_INSTANCE_ID) as list_of_service_levels,
            array_agg(DISTINCT cvd_line.COVERED_LINE_ID::bigint ) OVER ( PARTITION BY ib.PARENT_INSTANCE_ID) as list_of_covered_lines,
            array_agg(DISTINCT hdr_core_c.contract_number  ) OVER ( PARTITION BY ib.PARENT_INSTANCE_ID) as list_of_contracts,
            array_agg(DISTINCT ib.LAST_COVERED_LINE_ID::bigint ) OVER ( PARTITION BY ib.PARENT_INSTANCE_ID) as list_of_prior_covered_lines
    -- for notes...
--     array_agg(DISTINCT STS_CODE) OVER ( PARTITION BY INSTANCE_ID) as coverage_status_list,
--     array_agg(DISTINCT INSTALL_BASE_STATUS) OVER ( PARTITION BY INSTANCE_ID) as install_status_list,
--     array_agg(DISTINCT THIS_COVERED_LINE_ID) OVER ( PARTITION BY INSTANCE_ID) as this_covered_line_list
    from EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_INSTANCE_DETAIL ib
        left join CPS_DSCI_ARCHIVE.CORRECTED_PIDS cp on (ib.ITEM_NAME=cp.ITEM_NAME)
        left join EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_SAIB_ITEMS item on
        (item.INVENTORY_ITEM_ID = ib.inventory_item_id
         and nvl(item.EDWSF_SOURCE_DELETED_FLAG, 'N') = 'N'
        )
        left join EDW_SERVICE_ETL_DB.ss.CSF_XXCCS_DS_CVDPRDLINE_DETAIL cvd_line on
        ( ib.instance_id = cvd_line.instance_id
          and nvl(cvd_line.EDWSF_SOURCE_DELETED_FLAG, 'N') = 'N'
          and nvl(ib.EDWSF_SOURCE_DELETED_FLAG, 'N') = 'N'
        )
        left join EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_SAHDR_CORE hdr_core_c on
        ( cvd_line.contract_id = hdr_core_c.contract_id
          and cvd_line.service_line_id = hdr_core_c.service_line_id
          and  nvl(hdr_core_c.EDWSF_SOURCE_DELETED_FLAG, 'N') = 'N'
        )

;
"""