mce_src_prep_qry = """ use warehouse CPS_DSCI_ETL_EXT3_WH; --small
create or replace table CPS_DSCI_ARCHIVE.test_MCE_src_4_29 as
with resolved_eol as (
    select eol.BK_END_OF_LIFE_REQUEST_NUM,
           eol.BK_PRODUCT_ID,
           TO_CHAR(CPS_DSCI_ARCHIVE.FIX_DATES_W_DEF(gp.END_OF_CHANGE_DT::DATE, '2150-12-31'::DATE),'yyyy-mm-dd')    as END_OF_CHANGE_DT,
           TO_CHAR(CPS_DSCI_ARCHIVE.FIX_DATES_W_DEF(gp.END_OF_MANUFACTURING_DT::DATE, '2150-12-31'::DATE),'yyyy-mm-dd')   as END_OF_MANUFACTURING_DT,
           TO_CHAR(CPS_DSCI_ARCHIVE.FIX_DATES_W_DEF(gp.END_OF_NEW_SVC_ATTACHMENT_DT::DATE, '2150-12-31'::DATE),'yyyy-mm-dd')  as END_OF_NEW_SVC_ATTACHMENT_DT,
           TO_CHAR(CPS_DSCI_ARCHIVE.FIX_DATES_W_DEF(gp.END_OF_SOFTWARE_MAINTENANCE_DT::DATE, '2150-12-31'::DATE),'yyyy-mm-dd')  as END_OF_SOFTWARE_MAINTENANCE_DT,
           TO_CHAR(CPS_DSCI_ARCHIVE.FIX_DATES_W_DEF(gp.END_OF_ROUTINE_FAIL_ANLYSYS_DT::DATE, '2150-12-31'::DATE),'yyyy-mm-dd')  as END_OF_ROUTINE_FAIL_ANLYSYS_DT,
           TO_CHAR(CPS_DSCI_ARCHIVE.FIX_DATES_W_DEF(gp.END_OF_SALE_DT::DATE, '2150-12-31'::DATE),'yyyy-mm-dd')   as END_OF_SALE_DT,
           TO_CHAR(CPS_DSCI_ARCHIVE.FIX_DATES_W_DEF(gp.EOL_SOFTWARE_AVAILABLE_DT::DATE, '2150-12-31'::DATE),'yyyy-mm-dd')     as EOL_SOFTWARE_AVAILABLE_DT,
           TO_CHAR(CPS_DSCI_ARCHIVE.FIX_DATES_W_DEF(gp.END_OF_SFTWR_LICENSE_AVAIL_DT::DATE, '2150-12-31'::DATE),'yyyy-mm-dd')     as END_OF_SFTWR_LICENSE_AVAIL_DT,
           TO_CHAR(CPS_DSCI_ARCHIVE.FIX_DATES_W_DEF(gp.EOL_SIGNATURE_RELEASE_DT::DATE, '2150-12-31'::DATE),'yyyy-mm-dd')   as EOL_SIGNATURE_RELEASE_DT,
           TO_CHAR(CPS_DSCI_ARCHIVE.FIX_DATES_W_DEF(gp.END_OF_SVC_CONTRACT_RNWL_DT::DATE, '2150-12-31'::DATE),'yyyy-mm-dd')   as END_OF_SVC_CONTRACT_RNWL_DT,
           TO_CHAR(CPS_DSCI_ARCHIVE.FIX_DATES_W_DEF(gp.END_OF_TAC_ENGG_SUPPORT_DT::DATE, '2150-12-31'::DATE),'yyyy-mm-dd')      as END_OF_TAC_ENGG_SUPPORT_DT,
           rank() over ( partition by eol.BK_PRODUCT_ID order by eol.BK_END_OF_LIFE_REQUEST_NUM desc,eol.EDW_CREATE_DATETIME desc ) as orderv
    from CPS_DB.CPS_DSCI_EBV.BV_END_OF_LIFE_PRODUCT eol
             join CPS_DB.CPS_DSCI_EBV.BV_EOL_BULLETIN_MILESTONE_GROUP gp
                  ON   (
                              gp.BK_END_OF_LIFE_REQUEST_NUM = eol.BK_END_OF_LIFE_REQUEST_NUM
                          and
                              gp.BK_EOL_BULLETIN_PRODUCT_TYP_CD = eol.BK_EOL_BULLETIN_PRODUCT_TYP_CD
                          and
                               nvl(gp.EDWSF_SOURCE_DELETED_FLAG, 'N') = 'N'
                          and
                              nvl(gp.SOURCE_DELETED_FLG, 'N') = 'N'
                          and
                              nvl(eol.EDWSF_SOURCE_DELETED_FLAG, 'N') = 'N'
                          and
                              nvl(eol.SOURCE_DELETED_FLG, 'N') = 'N'
                      )
 )


SELECT
    IB.instance_id, --280
    IB.instance_number, -- 282
    --a.deal_id, --377
    ib.deal_id, --377 , 45
    nvl(cvd_line.USD_PRICE_UNIT ,cvd_line.PRICE_UNIT) as usd_prorated_list_price, --504
    nvl(cvd_line.USD_PRICE_UNIT ,cvd_line.PRICE_UNIT) * ib.QUANTITY as usd_extended_list_price, -- 505
    ib.PARENT_INSTANCE_ID, --108 309
    IB.covered_status, --219 42
    CASE  WHEN ib.covered_status = 'A' THEN 'COVERED' ELSE 'UNCOVERED' END as coverage_status,
    ib.INSTANCE_STATUS_DESC as install_base_status, --82 263
    case when ib.serial_number is null then 'F' else 'T' end as serialized_flag , --126, 602

    ib.serial_number, -- 125 , 334
    CASE
      WHEN NVL(ib.duplicate_coverage_flag, 'N') = 'N' THEN 'No'
           ELSE 'Yes'
      END as duplicate_coverage, --578 , 232
    CASE
         WHEN a.instance_status_id IN (10005, 10002, 1010041)  --Replaced-DEINSTALLED, Replace Pend-DEINSTALLED, RMA_inProgress  via : select name from EDW_SERVICE_ETL_DB.ss.CSF_CSI_INSTANCE_STATUSES where instance_status_id IN (10005, 10002, 1010041)
         THEN
                NVL(replace_ib.serial_number, replace_ib.dup_serial_number)
         ELSE
            NULL
      END as replaced_serial_number, --601 , 331
    ib.dup_serial_number, -- 490, 491
    cvd_line.maintenance_po_number, -- 492, 291
    NVL(ib.duplicate_ib_flag, 'N') as duplicate_ib_flag,  -- 50
    ib.duplicate_ib_ref_instance_id, --518, 634
    IB.item_type_flag, --88, 322
    CASE     WHEN IB.item_type_flag = 'S' THEN 'Standalone'
             WHEN IB.item_type_flag = 'P' THEN 'Parent'
             WHEN IB.item_type_flag = 'C' THEN 'Child'
             ELSE NULL
    END product_relationship, --493, 322 -- resolve to add to feed as new metric vs dynamic creation in canvas
    ib.item_name AS device_name, --85, 230
    --a.item_type,
    item.item_type, --87
    CPS_DSCI_ARCHIVE.FIX_DATES(cvd_line.END_DATE::date  )   as  product_coverage_end_date,  --52 , 403
    CPS_DSCI_ARCHIVE.FIX_DATES(cvd_line.START_DATE::date  ) as  product_coverage_start_date, -- 149 ,313
    CASE WHEN    cvd_line.STS_CODE NOT IN ('ACTIVE', 'SIGNED')
                     OR  cvd_line.STS_CODE IS NULL OR ( (cvd_line.END_DATE::date - current_date()) < 0)
        THEN  'NA (Not Eligible)'
        ELSE
            CASE WHEN datediff(day,  CURRENT_TIMESTAMP, cvd_line.END_DATE ) BETWEEN 0 AND 30    THEN 'Expiration within 30 Days (1 Month)'
                WHEN datediff(day,  CURRENT_TIMESTAMP, cvd_line.END_DATE ) BETWEEN 31 AND 60    THEN 'Expiration within 60 Days (2 Months)'
                WHEN datediff(day,  CURRENT_TIMESTAMP, cvd_line.END_DATE ) BETWEEN 61 AND 90    THEN 'Expiration within 90 Days (3 Months)'
                WHEN datediff(day,  CURRENT_TIMESTAMP, cvd_line.END_DATE )BETWEEN 91 AND 180   THEN 'Expiration within 180 Days (6 Months)'
                WHEN datediff(day,  CURRENT_TIMESTAMP, cvd_line.END_DATE ) BETWEEN 181 AND 270  THEN 'Expiration within 270 Days (9 Months)'
                WHEN datediff(day,  CURRENT_TIMESTAMP, cvd_line.END_DATE )BETWEEN 271 AND 365  THEN 'Expiration within 365 Days (12 Months)'
                WHEN datediff(day,  CURRENT_TIMESTAMP, cvd_line.END_DATE ) BETWEEN 366 AND 540  THEN 'Expiration within 540 Days (18 Months)'
                WHEN datediff(day,  CURRENT_TIMESTAMP, cvd_line.END_DATE ) BETWEEN 541 AND 730   THEN 'Expiration within 730 Days (24 Months)'
                WHEN datediff(day,  CURRENT_TIMESTAMP, cvd_line.END_DATE ) >= 731 OR cvd_line.END_DATE IS NULL  THEN 'Expiring after 2 years'
            END
  END as Coverage_Details_Months, --209, 576
CPS_DSCI_ARCHIVE.FIX_DATES(cvd_line.DATE_TERMINATED::date) as product_coverage_termination_date, --315,92
    --CPS_DSCI_ARCHIVE.FIX_DATES(a.last_date_of_support) as product_last_date_of_support_ldos,
    CPS_DSCI_ARCHIVE.FIX_DATES(item.last_date_of_support::date) as product_last_date_of_support_ldos, --89, 319
    case when item.mapped_to_service_flag = 'YES WITH SPM' then 'T' else 'F' end as  mapped_to_service_flag, --98, 293
    item.PRODUCT_FAMILY_MFG_DESCR,-- 494 , 636
    item.product_family_description, --111, 635
    item.DESCRIPTION as product_description, -- 519, 316
    item.product_family, --110 , 318
    item.ib_product_type as product_type,--60, 325
    ib.QUANTITY,
    cvd_line.PRICE_NEGOTIATED, --495  637 alt location vs nasty cte
    item.service_list_price as service_list_price_raw,--130 , 342
    item.product_list_price, --113, 320
    item.technology_group,  --156. 618
   item.business_entity_name_top as architecture, --499 , 160
   item.sub_business_entity_name_top as sub_architecture,--496 , 360
   item.BUSINESS_ENTITY_DESC_TOP as  architecture_d,--497 , 161
   item.SUB_BUSINESS_ENTITY_DESC_TOP as sub_architecture_d,--498 , 361
------------------------------------------------------------------------------------------
   --a.install_party_name,
   isite.party_name  as installed_at_customer_name, --74
   --a.install_address1, a.install_address2  as installed_at_address_lines,--500
   isite.address1 || ' ' || NVL (isite.address2, '') as installed_at_address_lines,--500, 265
   --a.install_state_province,
   isite.state as installed_at_state_province, --76
   ---a.install_city,
   isite.city as installed_at_city,--63
   --a.install_postal_code,
   isite.postal_code as installed_at_postal_code, --75
   --a.install_country,
   isite.country_name as installed_at_country,--65
   --a.install_gu_id,
   isite.gu_id as installed_at_gu_id,--68
  -- a.install_gu_name,
   isite.gu_name as installed_at_gu_name, -- 69
   isite.PARENT_PARTY_ID as install_parent_party_id, --72
   isite.PARENT_PARTY_NAME as install_parent_party_name, --73
   isite.cr_party_id as installed_at_cr_party_id, --501
   isite.cr_party_name as installed_at_cr_party_name, --502
   --a.install_at_site_use_id,
   isite.SITE_USE_ID as installed_at_site_id, -- 61



------------------------------------------------------------------------------------------
    hdr_core.BILLTO_CR_PARTY_NAME as bill_to_party_name, --26, 395
   -- a.bill_to_parent_party_id,
   --hdr_core.BILLTO_PARENT_PARTY_ID as bill_to_parent_party_id, --24
   hdr_core.BILLTO_PARENT_PARTY_ID as  contract_bid_parent_party_id, --24
   -- a.bill_to_parent_party_name,
   -- a.bill_to_site_use_id,
   hdr_core.bill_to_site_use_id as contract_bill_to_id,--27
   hdr_core.bill_to_address1 as contract_bill_to_address,
   hdr_core.bill_to_city as contract_bill_to_city,
   hdr_core.bill_to_country as contract_bill_to_country,
   hdr_core.bill_to_state_prov as contract_bill_to_province,
   hdr_core.BILL_TO_POSTAL_CODE as contract_bill_to_postal_code,


    hdr_core.contract_number, --38
    hdr_core.service_line_name as  service_level, --128
    hdr_core.contract_sts_code as contract_status , --39
    hdr_core.BILL_TO_CUSTOMER_NAME as contract_bill_to_customer_name, --33
    hdr_core.BILLTO_GU_ID as contract_billto_gu_id, --35, 575
    hdr_core.BILLTO_GU_NAME as contract_bill_to_customer_gu_name,--199, 36
    hdr_core.BILLTO_PARENT_PARTY_NAME as contract_bid_parent_party_name, -- 32, 569

   hdr_core.Coverage_template_desc as service_level_description,

hdr_core.service_brand_code as service_brand_code,
    CPS_DSCI_ARCHIVE.FIX_DATES(hdr_core.coverage_begin_date ) as service_level_start_date, --338, 606
    CPS_DSCI_ARCHIVE.FIX_DATES(hdr_core.coverage_end_date ) as service_level_end_date,    -- 339, 607

    hdr_core.contract_attribute16 as MSS_FLAG, --298, 596
    hdr_core.service_line_sts_code as service_level_status, --340, 608,
   hdr_core.billto_begeo_name as service_partner, --344, 609

   cvd_line.line_number as product_coverage_line_number,--312
   hdr_core.SERVICES_FULL_COVERAGE as SFC_FLAG, --131

   CPS_DSCI_ARCHIVE.FIX_DATES(cvd_line.LINE_CREATION_DATE  ) as sa_creation_date, --332 mce onlu
   CPS_DSCI_ARCHIVE.FIX_DATES(cvd_line.LINE_LAST_UPDATE_DATE  ) as sa_last_update_date, -- 333 moc only
------------------------------------------------------------------------------------------
    --a.ship_to_site_use_id,
    st_site.site_use_id as ship_to_site_use_id, --143
    --a.ship_to_party_name,
    st_site.party_name   as ship_to_party_name, --141
    st_site.PARTY_ID   as ship_to_party_id, --389, 616
    --a.ship_to_gu_id,
    st_site.gu_id as ship_to_gu_id, --137
    --a.ship_to_gu_name,
    st_site.gu_name as ship_to_gu_name, --138
    --a.ship_to_parent_party_id,
    st_site.PARENT_PARTY_ID as ship_to_parent_party_id, --139
    --a.ship_to_parent_party_name,
    st_site.PARENT_PARTY_NAME as ship_to_parent_party_name, --140
    -- a.ship_to_city,
    st_site.city as ship_to_city, -- 133
    --a.ship_to_state_province,
    st_site.state as ship_to_state_province, -- 145
    --a.ship_to_country,
    st_site.country_name as ship_to_country, --135
    --a.ship_to_postal_code,
    st_site.postal_code as ship_to_postal_code, --142
    st_site.address1 || ' ' || NVL (st_site.address2, '') as ship_to_address_lines,
    st_site.cr_party_name as ship_to_cr_party_name,
------------------------------------------------------------------------------------------
    bt_site.party_name  as bill_to_customer_name,
    bt_site.address1 || ' ' || NVL (bt_site.address2, '') as bill_to_address_lines,  -- 402
    bt_site.city as bill_to_city,
    bt_site.country_name as bill_to_country,
    bt_site.postal_code as bill_to_postal_code,
    bt_site.state as bill_to_state_province,
    bt_site.cr_party_id as bill_to_cr_party_id,
    bt_site.cr_party_name as bill_to_cr_party_name,

    --a.bill_to_gu_id,
    bt_site.gu_id as  bill_to_gu_id, --22, 391
    bt_site.gu_name as bill_to_gu_name, -- 23
    bt_site.site_use_id as bill_to_site_use_id, --27
------------------------------------------------------------------------------------------
    cvd_line.COVERED_LINE_ID as coverage_line_id_cpl_id, --212, 41
    cvd_line.sts_code, --151
    cvd_line.MAINTENANCE_SO_NUMBER, --96


    item.ldos_flag,--93 , 639

    CASE WHEN item.item_status_mfg = 'E.O.L.' THEN 'YES' ELSE 'NO' END as Product_End_of_Life_Flag,

    item.msa_flag, --359 ,102
    --a.service_billing_sku,
    cvd_line.MAPPED_SKU as service_billing_sku, --603-127
    -- s.contract_cxea_flag,
    hdr_core.CXEA_FLAG as  contract_cxea_flag, --37 , 638
item.business_unit,
     CASE
                 WHEN     NVL (a.c3_matched, 'N') = 'Y'
                      AND NVL (ib.duplicate_ib_flag, 'N') = 'N'
                 THEN
                    'ORIGINAL'
                 WHEN     NVL (a.c3_matched, 'N') = 'Y'
                      AND ib.duplicate_ib_flag IN ('M', 'S')
                      AND ib.instance_id = ib.duplicate_ib_ref_instance_id
                 THEN
                    'ORIGINAL'
                 WHEN     NVL (a.c3_matched, 'N') = 'Y'
                      AND ib.duplicate_ib_flag IN ('M', 'S')
                      AND ib.instance_id != ib.duplicate_ib_ref_instance_id
                THEN
                    'DUPLICATE'
                 ELSE
                    NULL
              END as duplicate_ib_flag_mce, --640
 a.collector_host_name, --190
 a.collector_matched,   -- 191
 CPS_DSCI_ARCHIVE.FIX_DATES(a.collection_date   ) as collection_date, --188
 a.customer_matched, --191
a.data_input_source, --277
 a.c3_matched found_in_cicso_db,    --254
 a.cmrc_matched found_collector_db,--253
 a.dnr_flag, --231 MCE onl;y
  CASE
     WHEN (cvd_line.STS_CODE IN ('EXPIRED', 'TERMINATED', 'OVERDUE'))
     THEN
        cvd_line.STS_CODE
     ELSE
        CASE
           WHEN datediff(day,  CURRENT_TIMESTAMP, cvd_line.END_DATE ) > 90 THEN  'Upcoming 90+ days '
           WHEN datediff(day,  CURRENT_TIMESTAMP , cvd_line.END_DATE ) BETWEEN 61 AND 90 THEN 'Upcoming 90 days'
           WHEN datediff(day,  CURRENT_TIMESTAMP , cvd_line.END_DATE ) BETWEEN 31 AND 60 THEN 'Upcoming 60 days'
           WHEN datediff(day,  CURRENT_TIMESTAMP , cvd_line.END_DATE ) BETWEEN 0  AND 30  THEN 'Upcoming 30 days'
           ELSE cvd_line.STS_CODE END
    END as contract_expired_category, --205 mce only
    CASE
         WHEN a.aligned_gu_flag = 'Y' AND ib.instance_id IS NOT NULL
         THEN
            'GU Aligned'
         WHEN a.aligned_gu_flag = 'N' AND ib.instance_id IS NOT NULL
         THEN
            'GU Not Aligned'
         ELSE
            NULL
      END as gu_aligned, -- 257 mce only
    a.confidence_level as ownership_confidence, --305 MCE only
    CASE
            WHEN     cvd_line.cvd_attribute11 IS NOT NULL  AND ib.instance_id IS NOT NULL THEN  'Y'
            ELSE
               NULL  END as EXS_Number_Flag, -- 252 mce only
    a.verification_flag, --372
    a.decomm_flag as decommission_flag, --228
    a.approved_contract_flag, -- 158
    a.approved_site_flag, --159

       CASE
                 WHEN (    NVL (a.collector_matched, 'N') = 'Y'
                       AND NVL (a.c3_matched, 'N') = 'Y'
                       AND NVL (a.customer_matched, 'N') = 'Y')
                 THEN
                    'ZONE1.3'
                 WHEN (    NVL (a.collector_matched, 'N') = 'Y'
                       AND NVL (a.c3_matched, 'N') = 'Y'
                       AND NVL (a.customer_matched, 'N') = 'N')
                 THEN
                    'ZONE1.1'
                 WHEN (    NVL (a.collector_matched, 'N') = 'Y'
                       AND NVL (a.c3_matched, 'N') = 'N'
                       AND NVL (a.customer_matched, 'N') = 'Y')
                 THEN
                    'ZONE2.1'
                 WHEN (    NVL (a.collector_matched, 'N') = 'Y'
                       AND NVL (a.c3_matched, 'N') = 'N'
                       AND NVL (a.customer_matched, 'N') = 'N')
                 THEN
                    'ZONE2.0'
                 WHEN (    NVL (a.collector_matched, 'N') = 'N'
                       AND NVL (a.c3_matched, 'N') = 'Y'
                       AND NVL (a.customer_matched, 'N') = 'Y')
                 THEN
                    'ZONE1.2'
                 WHEN (    NVL (a.collector_matched, 'N') = 'N'
                       AND NVL (a.c3_matched, 'N') = 'Y'
                       AND NVL (a.customer_matched, 'N') = 'N')
                 THEN
                    'ZONE4.0'
                 WHEN (    NVL (a.collector_matched, 'N') = 'N'
                       AND NVL (a.c3_matched, 'N') = 'N'
                       AND NVL (a.customer_matched, 'N') = 'Y')
                 THEN
                    'ZONE3.0'
              END as zone_id,
            CASE
                 WHEN (    NVL (a.collector_matched, 'N') = 'Y'
                       AND NVL (a.c3_matched, 'N') = 'Y'
                       AND NVL (a.customer_matched, 'N') = 'Y')
                 THEN
                    'If we have all three views'
                 WHEN (    NVL (a.collector_matched, 'N') = 'Y'
                       AND NVL (a.c3_matched, 'N') = 'Y'
                       AND NVL (a.customer_matched, 'N') = 'N')
                 THEN
                    'If we have only Cisco and Collector View'
                 WHEN (    NVL (a.collector_matched, 'N') = 'Y'
                       AND NVL (a.c3_matched, 'N') = 'N'
                       AND NVL (a.customer_matched, 'N') = 'Y')
                 THEN
                    'If we have collector and customer only'
                 WHEN (    NVL (a.collector_matched, 'N') = 'Y'
                       AND NVL (a.c3_matched, 'N') = 'N'
                       AND NVL (a.customer_matched, 'N') = 'N')
                 THEN
                    'Only Collector'
                 WHEN (    NVL (a.collector_matched, 'N') = 'N'
                       AND NVL (a.c3_matched, 'N') = 'Y'
                       AND NVL (a.customer_matched, 'N') = 'Y')
                 THEN
                    'If we have only Cisco and Customer View'
                 WHEN (    NVL (a.collector_matched, 'N') = 'N'
                       AND NVL (a.c3_matched, 'N') = 'Y'
                       AND NVL (a.customer_matched, 'N') = 'N')
                 THEN
                    'Only Cisco/C3'
                 WHEN (    NVL (a.collector_matched, 'N') = 'N'
                       AND NVL (a.c3_matched, 'N') = 'N'
                       AND NVL (a.customer_matched, 'N') = 'Y')
                 THEN
                    'Only customer'
              END as zone_description, --377

CASE
                 WHEN ib.instance_id IS NULL THEN NULL
                 WHEN (current_date()::date - a.cpl_end_date)         <= 30       THEN '30 Days '
                 WHEN (current_date()::date - a.cpl_end_date) BETWEEN 31 AND 60   THEN '60 Days'
                 WHEN (current_date()::date - a.cpl_end_date) BETWEEN 61 AND 90   THEN '90 Days'
                 WHEN (current_date()::date - a.cpl_end_date) BETWEEN 91 AND 180  THEN '180 Days'
                 WHEN (current_date()::date - a.cpl_end_date) BETWEEN 181 AND 365 THEN '1 Year'
                 WHEN (current_date()::date - a.cpl_end_date) BETWEEN 366 AND 730 THEN '2 Year'
                 WHEN (current_date()::date - a.cpl_end_date) BETWEEN 731 AND 1095  THEN '3 Year'
                 ELSE 'More Than 3 Years' END as renewal_category, --329

    a.exclusion_flag as excluded_asset, --249
        CASE
                 WHEN a.exclusion_flag = 'Y' AND ib.attribute26 IS NOT NULL
                 THEN 'Cisco Hybrid Cloud as-a-Service(Athena)'
                 WHEN a.exclusion_flag = 'Y' AND ib.attribute26 IS NULL
                 THEN 'User Requested Exclusion'
                 ELSE NULL
              END as exclusion_reason, --250

            a.critical_flag as critical_asset, --224

            CASE
                 WHEN (    NVL (a.c3_matched, 'N') = 'N' AND NVL (a.cmrc_matched, 'N') = 'Y')
                 THEN  item.item_name
                ELSE  NULL
              END as cisco_mfg_pid, --187

        CASE
                 WHEN (hdr.engagement_outcome = 'Smart Assists')
                 THEN
                    CASE
                       WHEN (    hdr_core.bill_to_site_use_id =
                                    hdr.covered_major_bill_to
                             AND NVL (a.last_date_of_support,current_date + 1)  > current_date
                             AND a.covered_status = 'A'
                             AND (   NVL (ib.duplicate_ib_flag, 'N') = 'N'
                                  OR (    ib.duplicate_ib_flag IN ('M', 'S')
                                      AND ib.instance_id =
                                             ib.duplicate_ib_ref_instance_id))
                             AND a.instance_status_id = 10000) -- Latest-INSTALLED
                       THEN
                          '1. Covered -Main Partner'
                       WHEN (    hdr_core.bill_to_site_use_id !=hdr.covered_major_bill_to
                             AND NVL (a.last_date_of_support,current_date + 1)  > current_date
                             AND (   NVL (ib.duplicate_ib_flag, 'N') = 'N'
                                  OR (    ib.duplicate_ib_flag IN ('M', 'S')
                                      AND ib.instance_id =
                                             ib.duplicate_ib_ref_instance_id))
                             AND a.covered_status = 'A'
                             AND a.instance_status_id = 10000) -- Latest-INSTALLED
                       THEN
                          '2. Covered - Other Partner Found'
                       WHEN (    a.covered_status IN ('I', 'N')
                             AND NVL (a.last_date_of_support,current_date + 1)  > current_date
                             AND (   NVL (ib.duplicate_ib_flag, 'N') = 'N'
                                  OR (    ib.duplicate_ib_flag IN ('M', 'S')
                                      AND ib.instance_id =
                                             ib.duplicate_ib_ref_instance_id))
                             AND a.instance_status_id = 10000) -- Latest-INSTALLED
                       THEN
                          '3. Uncovered'
                       WHEN (   NVL (a.last_date_of_support,current_date + 1)  <= current_date
                             AND (   NVL (ib.duplicate_ib_flag, 'N') = 'N'
                                  OR (    ib.duplicate_ib_flag IN ('M', 'S')
                                      AND ib.instance_id =
                                             ib.duplicate_ib_ref_instance_id))
                             AND a.instance_status_id = 10000) -- Latest-INSTALLED
                       THEN
                          '4. Past Last Date of Support'
                       WHEN (    NVL (a.c3_matched, 'N') = 'N'
                             AND NVL (a.cmrc_matched, 'N') = 'Y'
                             AND NVL (a.collector_matched, 'N') = 'Y')
                       THEN
                          '5. Not Found in C3'
                       WHEN (    ib.duplicate_ib_flag IN ('M', 'S')
                             AND ib.instance_id !=
                                    ib.duplicate_ib_ref_instance_id
                             AND a.instance_status_id = 10000) -- Latest-INSTALLED
                       THEN
                          '6. Duplicate Lines'
                       WHEN (    NVL (a.c3_matched, 'N') = 'N'
                             AND NVL (a.collector_matched, 'N') = 'Y'
                             AND NVL (a.cmrc_matched, 'N') = 'N')
                       THEN
                          '7. Unknown'
                       WHEN a.instance_status_id = 1010041   --RMA_inProgress
                       THEN
                          '8. RMA Related Status'
                       WHEN (a.instance_status_id NOT IN (10000, 1010041))  ----- Latest-INSTALLED -RMA_inProgress
                       THEN
                          '9. Not Latest Installed'
                       ELSE
                          NULL
                    END
                 ELSE
                    NULL
              END as  smart_assist_line_status_summary, --354
----------------------------------------------------------------------------------
    ib.delist_flag , --48
    --a.offer_ato_suite_description as offer_ato_suite_description_acat,-- 105
    item.DESCRIPTION as offer_ato_suite_description, -- 105
    -- a.offer_ato_suite_name as offer_ato_suite_name_acat, --106
    cvd_line.OFFER_ATO_SUITE_NAME, --106
    -- CPS_DSCI_ARCHIVE.FIX_DATES(a.ship_date) as ship_date,
    CPS_DSCI_ARCHIVE.FIX_DATES(ib.ship_date) as ship_date_header, --132, 348
    ib_prnt.instance_number as parent_instance_number, --109
    NVL (ib_prnt.serial_number, ib_prnt.dup_serial_number) as parent_serial_number, --407
    ib_prnt.inventory_item_id as parent_device_id, -- 405



    --??????????
    ib_prnt.item_name as parent_device_name, -- 404
    -- wast of resources to get this   p_item.ITEM_NAME as parent_pid,


    CASE
             WHEN IB.item_type_flag = 'C'
             THEN
                CASE
                   WHEN isite.SITE_USE_ID = ib_prnt.install_at_site_use_id
                   THEN
                      'YES'
                   ELSE
                      'NO'
                END
             ELSE
                NULL
          END as install_site_synch_in_config_flag, -- 503 , 433

        CASE
                 WHEN ib.instance_id IS NOT NULL
                 THEN
                    CASE
                       WHEN isite.site_use_created_by_module LIKE '%SVO%'
                       THEN
                          'DROP_SHIP'
                       WHEN isite.party_name LIKE '%UNKNOWN%'
                       THEN
                          'UNKNOWN'
                       WHEN (   isite.site_use_status = 'I'
                             OR isite.cust_acct_site_status = 'I'
                             OR isite.account_status = 'I')
                       THEN
                          'INACTIVE'
                       WHEN (   isite.site_use_si_flag = 'Y'
                             OR isite.cust_acct_site_si_flag = 'Y'
                             OR isite.account_si_flag = 'Y')
                       THEN
                          'ON-HOLD'
                       ELSE
                          'VALID'
                    END
                 ELSE
                    NULL
              END as installed_at_site_status, --277, 591



--    CPS_DSCI_ARCHIVE.FIX_DATES(a.last_update_date) as last_update_date, --90
        CPS_DSCI_ARCHIVE.FIX_DATES(ib.INSTANCE_LAST_UPDATE_DATE) as INSTANCE_LAST_UPDATE_DATE, --664, 665
    -- this is ship
    dsd.FISCAL_WEEK_SORTED_NAME as ship_date_fiscal_week,
    dsd.FISCAL_QTR_SORTED_NAME as ship_date_fiscal_qtr,
    dsd.FISCAL_MTH_SORTED_NAME  as ship_date_fiscal_mon,
    dsd.FISCAL_YEAR_NUMBER  as ship_date_fiscal_yr,
    dsd.CAL_WEEK_SORTED_NAME as ship_date_cal_week,
    dsd.CAL_QTR_SORTED_NAME as ship_date_cal_qtr,

    dldos.FISCAL_WEEK_SORTED_NAME as ldos_date_fiscal_week,
    dldos.FISCAL_QTR_SORTED_NAME as ldos_date_fiscal_qtr,
    dldos.FISCAL_MTH_SORTED_NAME  as ldos_date_fiscal_mon,
    dldos.FISCAL_YEAR_NUMBER  as ldos_date_fiscal_yr,
    dldos.CAL_WEEK_SORTED_NAME as ldos_date_cal_week,
    dldos.CAL_QTR_SORTED_NAME as ldos_date_cal_qtr,

    dcvd.FISCAL_WEEK_SORTED_NAME as cdv_to_date_fiscal_week,
    dcvd.FISCAL_QTR_SORTED_NAME as cdv_to_date_fiscal_qtr,
    dcvd.FISCAL_MTH_SORTED_NAME  as cdv_to_date_fiscal_mon,
    dcvd.FISCAL_YEAR_NUMBER  as cdv_to_date_fiscal_yr,
    dcvd.CAL_WEEK_SORTED_NAME as cdv_to_date_cal_week,
    dcvd.CAL_QTR_SORTED_NAME as cdv_to_date_cal_qtr,

   CASE
        WHEN cvd_line.sts_code IS NOT NULL THEN cvd_line.sts_code
             ELSE 'NEVER COVERED'
       END as product_coverage_status,


    CASE
        WHEN datediff(day,  ib.ship_date,  CURRENT_TIMESTAMP ) BETWEEN 0 AND 365 THEN 'Shipped within 1 year'
        WHEN datediff(day,  ib.ship_date,  CURRENT_TIMESTAMP ) BETWEEN 366 AND 730 THEN'Shipped within 2 year'
        WHEN datediff(day,  ib.ship_date,  CURRENT_TIMESTAMP ) BETWEEN 731 AND 1095   THEN  'Shipped within 3 year'
        WHEN datediff(day,  ib.ship_date,  CURRENT_TIMESTAMP ) BETWEEN 1096  AND 1460  THEN  'Shipped within 4 year'
        WHEN datediff(day,  ib.ship_date,  CURRENT_TIMESTAMP ) BETWEEN 1461 AND 1825  THEN 'Shipped within 5 year'
        WHEN datediff(day,  ib.ship_date,  CURRENT_TIMESTAMP ) >= 1826 OR ib.ship_date IS NULL THEN  'Shipped more than 5 year back'
        END as ship_to_category, --351, 613
    CPS_DSCI_ARCHIVE.FIX_DATES(hdr_core.contract_start_date ) as contract_start_date, --408
    CPS_DSCI_ARCHIVE.FIX_DATES(hdr_core.contract_end_date ) as contract_end_date, --204
   CASE
             WHEN datediff(day,  CURRENT_TIMESTAMP, item.last_date_of_support  ) >= 731 OR item.last_date_of_support IS NULL  THEN  'LDoS Not in 2 years'
             WHEN datediff(day,  CURRENT_TIMESTAMP, item.last_date_of_support  ) BETWEEN 541 AND 730  THEN  'Within 730 Days (24 Months)'
             WHEN datediff(day,  CURRENT_TIMESTAMP, item.last_date_of_support  ) BETWEEN 366 AND 540  THEN  'Within 540 Days (18 Months)'
             WHEN datediff(day,  CURRENT_TIMESTAMP, item.last_date_of_support  ) BETWEEN 271 AND 365  THEN  'Within 365 Days (12 Months)'
             WHEN datediff(day,  CURRENT_TIMESTAMP, item.last_date_of_support  ) BETWEEN 181 AND 270 THEN   'Within 270 Days (9 Months)'
             WHEN datediff(day,  CURRENT_TIMESTAMP, item.last_date_of_support  ) BETWEEN 91 AND 180 THEN  'Within 180 Days (6 Months)'
             WHEN datediff(day,  CURRENT_TIMESTAMP, item.last_date_of_support  ) BETWEEN 61 AND 90 THEN 'Within 90 Days (3 Months)'
             WHEN datediff(day,  CURRENT_TIMESTAMP, item.last_date_of_support  ) BETWEEN 31  AND 60 THEN  'Within 60 Days (2 Months)'
             WHEN datediff(day,  CURRENT_TIMESTAMP, item.last_date_of_support  ) BETWEEN 0  AND 30 THEN  'Within 30 Days (1 Month)'
             else 'Past LDoS'
           END as LDOS_Details_in_Months,

CASE    WHEN  item.last_date_of_support IS NULL  THEN 'LDoS Not Announced'
                  WHEN (item.last_date_of_support) < CURRENT_DATE THEN 'LDOS'
                  WHEN (item.last_date_of_support) BETWEEN CURRENT_DATE AND ADD_MONTHS ( CURRENT_DATE,12) THEN 'LDoS < 12 Mos'
				  WHEN (item.last_date_of_support) BETWEEN ADD_MONTHS (CURRENT_DATE,12) AND ADD_MONTHS (CURRENT_DATE,24) THEN  '12 Mos < LDoS < 24 Mos'
                  ELSE 'LDoS > 24 Mos'
		      END  ldos_details_months,
   hdr_core.MEU_ALLOWED_FLAG as meu_allowed_contract_flag,
       CASE
             WHEN ib.covered_status  = 'A'
             THEN CASE  WHEN     NVL (hdr_core.MEU_ALLOWED_FLAG, 'N') = 'N' AND hdr_core.CONTRACT_INSTALL_GU_COUNT > 1
                   THEN 'Y' ELSE 'N' END
             ELSE
                NULL
          END as meu_polluted_contract_flag,

       CASE
                 WHEN     ib.covered_status = 'A'  AND cvd_line.CLE_ID_RENEWED_TO IS NULL
                 THEN 'NO'
                 WHEN     ib.covered_status = 'A'AND cvd_line.CLE_ID_RENEWED_TO IS NOT NULL
                 THEN 'YES'
                 ELSE
                    NULL
              END as cpl_renewed, -- -- 641, 222

       CASE
                      WHEN     cvd_line.STS_CODE  IN
                                  ('OVERDUE', 'ACTIVE', 'SIGNED')
                           AND NVL (item.last_date_of_support,
                                    (CURRENT_DATE + 1)) > CURRENT_DATE
                           AND cvd_line.cvd_attribute14 IS NULL
                           AND NVL (item.last_date_of_support,
                                    (TO_DATE (cvd_line.END_DATE) + 1)) > cvd_line.END_DATE
                           AND cvd_line.cle_id_renewed IS NULL
                      THEN
                         'Renewable'
                      WHEN      cvd_line.STS_CODE IN ('ACTIVE', 'SIGNED')
                           AND cvd_line.cle_id_renewed IS NOT NULL
                      THEN
                         'Already Renewed'
                      WHEN      cvd_line.STS_CODE = 'EXPIRED'
                           AND NVL (item.last_date_of_support,
                                    (CURRENT_DATE + 1)) > CURRENT_DATE
                           AND NVL (item.last_date_of_support,
                                    (CURRENT_DATE + 1)) > CURRENT_DATE
                           AND cvd_line.cvd_attribute14 IS NULL
                      THEN
                         'Uncovered but Eligible'
                      WHEN     NVL (item.last_date_of_support,
                                    (CURRENT_DATE + 1)) < CURRENT_DATE
                           AND NVL (item.last_date_of_support,
                                    (TO_DATE ( cvd_line.END_DATE) + 1)) < NVL (cvd_line.END_DATE, CURRENT_DATE)
                      THEN
                         'Not Eligible'
                      WHEN cvd_line.cvd_attribute14 IS NOT NULL
                      THEN
                         'Not Eligible'
                      ELSE
                         'Not Eligible'
                   END
                      cpl_renewable, --221

    ib.so_number as product_so, --323-147
    ib.so_line_id as product_so_line_id, --632, 324
    ib.po_number as product_po, --597, 321


CPS_DSCI_ARCHIVE.FIX_DATES(p_item.last_date_of_support ) as parent_last_date_of_support,
   eol.END_OF_CHANGE_DT,
   eol.END_OF_MANUFACTURING_DT,
   eol.END_OF_NEW_SVC_ATTACHMENT_DT,
   eol.END_OF_SOFTWARE_MAINTENANCE_DT,
   eol.END_OF_ROUTINE_FAIL_ANLYSYS_DT,
   eol.END_OF_SALE_DT,
   eol.EOL_SOFTWARE_AVAILABLE_DT,
   eol.EOL_SIGNATURE_RELEASE_DT,
   eol.END_OF_SVC_CONTRACT_RNWL_DT,
   eol.END_OF_TAC_ENGG_SUPPORT_DT,
       eol.END_OF_SFTWR_LICENSE_AVAIL_DT,  -- missd this on first pass

   CPS_DSCI_ARCHIVE.FIX_DATES(item.last_date_of_service_attach) as last_date_of_service_attach, --285, 593
   CPS_DSCI_ARCHIVE.FIX_DATES(item.last_date_of_renewal) as last_date_of_renewal, -- 592, 284

item.product_list_price_gpl_us as  global_product_list_price, --255, 587
ib.WARRANTY_TYPE, -- 376, 621

 item.serviceable_product_flag,  --345 not replaicated
CPS_DSCI_ARCHIVE.FIX_DATES(ib.warranty_end_date) as warranty_end_date, -- 375, 620


CPS_DSCI_ARCHIVE.FIX_DATES(ib.instance_creation_date ) as instance_creation_date, -- 78, 279
CASE
                     WHEN IB.item_type_flag  = 'S' THEN 'Standalone'
                     WHEN IB.item_type_flag  = 'P' THEN 'Major'
                     WHEN IB.item_type_flag  = 'C' THEN 'Minor'
                     ELSE NULL
        END as Config_Type, -- 489, 195


                   org_bill.name as bill_to_id_business_entity, --564, 185
            org_ins.name as installed_at_business_entity, --590, 266


hdr.ENGAGEMENT_NAME,
CONTRACT_HEALTH_SCORE,
DEVICE_HEALTH_SCORE,
SMART_ACCOUNT_ID,
SMART_ACCOUNT_NAME,
VIRTUAL_ACCOUNT,
TRANSACTION_ID,
UPDATED_VERSION,
COLLECTOR_EXPOSURE,
ASSESSMENT_START_FLAG,
GU_DATA_FLAG,
VERIFIED_STATUS,
IBSA_KEY,
IBSA_ID,
SUMMARY_KEY,
SUMMARY_ID,
SUMMARY_WORKER_ID,
COVERAGE_SUMMARY_KEY,
COVERAGE_SUMMARY_ID,
COVERAGE_SUMMARY_WORKER_ID,
IB_KEY,
CPL_KEY,
CONFIDENCE_PRECEDENCE,
PSS_CONTRACT_FLAG,
a.RENEWAL_ELIGIBLE_FLAG,
GREATER_CHINA_FLAG,
SFC_ASSET_FLAG,
hdr.engagement_number
FROM  SERVICES_DB.SERVICES_ENT_FBV.BV_MCE_AM_ENGAGEMENT_HDR hdr
    join SERVICES_DB.SERVICES_ENT_FBV.BV_MCE_AM_ENGAGEMENT_DATA  a on
        (
        a.ENGAGEMENT_ID=hdr.ENGAGEMENT_ID
        AND
        a.operation_code IN ('I', 'U', 'N')
        )
    join EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_INSTANCE_DETAIL ib
        on ( ib.INSTANCE_ID=a.INSTANCE_ID
                 and
             nvl(ib.EDWSF_SOURCE_DELETED_FLAG,'N') = 'N'


            )
    -- THIS IS A ! OFF TTO BACK POPULATE AS BEST AS WE CAN--  FORCE THE HISTORICAL JOIN BASED ON MCE LOCKED DATA
    --left join EDW_SERVICE_ETL_DB.ss.CSF_XXCCS_DS_CVDPRDLINE_DETAIL cvd_line on
    left join CPS_DSCI_ARCHIVE.clh_mce cvd_line  on
            (
            ib.instance_id = cvd_line.instance_id
            and
            a.COVERED_LINE_ID = cvd_line.COVERED_LINE_ID
            and
            nvl(cvd_line.EDWSF_SOURCE_DELETED_FLAG,'N') = 'N'
            )

    left join EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_SAHDR_CORE hdr_core  on
        (
            cvd_line.contract_id = hdr_core.contract_id and cvd_line.service_line_id = hdr_core.service_line_id
            and
            nvl(hdr_core.EDWSF_SOURCE_DELETED_FLAG,'N') = 'N'
          )
    left join EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_SAIB_ITEMS item on
        (
        item.INVENTORY_ITEM_ID = ib.inventory_item_id
        and
        nvl(item.EDWSF_SOURCE_DELETED_FLAG,'N') = 'N'
        )
     left join EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_SITE_GU_DENORM isite on
        (
        ib.install_at_site_use_id = isite.site_use_id
        and
        nvl(isite.EDWSF_SOURCE_DELETED_FLAG,'N') = 'N'
        and          isite.site_use_code = 'SHIP_TO'
        )
    --ship_to_site_use_id -> ship tp  and          site.site_use_code = 'SHIP_TO'
    left join EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_SITE_GU_DENORM st_site on
        (
        ib.ship_to_site_use_id = st_site.site_use_id
        and
        nvl(st_site.EDWSF_SOURCE_DELETED_FLAG,'N') = 'N'
        and          st_site.site_use_code = 'SHIP_TO'
        )
    --bill_to_site_use_id -> bill to  and          site.site_use_code = 'BILL_TO'
    left join EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_SITE_GU_DENORM bt_site on
        (
        ib.bill_to_site_use_id = bt_site.site_use_id
        and
        nvl(bt_site.EDWSF_SOURCE_DELETED_FLAG,'N') = 'N'
        and          bt_site.site_use_code = 'BILL_TO'
        )
    left join EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_INSTANCE_DETAIL ib_prnt on
        (
        ib.parent_instance_id = ib_prnt.instance_id
        and
        nvl(ib_prnt.EDWSF_SOURCE_DELETED_FLAG,'N') = 'N'
        )
     left join CPS_DSCI_ARCHIVE.DIM_DATE_NEW dsd on (
        dsd.DATE=CPS_DSCI_ARCHIVE.FIX_DATES(a.ship_date)
        )
    left join CPS_DSCI_ARCHIVE.DIM_DATE_NEW dldos on (
        dldos.DATE=CPS_DSCI_ARCHIVE.FIX_DATES_W_DEF(a.LAST_DATE_OF_SUPPORT::DATE,'2150-12-31'::DATE)
        )
    left join CPS_DSCI_ARCHIVE.DIM_DATE_NEW dcvd on (
        dcvd.DATE=CPS_DSCI_ARCHIVE.FIX_DATES(cvd_line.END_DATE::DATE)
        )
   left join EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_SAIB_ITEMS p_item on
        (
        p_item.INVENTORY_ITEM_ID = ib_prnt.inventory_item_id
        and
        nvl(p_item.EDWSF_SOURCE_DELETED_FLAG,'N') = 'N')
   left join resolved_eol eol on (eol.BK_PRODUCT_ID = item.ITEM_NAME and eol.orderv = 1)
   left join EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_INSTANCE_DETAIL replace_ib on
            (
            ib.replaced_instance_id =replace_ib.instance_id
            and
            nvl(replace_ib.EDWSF_SOURCE_DELETED_FLAG,'N') = 'N'
            )
    left join EDW_SERVICE_ETL_DB.SS.CSF_HR_ALL_ORGANIZATION_UNITS org_bill on
                (
                    org_bill.organization_id = hdr_core.bill_to_org_id
                    and
                    nvl(org_bill.EDWSF_SOURCE_DELETED_FLAG,'N') = 'N'
                )
      left join EDW_SERVICE_ETL_DB.SS.CSF_HR_ALL_ORGANIZATION_UNITS org_ins on
                (
                    org_ins.organization_id = isite.site_use_org_id
                    and
                    nvl(org_ins.EDWSF_SOURCE_DELETED_FLAG,'N') = 'N'
                )
where hdr.ENGAGEMENT_NUMBER in (select ENGAGEMENT_NUMBER  from CPS_DSCI_ARCHIVE.available_engagements);
"""