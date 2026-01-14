def get_managed_qry(eng_id):
    managed_query = f"""
   select hdr.contract_number,
    case when hdr.contract_number in (select replace(c.value::string,' ','') as CONTRACT_NUMBER from CPS_BIA_BR.DATA_CANVAS_CONTRACT_DATA_V ,lateral flatten(input=>split(CONTRACT_NUMBER, ',')) c where ID = concat('CAM-',{eng_id}))
    then 'managed' else 'not-managed' end as CAM_MANAGED,
    hdr.VENDOR_ORGANIZATION_ID as contract_org_id ,
    hdr.VENDOR_ORGANIZATION_NAME,
    site.site_use_id as install_site_id,
    xx.customer_name,
    case
            when  site.SITE_USE_ORG_ID  = 112	    then 'NETHERLANDS Operating'
            when  site.SITE_USE_ORG_ID  = 147	    then 'CISCO KOREA OPERATING UNIT'
            when  site.SITE_USE_ORG_ID  = 161	    then 'CISCO US OPERATING UNIT'
            when  site.SITE_USE_ORG_ID  = 163	    then 'CISCO CANADA OPERATING UNIT'
            when  site.SITE_USE_ORG_ID  = 165	    then 'CISCO AUSTRALIA OPERATING UNIT'
            when  site.SITE_USE_ORG_ID  = 167	    then 'CISCO JAPAN OPERATING UNIT'
            when  site.SITE_USE_ORG_ID  = 2290    then 'CISCO BRAZIL CA OPERATING UNIT'
            when  site.SITE_USE_ORG_ID  = 2293	then 'CISCO SOUTH AFRICA CA OPERATING UNIT'
            when  site.SITE_USE_ORG_ID  = 2363	then 'CISCO UK HOME OPERATING UNIT'
            when  site.SITE_USE_ORG_ID  = 3563	then 'CISCO IN CCIPL OPERATING UNIT'
            when  site.SITE_USE_ORG_ID  = 3765	then 'CISCO ITALY SRL OPERATING UNIT'
            when  site.SITE_USE_ORG_ID  = 4245	then 'CISCO CHINA HANGZHOU OPERATING UNIT'
            when  site.SITE_USE_ORG_ID  = 4345	then 'CISCO RUSSIA SOL OPERATING UNIT'
            when  site.SITE_USE_ORG_ID  = 4365	then 'CISCO GERMANY TECH OPERATING UNIT'
            else 'i dont know'
    end as site_Business_Entity,
    site.GU_ID  ,
    site.GU_NAME,
    site.party_id as customer_id,
    site.party_site_id  as address_id,
    site.SITE_USE_ORG_ID as install_site_org_id,
    coalesce(site.ADDRESS1,'') || coalesce(site.ADDRESS2,'') || coalesce(site.ADDRESS3,'') || coalesce(site.ADDRESS4,'') as address4,
    site.CITY, site.STATE, site.POSTAL_CODE , site.COUNTRY_CODE_ISO,
    coalesce(site.CR_ADDRESS1,'') || coalesce(site.CR_ADDRESS2,'') || coalesce(site.CR_ADDRESS3,'') || coalesce(site.CR_ADDRESS4,'') as cr_adddress4,
    site.cr_CITY, site.cr_STATE, site.cr_POSTAL_CODE , site.CR_COUNTRY,
    case when INS.covered_status='N' then 'ib_count_never_covered'
         when INS.covered_status='I' then 'ib_count_uncovered'
         when INS.covered_status='A' then 'ib_count_covered'
         ELSE 'UNKNOWN' END AS covered_status,
    case when  NVL(ACCOUNT_SI_FLAG,'N')='N' AND  NVL(SITE_USE_SI_FLAG,'N')='N' AND NVL(CUST_ACCT_SITE_SI_FLAG,'N')='N' THEN 'NO HOLDS' else 'ON HOLD' end has_holds,
     nvl(site.site_use_id ,-1) as site_use_id , INS.instance_id, INS.INSTANCE_NUMBER,ins.PARENT_INSTANCE_ID,
             INS.IMMEDIATE_PARENT_INSTANCE_ID, ins.REPLACED_INSTANCE_ID,ins.BILL_TO_PARTY_ID,
              ins.ERP_LIST_PRICE, ins.ERP_SELLING_PRICE,ins.ITEM_NAME as PID, ins.ATTRIBUTE3 serial_qq,
            ins.INVENTORY_ITEM_ID,
           ins.LOCATION_ID, ins.DEAL_ID,ins.SHIP_DATE,
           ins.warranty_end_date  as warranty_end_date,
           datediff(days, ins.SHIP_DATE,current_timestamp()) as ship_date_days_prior,
           case
               when coverage.LAST_DATE_OF_RENEWAL is null then datediff(days, current_timestamp(),nvl(coverage.LAST_DATE_OF_RENEWAL,'2100-01-01 12:00:00'::timestamp) )
               else datediff(days, current_timestamp(),coverage.LAST_DATE_OF_RENEWAL ) end as days_left_to_renew,
           datediff(days, nvl(ins.warranty_end_date ,current_timestamp()) ,current_timestamp()) as days_left_on_warranty,
               site.cr_party_id,
               site.party_id,
               site.party_site_id,
               case when covered_status = 'N' then 'ib_count_never_covered'
                    when covered_status = 'I' then  'ib_count_uncovered'
                    when covered_status = 'A' then 'ib_count_covered'
                    else 'not_classified' end as coverage,
               case when  site.site_use_id is null then 'Y' else 'N' end as is_null_site,
               case when nvl(site.site_use_id ,-1) < 0 then 'Y' else  'N' end as invalid_site_number,
           ins.PO_NUMBER, ins.SO_NUMBER,ins.QUANTITY,
           coverage.PRODUCT_FAMILY, coverage.ITEM_NAME, coverage.MAPPED_TO_SERVICE_FLAG, coverage.BUSINESS_UNIT,
           nvl(coverage.LAST_DATE_OF_RENEWAL,'2100-01-01 12:00:00'::timestamp) as LAST_DATE_OF_RENEWAL,
           nvl(coverage.LAST_DATE_OF_SERVICE_ATTACH,'2100-01-01 12:00:00'::timestamp) as LAST_DATE_OF_SERVICE_ATTACH,
           nvl(coverage.LAST_DATE_OF_SUPPORT,'2100-01-01 12:00:00'::timestamp) as  LAST_DATE_OF_SUPPORT ,
           coverage.CATALOG_PRODUCT_TYPE,
           case when coverage.MAPPED_TO_SERVICE_FLAG ='YES WITH SPM'
                and current_timestamp() < nvl(coverage.LAST_DATE_OF_SUPPORT,'2100-01-01 12:00:00'::timestamp)
               then 'YES' else 'NO' end as is_spm_coverable,
           case when coverage.SERVICEABLE_PRODUCT_FLAG ='Y'
                and current_timestamp() < nvl(coverage.LAST_DATE_OF_SUPPORT,'2100-01-01 12:00:00'::timestamp)
               then 'YES' else 'NO' end as is_servicable,
           coverage.ITEM_TYPE,
            coverage.SERVICEABLE_PRODUCT_FLAG

    from "EDW_SERVICE_ETL_DB"."SS"."CSF_XXCCS_DS_SITE_GU_DENORM" site
        join "EDW_SERVICE_ETL_DB"."SS"."CSF_XXCCS_DS_INSTANCE_DETAIL" ins  on (site.site_use_id = ins.INSTALL_AT_SITE_USE_ID )
        JOIN EDW_MASTER_ETL_DB.SS.CRT_XXNGCR_SOURCE_IMAGE xx
           on (site.cr_party_id = xx.party_id
           and site.party_id::varchar = xx.customer_id
           and site.party_site_id::varchar = xx.address_id)
        left join EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_SAIB_ITEMS coverage
            on (coverage.INVENTORY_ITEM_ID = ins.INVENTORY_ITEM_ID  and nvl(coverage.EDWSF_SOURCE_DELETED_FLAG,'N') = 'N')
        left join "EDW_SERVICE_ETL_DB"."SS"."CSF_XXCCS_DS_CVDPRDLINE_DETAIL" cvd
            on (ins.instance_id = cvd.instance_id)
        left join "EDW_SERVICE_ETL_DB"."SS"."CSF_XXCCS_DS_SAHDR_CORE" hdr
            on ( cvd.contract_id = hdr.contract_id and cvd.service_line_id = hdr.service_line_id)
            where 1=1
              --and hdr.CONTRACT_SCS_CODE ='SERVICE'
              --and hdr.SERVICE_LINE_STATUS= 'ACTIVE'
              and site.site_use_code = 'SHIP_TO'
              --and cvd.sts_code in ('ACTIVE','SIGNED','OVERDUE')
              --and nvl(cvd.EDWSF_SOURCE_DELETED_FLAG,'N')='N'
              and nvl(ins.EDWSF_SOURCE_DELETED_FLAG,'N')='N'
              and site.GU_ID in (with sub as (
                                select uid, GUID  from CPS_DSCI_ARCHIVE.RPT_PARTIES where UID = {eng_id} -- DC enagagement Number
                            ) select distinct nvl(nvl(GLOBAL_ULTIMATE_ID, PARENT_PARTY_ID),PARTY_ID) as real_gu_ids
                            from EDW_MASTER_ETL_DB.SS.CRT_XXNGCR_DNM_RELATIONSHIPS r join sub on (r.PARTY_ID = sub.GUID));"""

    return managed_query