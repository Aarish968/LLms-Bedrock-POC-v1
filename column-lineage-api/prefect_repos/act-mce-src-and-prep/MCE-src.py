from common import sec
from sqlalchemy import create_engine
from prefect.engine import signals
import shutil
import os
import pandas as pd
from prefect import task, unmapped, Flow, Parameter, case
import string
import random
import datetime as dt
from common import file_ops
import s3fs
import json
import boto3
import parse
import awswrangler as wr
from prefect.engine.results.s3_result import S3Result
from prefect.executors.dask import LocalDaskExecutor
from prefect.run_configs.kubernetes import KubernetesRun
from prefect.storage import Docker
from datetime import date

snowflake_db = "CPS_DB"
dn_key_name = "prd_cps_dsci_etl_svc"
schema = "CPS_DSCI_ARCHIVE"


def create_meta_json_from_df(df):
    "Takes in a df and returns a json with columns, rows and mem size"
    df_rows = df.shape[0]
    df_columns = df.shape[1]
    size = df.memory_usage(deep=True).sum()

    meta_json = {
        "rows": int(df_rows),
        "columns": int(df_columns),
        "mem_usage": int(size),
    }
    return meta_json


def write_dict_to_json_file_in_s3(dictionary, bucket, key):
    """onverts a dict to a json file and writes to an s3 bucket/key location"""
    s3 = boto3.resource("s3")
    s3object = s3.Object(bucket, key)
    s3object.put(Body=(bytes(json.dumps(dictionary).encode("UTF-8"))))


@task(log_stdout=True, tags=[f"snowflake_large"])
def extract_mce_data_large(root_parq, lst, dte_run, sf_warehouse):
    extract_mce_data(root_parq, lst, dte_run, sf_warehouse)


@task(log_stdout=True, tags=[f"snowflake_medium"])
def extract_mce_data_large(root_parq, lst, dte_run, sf_warehouse):
    extract_mce_data(root_parq, lst, dte_run, sf_warehouse)


@task(log_stdout=True, tags=[f"snowflake_small"])
def extract_mce_data_large(root_parq, lst, dte_run, sf_warehouse):
    extract_mce_data(root_parq, lst, dte_run, sf_warehouse)


@task(log_stdout=True, tags=[f"snowflake_xsmall"])
def extract_mce_data_large(root_parq, lst, dte_run, sf_warehouse):
    extract_mce_data(root_parq, lst, dte_run, sf_warehouse)


@task(
    log_stdout=True,
)
def extract_mce_core_data(root_parq, in_list, dte_run, sf_warehouse):

    engagement_num_list = []
    for i in in_list:
        engagement_num_list.append(i[0])

    print(f"Starting extract_mce_data for {engagement_num_list}")

    engine = create_engine(sec.get_sf_pw(dn_key_name, sf_warehouse, schema))

    letters = string.ascii_letters
    snowflake_temp_loc = "CPS_DSCI_ARCHIVE.mce__src_stage_{}".format(
        "".join(random.choice(letters) for i in range(10))
    )
    print(f"Temporary Snowflkae table name :  {snowflake_temp_loc}")
    con = engine.connect()
    resultsS = con.execute(
        "USE {db}.{schema}".format(db=snowflake_db, schema="CPS_DSCI_STG")
    )
    resultsW = con.execute("USE warehouse {}".format(sf_warehouse)).fetchall()
    cmd = "create or replace temporary stage {tmp_name} file_format=(TYPE = PARQUET compression=snappy)".format(
        tmp_name=snowflake_temp_loc
    )
    resultsT = pd.DataFrame(con.execute(cmd).fetchall())

    if len(in_list) > 1:
        in_list = tuple(engagement_num_list)
        cmd = f"""create table {snowflake_temp_loc} as 
    with hist_prices as (
            SELECT ed.covered_line_id,
                   MAX(cvd_line_hh.price_unit) as mx_price_unit,
                   MAX(cvd_line_hh.price_negotiated) as mx_price_negotiated
                   FROM EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_CVDPRDLINE_DETAIL_H cvd_line_hh
                   join SERVICES_DB.SERVICES_ENT_FBV.BV_MCE_AM_ENGAGEMENT_DATA  ed on
                   (cvd_line_hh.covered_line_id = ed.covered_line_id)
                   join SERVICES_DB.SERVICES_ENT_FBV.BV_MCE_AM_ENGAGEMENT_HDR hdr on
                   (ed.ENGAGEMENT_ID=hdr.ENGAGEMENT_ID)
                    where ed.covered_status = 'I'
                    and hdr.engagement_number in {in_list}
                   group by ed.covered_line_id
        ), gpl as ( -- BAD but replicaes the work MCE did  Ben J knows
                   SELECT aaa.inventory_item_id,aaa.service_line_name,
                   MAX(srvc_price.service_list_price_gpl_us) as mx_global_service_list_price,
                   max(srvc_price.PROD_BASED_SERVICE_ITEM)     as mx_service_sku,
                   max(srvc_price.DURATION) as mx_duration
                   FROM SERVICES_DB.SERVICES_ENT_FBV.BV_CPS_SAIB_ITEMS_PRICE srvc_price
                   join SERVICES_DB.SERVICES_ENT_FBV.BV_MCE_AM_ENGAGEMENT_DATA  aaa on
                    (srvc_price.inventory_item_id = aaa.inventory_item_id
                    AND
                    srvc_price.generic_service_item = aaa.service_line_name
                    )
                   join SERVICES_DB.SERVICES_ENT_FBV.BV_MCE_AM_ENGAGEMENT_HDR hdr on
                   (aaa.ENGAGEMENT_ID=hdr.ENGAGEMENT_ID)
                   where aaa.covered_status IN ('A', 'I')
                   and hdr.engagement_number in {in_list}
                   group by aaa.inventory_item_id,aaa.service_line_name
        ), eol as (
            select eol.BK_PRODUCT_ID,
             max(nvl(TO_CHAR (CPS_DSCI_ARCHIVE.FIX_DATES(TO_DATE (gp.END_OF_ROUTINE_FAIL_ANLYSYS_DT)),'yyyy-mm-dd'),null)) as END_OF_ROUTINE_FAIL_ANLYSYS_DT
            ,max(nvl(TO_CHAR (CPS_DSCI_ARCHIVE.FIX_DATES(TO_DATE (gp.END_OF_SALE_DT)),'yyyy-mm-dd'),null)) as END_OF_SALE_DT
            ,max(nvl(TO_CHAR (CPS_DSCI_ARCHIVE.FIX_DATES(TO_DATE (gp.END_OF_TAC_ENGG_SUPPORT_DT)),'yyyy-mm-dd'),null)) as END_OF_TAC_ENGG_SUPPORT_DT
            ,max(nvl(TO_CHAR (CPS_DSCI_ARCHIVE.FIX_DATES(TO_DATE (gp.END_OF_SVC_CONTRACT_RNWL_DT)),'yyyy-mm-dd'),null)) as END_OF_SVC_CONTRACT_RNWL_DT
            ,max(nvl(TO_CHAR (CPS_DSCI_ARCHIVE.FIX_DATES(TO_DATE (gp.EOL_SIGNATURE_RELEASE_DT)),'yyyy-mm-dd'),null) )as EOL_SIGNATURE_RELEASE_DT
            ,max(nvl(TO_CHAR (CPS_DSCI_ARCHIVE.FIX_DATES(TO_DATE (gp.EOL_SOFTWARE_AVAILABLE_DT)),'yyyy-mm-dd'),null)) as EOL_SOFTWARE_AVAILABLE_DT
            ,max(nvl(TO_CHAR (CPS_DSCI_ARCHIVE.FIX_DATES(TO_DATE (gp.END_OF_SOFTWARE_MAINTENANCE_DT)),'yyyy-mm-dd'),null)) as END_OF_SOFTWARE_MAINTENANCE_DT
            ,max(nvl(TO_CHAR (CPS_DSCI_ARCHIVE.FIX_DATES(TO_DATE (gp.END_OF_SFTWR_LICENSE_AVAIL_DT)),'yyyy-mm-dd'),null)) as END_OF_SFTWR_LICENSE_AVAIL_DT
        from
            SERVICES_DB.SERVICES_ENT_FBV.BV_MCE_AM_ENGAGEMENT_DATA  ed
            join SERVICES_DB.SERVICES_ENT_FBV.BV_MCE_AM_ENGAGEMENT_HDR hdr on
                   (ed.ENGAGEMENT_ID=hdr.ENGAGEMENT_ID
                       and
                    ed.covered_status = 'I'
                    )
            left join EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_SAIB_ITEMS item on
                (
                item.INVENTORY_ITEM_ID = ed.inventory_item_id
                and
                nvl(item.EDWSF_SOURCE_DELETED_FLAG,'N') = 'N'
                )
            join CPS_DB.CPS_DSCI_EBV.BV_END_OF_LIFE_PRODUCT eol on
                (
                 EOL.BK_PRODUCT_ID = ITEM.item_name
                 AND
                 nvl(eol.EDWSF_SOURCE_DELETED_FLAG, 'N') = 'N'
                )
            join CPS_DB.CPS_DSCI_EBV.BV_EOL_BULLETIN_MILESTONE_GROUP gp
              ON (
                gp.BK_END_OF_LIFE_REQUEST_NUM = eol.BK_END_OF_LIFE_REQUEST_NUM
                and
                nvl(gp.EDWSF_SOURCE_DELETED_FLAG,'N') = 'N'
                )
            where  hdr.engagement_number in {in_list}
            group by eol.BK_PRODUCT_ID
         )
    SELECT
          ib.instance_number,
          ib.instance_id,
          ib.PARENT_INSTANCE_ID,
          a.serial_number,
         CASE
               WHEN NVL(ib.duplicate_coverage_flag, 'N') = 'N' THEN 'No'
                       ELSE 'Yes'
                    END as duplicate_coverage,
          CASE
                 WHEN a.instance_status_id IN (10005, 10002, 1010041)  --Replaced-DEINSTALLED, Replace Pend-DEINSTALLED, RMA_inProgress  via : EDW_SERVICE_ETL_DB.ss.CSF_CSI_INSTANCE_STATUSES
                 THEN
                    NVL(replace_ib.serial_number, replace_ib.dup_serial_number)
                 ELSE
                    NULL
              END as replaced_serial_number,
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
              END as duplicate_ib_flag,
           item.item_name as device_name,
          item.DESCRIPTION as product_description,
           item.ib_product_type as product_type,
           item.product_family,
           item.business_entity_name_top as architecture,
           item.sub_business_entity_name_top as sub_architecture,
          item.BUSINESS_ENTITY_DESC_TOP as  architecture_d,
           item.SUB_BUSINESS_ENTITY_DESC_TOP as sub_architecture_d,
          -- duplicate with  Config_Type
          CASE     WHEN IB.item_type_flag = 'S' THEN 'Standalone'
                    WHEN IB.item_type_flag = 'P' THEN 'Parent'
                    WHEN IB.item_type_flag = 'C' THEN 'Child'
                    ELSE NULL
                   END product_relationship,
           a.collector_host_name,
           a.quantity,
           item.product_list_price,
          ib.INSTANCE_STATUS_DESC as install_base_status,
           NVL (ib.so_number, a.cmrc_so_number) as product_so,
           NVL (ib.so_line_id, a.cmrc_so_line_id)::bigint as product_so_line_id,
           ib.po_number as product_po,
           CPS_DSCI_ARCHIVE.FIX_DATES(a.ship_date) as ship_date_header,
           --a.ship_date,
           -- this seems not good
           --a.cpl_sts_code product_coverage_status,
           CASE
                 WHEN a.cpl_sts_code IS NOT NULL THEN a.cpl_sts_code
                 ELSE 'NEVER COVERED'
           END as product_coverage_status,
          CASE  WHEN a.covered_status = 'A' THEN 'COVERED' ELSE 'UNCOVERED' END as coverage_status,
           ----------------------
            CASE
                 WHEN (current_date() - a.ship_date) BETWEEN 0 AND 365
                 THEN 'Shipped within 1 year'
                 WHEN (current_date() - a.ship_date) BETWEEN 366 AND 730
                 THEN'Shipped within 2 year'
                 WHEN (current_date() - a.ship_date) BETWEEN 731 AND 1095
                 THEN  'Shipped within 3 year'
                 WHEN (current_date() - a.ship_date) BETWEEN 1096  AND 1460
                 THEN  'Shipped within 4 year'
                 WHEN (current_date() - a.ship_date) BETWEEN 1461 AND 1825
                 THEN 'Shipped within 5 year'
                 WHEN    (current_date() - a.ship_date) >= 1826 OR a.ship_date IS NULL
                 THEN  'Shipped more than 5 year back'
              END as Ship_to_Category,
             'FY' || fq_ship.BK_FISCAL_YEAR_NUMBER_INT || '-Q' || fq_ship.BK_FISCAL_QUARTER_NUMBER_INT as  Ship_Date_sortable_FQ ,
             'CY' || Year(coalesce(a.ship_date,to_date('1989-10-28') )) || '-Q' || quarter(coalesce(a.ship_date,to_date('1989-10-28') )) as  Ship_Date_sortable_Cal_Q ,
            a.install_at_site_use_id as installed_at_site_id,
            CASE
                 WHEN a.item_type_flag = 'C'
                 THEN
                    CASE
                       WHEN A.install_at_site_use_id = ib_prnt.install_at_site_use_id
                       THEN
                          'YES'
                       ELSE
                          'NO'
                    END
                 ELSE
                    NULL
              END as install_site_synch_in_config_flag,
            CASE
                 WHEN ib.instance_id IS NOT NULL
                 THEN
                    CASE
                       WHEN site.site_use_created_by_module LIKE '%SVO%'
                       THEN
                          'DROP_SHIP'
                       WHEN site.party_name LIKE '%UNKNOWN%'
                       THEN
                          'UNKNOWN'
                       WHEN (   site.site_use_status = 'I'
                             OR site.cust_acct_site_status = 'I'
                             OR site.account_status = 'I')
                       THEN
                          'INACTIVE'
                       WHEN (   site.site_use_si_flag = 'Y'
                             OR site.cust_acct_site_si_flag = 'Y'
                             OR site.account_si_flag = 'Y')
                       THEN
                          'ON-HOLD'
                       ELSE
                          'VALID'
                    END
                 ELSE
                    NULL
              END as installed_at_site_status,
           site.party_name  as installed_at_customer_name,
           site.address1 || ' ' || NVL (site.address2, '') as installed_at_address_lines,
           site.city as installed_at_city,
           site.country_name as installed_at_country,
           site.postal_code as installed_at_postal_code,
           site.state as installed_at_state_province,
           site.cr_party_id as installed_at_cr_party_id,
           site.cr_party_name as installed_at_cr_party_name,
           site.gu_id as installed_at_gu_id,
           site.gu_name as installed_at_gu_name,
           CPS_DSCI_ARCHIVE.FIX_DATES(a.last_date_of_support) as product_last_date_of_support_LDOS,
           --a.last_date_of_support as product_last_date_of_support_LDOS,
            CASE
                 WHEN (a.last_date_of_support - current_date()) >=731 OR a.last_date_of_support IS NULL  THEN  'LDoS Not in 2 years'
                 WHEN (a.last_date_of_support - current_date()) BETWEEN 541 AND 730  THEN  'Within 730 Days (24 Months)'
                 WHEN (a.last_date_of_support - current_date()) BETWEEN 366 AND 540  THEN  'Within 540 Days (18 Months)'
                 WHEN (a.last_date_of_support - current_date()) BETWEEN 271 AND 365  THEN  'Within 365 Days (12 Months)'
                 WHEN (a.last_date_of_support - current_date()) BETWEEN 181 AND 270 THEN   'Within 270 Days (9 Months)'
                 WHEN (a.last_date_of_support - current_date()) BETWEEN 91 AND 180 THEN  'Within 180 Days (6 Months)'
                 WHEN (a.last_date_of_support - current_date()) BETWEEN 61 AND 90 THEN 'Within 90 Days (3 Months)'
                 WHEN (a.last_date_of_support - current_date()) BETWEEN 31  AND 60 THEN  'Within 60 Days (2 Months)'
                 WHEN (a.last_date_of_support - current_date()) BETWEEN 0  AND 30 THEN  'Within 30 Days (1 Month)'
                 WHEN (a.last_date_of_support < current_date())THEN 'Past LDoS'
              END as LDOS_Details_in_Months,
            CASE    WHEN  a.last_date_of_support IS NULL  THEN 'LDoS Not Announced'
                      WHEN (item.last_date_of_support) < CURRENT_DATE THEN 'LDOS'
                      WHEN (item.last_date_of_support) BETWEEN CURRENT_DATE AND ADD_MONTHS ( CURRENT_DATE,12) THEN 'LDoS < 12 Mos'
                  WHEN (item.last_date_of_support) BETWEEN ADD_MONTHS (CURRENT_DATE,12) AND ADD_MONTHS (CURRENT_DATE,24) THEN  '12 Mos < LDoS < 24 Mos'
                      ELSE 'LDoS > 24 Mos'
                END  ldos_details_months,
           'FY' || fq_ldos.BK_FISCAL_YEAR_NUMBER_INT || '-Q' || fq_ldos.BK_FISCAL_QUARTER_NUMBER_INT as LDOS_in_sortable_FQ,
           'CY' || Year(coalesce(a.last_date_of_support,to_date('2040-07-27') )) || '-Q' || quarter(coalesce(a.last_date_of_support,to_date('2040-07-27') )) as  LDOS_in_sortable_cal_Q ,
           CPS_DSCI_ARCHIVE.FIX_DATES(item.last_date_of_service_attach) as last_date_of_service_attach,
           CPS_DSCI_ARCHIVE.FIX_DATES(item.last_date_of_renewal) as last_date_of_renewal,
           item.product_list_price_gpl_us as  global_product_list_price,
          CASE WHEN item.item_status_mfg = 'E.O.L.' THEN 'YES' ELSE 'NO' END as Product_End_of_Life_Flag,
           item.serviceable_product_flag,  -- not the best answer
           case when item.mapped_to_service_flag = 'YES WITH SPM' then 'Yes' else item.mapped_to_service_flag end as  mapped_to_service_flag, -- added 11-9- alanzen
          ib.WARRANTY_TYPE ,
           CPS_DSCI_ARCHIVE.FIX_DATES(ib.warranty_end_date) as warranty_end_date,
           CPS_DSCI_ARCHIVE.FIX_DATES(ib.instance_creation_date ) as instance_creation_date,
           -- CPS_DSCI_ARCHIVE.FIX_DATES(ib.instance_last_update_date ) as instance_last_update_date,
          ib.bill_to_site_use_id hardware_bill_to_name,
          CASE
                 WHEN a.item_type_flag = 'S' THEN 'Standalone'
                 WHEN a.item_type_flag = 'P' THEN 'Major'
                 WHEN a.item_type_flag = 'C' THEN 'Minor'
                 ELSE NULL
             END as Config_Type,
           ib_prnt.instance_number as parent_instance_number,
           NVL (ib_prnt.serial_number, ib_prnt.dup_serial_number) as Parent_serial_number,
           ib_prnt.inventory_item_id as parent_device_id,  -- really?
           ib_prnt.item_name as parent_device_name,
           hdr_core.contract_number,
           item.business_unit,
           a.SFC_FLAG,
           hdr_core.bill_to_site_use_id as contract_bill_to_id,
           hdr_core.bill_to_customer_name as contract_bill_to_customer_name,
           hdr_core.bill_to_address1 as contract_bill_to_address,
           hdr_core.bill_to_city as contract_bill_to_city,
           hdr_core.bill_to_country as contract_bill_to_country,
           hdr_core.bill_to_state_prov as contract_bill_to_province,
           hdr_core.BILL_TO_POSTAL_CODE as contract_bill_to_postal_code,
           hdr_core.billto_gu_name as contract_bill_to_customer_gu_name,
           CPS_DSCI_ARCHIVE.FIX_DATES(hdr_core.contract_start_date ) as contract_start_date,
           CPS_DSCI_ARCHIVE.FIX_DATES(hdr_core.contract_end_date ) as contract_end_date,
           CPS_DSCI_ARCHIVE.FIX_DATES(ib.instance_last_update_date ) as ib_last_update_date,
           hdr_core.contract_sts_code as contract_status,
           a.service_line_name as service_level,
           hdr_core.Coverage_template_desc as service_level_description,
           hdr_core.service_brand_code as service_brand_code,
          CPS_DSCI_ARCHIVE.FIX_DATES(hdr_core.coverage_begin_date ) as service_level_start_date,
          CPS_DSCI_ARCHIVE.FIX_DATES(hdr_core.coverage_end_date ) as service_level_end_date,
          hdr_core.service_line_sts_code as service_level_status,
          hdr_core.billto_begeo_name as service_partner,
    --                   (SELECT MAX (business_entity)
    --                      FROM CSF_HR_ALL_ORGANIZATION_ALL
    --                     WHERE organization_id = sahdr.bill_to_org_id),
    --                   '[^ () _0-9A-Za-z]')
    --                   "Bill-To-ID  Business Entity",
          a.covered_line_id as coverage_line_id_cpl_id,
          cvd_line.line_number as product_coverage_line_number,
          CPS_DSCI_ARCHIVE.FIX_DATES(a.cpl_start_date ) as product_coverage_start_date,
          CPS_DSCI_ARCHIVE.FIX_DATES(a.cpl_end_date ) as product_coverage_end_date,

        CASE WHEN    a.cpl_sts_code NOT IN ('ACTIVE', 'SIGNED')  OR a.cpl_sts_code IS NULL OR ( (a.cpl_end_date - current_date()) < 0)  THEN  'NA (Not Eligible)'
                ELSE
                    CASE WHEN (a.cpl_end_date - current_date()) BETWEEN 0 AND 30    THEN 'Expiration within 30 Days (1 Month)'
                        WHEN (a.cpl_end_date - current_date()) BETWEEN 31 AND 60    THEN 'Expiration within 60 Days (2 Months)'
                        WHEN (a.cpl_end_date - current_date()) BETWEEN 61 AND 90    THEN 'Expiration within 90 Days (3 Months)'
                        WHEN (a.cpl_end_date - current_date()) BETWEEN 91 AND 180   THEN 'Expiration within 180 Days (6 Months)'
                        WHEN (a.cpl_end_date - current_date()) BETWEEN 181 AND 270  THEN 'Expiration within 270 Days (9 Months)'
                        WHEN (a.cpl_end_date - current_date()) BETWEEN 271 AND 365  THEN 'Expiration within 365 Days (12 Months)'
                        WHEN (a.cpl_end_date - current_date()) BETWEEN 366 AND 540  THEN 'Expiration within 540 Days (18 Months)'
                        WHEN (a.cpl_end_date - current_date()) BETWEEN 541 AND 730   THEN 'Expiration within 730 Days (24 Months)'
                        WHEN    (a.cpl_end_date - current_date()) >= 731 OR a.cpl_end_date IS NULL  THEN 'Expiring after 2 years'
                    END
          END as Coverage_Details_Months,
           'FY' || fq_cpl_end.BK_FISCAL_YEAR_NUMBER_INT || '-Q' || fq_cpl_end.BK_FISCAL_QUARTER_NUMBER_INT as coverage_ends_sortable_FQ,
           'CY' || Year(coalesce(a.cpl_end_date,to_date('2040-07-27') )) || '-Q' || quarter(coalesce(a.cpl_end_date,to_date('2040-07-27') )) as  coverage_ends_sortable_Cal_Q ,
           'FY' || fq_cpl_start.BK_FISCAL_YEAR_NUMBER_INT || '-Q' || fq_cpl_start.BK_FISCAL_QUARTER_NUMBER_INT as coverage_starts_sortable_FQ,
          'CY' || Year(coalesce(a.cpl_start_date,to_date('2040-07-27') )) || '-Q' || quarter(coalesce(a.cpl_start_date,to_date('2040-07-27') )) as  coverage_starts_sortable_Cal_Q ,
           CPS_DSCI_ARCHIVE.FIX_DATES(a.cpl_term_date ) as product_coverage_termination_date,
           ib.covered_status,
           CASE
                 WHEN a.covered_status = 'A'
                 THEN CASE  WHEN     NVL (a.meu_allowed_flag, 'N') = 'N' AND a.contract_install_gu_count > 1
                       THEN 'Y' ELSE 'N' END
                 ELSE
                    NULL
              END as meu_polluted_contract_flag,
            CASE
                 WHEN     a.covered_status = 'A'  AND cvd_line.CLE_ID_RENEWED_TO IS NULL
                 THEN 'NO'
                 WHEN     a.covered_status = 'A'AND cvd_line.CLE_ID_RENEWED_TO IS NOT NULL
                 THEN 'YES'
                 ELSE
                    NULL
              END as cpl_renewed,
          CASE
                      WHEN     a.cpl_sts_code IN
                                  ('OVERDUE', 'ACTIVE', 'SIGNED')
                           AND NVL (item.last_date_of_support,
                                    (CURRENT_DATE + 1)) > CURRENT_DATE
                           AND cvd_line.cvd_attribute14 IS NULL
                           AND NVL (item.last_date_of_support,
                                    (TO_DATE (a.cpl_end_date) + 1)) >
                                  a.cpl_end_date
                           AND cvd_line.cle_id_renewed IS NULL
                      THEN
                         'Renewable'
                      WHEN     a.cpl_sts_code IN ('ACTIVE', 'SIGNED')
                           AND cvd_line.cle_id_renewed IS NOT NULL
                      THEN
                         'Already Renewed'
                      WHEN     a.cpl_sts_code = 'EXPIRED'
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
                                    (TO_DATE (a.cpl_end_date) + 1)) <
                                  NVL (a.CPL_END_DATE, CURRENT_DATE)
                      THEN
                         'Not Eligible'
                      WHEN cvd_line.cvd_attribute14 IS NOT NULL
                      THEN
                         'Not Eligible'
                      ELSE
                         'Not Eligible'
                   END
                      cpl_renewable,
           cvd_line.maintenance_so_number,
           cvd_line.maintenance_po_number,
           a.service_list_price as service_list_price_raw,
           hist_prices.mx_price_unit as service_list_price_d,
           hist_prices.mx_price_negotiated as service_net_price_d,
    --       (CASE
    --         WHEN ib.covered_status = 'A' THEN cvd_line.price_unit
    --                    WHEN ib.covered_status = 'I'
    --                    THEN (SELECT MAX (cvd_line_hh.price_unit)
    --                          FROM EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_CVDPRDLINE_DETAIL_H cvd_line_hh
    --                         WHERE cvd_line_hh.covered_line_id = a.covered_line_id)
    --                    ELSE
    --                       NULL
    --                 END
    --       ) as service_list_price_d,
    --      (
    --             CASE WHEN ib.covered_status = 'A'
    --                            THEN cvd_line.price_negotiated WHEN ib.covered_status = 'I'
    --                            THEN (SELECT MAX (cvd_line_hh.price_negotiated)
    --                                  FROM EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_CVDPRDLINE_DETAIL_H cvd_line_hh
    --                                 WHERE cvd_line_hh.covered_line_id = a.covered_line_id)
    --                            ELSE
    --                               NULL
    --                         END
    --               ) as service_net_price_d,
    --        CASE
    --         WHEN ib.covered_status = 'A' THEN cvd_line.price_unit
    --         WHEN ib.covered_status = 'I' THEN MAX(hist_prices.price_unit)
    --        ELSE  NULL END as service_list_price_d,
    --
    --        CASE
    --         WHEN ib.covered_status = 'A' THEN cvd_line.price_negotiated
    --         WHEN ib.covered_status = 'I' THEN MAX (hist_prices.price_negotiated)
    --        ELSE  NULL END as service_net_price_d,
    --        ----------------------------------------------------------------------------------------------
    --       CASE
    --        WHEN ib.covered_status IN ('A', 'I')  THEN MAX(sku.service_list_price_gpl_us)
    --        ELSE item.service_list_price_gpl_us
    --        END as global_service_list_price,
          ----------------------------------------------------------------------------------------------
    --
    --         (CASE
    --           WHEN ib.covered_status IN ('A', 'I')
    --           THEN
    --                      (SELECT MAX (service_list_price_gpl_us)
    --                         FROM SERVICES_DB.SERVICES_ENT_FBV.BV_CPS_SAIB_ITEMS_PRICE srvc_price
    --                        WHERE     srvc_price.inventory_item_id =
    --                                     a.inventory_item_id
    --                              AND srvc_price.generic_service_item =
    --                                     a.service_line_name)
    --                   ELSE
    --                      item.service_list_price_gpl_us
    --                END
    --            ) as global_service_list_price,
           gpl.mx_global_service_list_price,
           gpl.mx_service_sku,
           gpl.mx_duration,
          a.dnr_flag,
           a.meu_allowed_flag as meu_allowed_contract_flag,
           hdr_core.contract_attribute16 as MSS_FLAG,
           CPS_DSCI_ARCHIVE.FIX_DATES(cvd_line.LINE_CREATION_DATE  ) as sa_creation_date,
           CPS_DSCI_ARCHIVE.FIX_DATES(cvd_line.LINE_LAST_UPDATE_DATE  ) as sa_last_update_date,
           a.collector_matched,
           CPS_DSCI_ARCHIVE.FIX_DATES(a.collection_date   ) as collection_date,
           a.customer_matched,
           a.data_input_source,
           a.c3_matched found_in_cicso_db,
           a.cmrc_matched found_collector_db,
           --found_in_cisco_shipment_history where?
           CASE
                 WHEN (a.cpl_sts_code IN ('EXPIRED', 'TERMINATED', 'OVERDUE'))
                 THEN
                    a.cpl_sts_code
                 ELSE
                    CASE
                       WHEN (a.cpl_end_date - current_date()) > 90 THEN  'Upcoming 90+ days '
                       WHEN ( (a.cpl_end_date - current_date()) BETWEEN 61 AND 90) THEN 'Upcoming 90 days'
                       WHEN (a.cpl_end_date - current_date()) BETWEEN 31 AND 60 THEN 'Upcoming 60 days'
                       WHEN (a.cpl_end_date - current_date()) BETWEEN 0  AND 30  THEN 'Upcoming 30 days'
                       ELSE a.cpl_sts_code END
                END as contract_expired_category,
            CASE
                 WHEN a.aligned_gu_flag = 'Y' AND ib.instance_id IS NOT NULL
                 THEN
                    'GU Aligned'
                 WHEN a.aligned_gu_flag = 'N' AND ib.instance_id IS NOT NULL
                 THEN
                    'GU Not Aligned'
                 ELSE
                    NULL
              END as gu_aligned,
            a.confidence_level as ownership_confidence,
            CASE
                    WHEN     cvd_line.cvd_attribute11 IS NOT NULL  AND ib.instance_id IS NOT NULL THEN  'Y'
                    ELSE
                       NULL  END as EXS_Number_Flag,
           CPS_DSCI_ARCHIVE.FIX_DATES(prnt_item.last_date_of_support ) as parent_last_date_of_support,
           a.verification_flag,
           a.decomm_flag as decommission_flag,
           a.approved_contract_flag,
           a.approved_site_flag,
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
              END as zone_description,
            CASE
                 WHEN ib.instance_id IS NULL THEN NULL
                 WHEN (current_date() - a.cpl_end_date)         <= 30       THEN '30 Days '
                 WHEN (current_date() - a.cpl_end_date) BETWEEN 31 AND 60   THEN '60 Days'
                 WHEN (current_date() - a.cpl_end_date) BETWEEN 61 AND 90   THEN '90 Days'
                 WHEN (current_date() - a.cpl_end_date) BETWEEN 91 AND 180  THEN '180 Days'
                 WHEN (current_date() - a.cpl_end_date) BETWEEN 181 AND 365 THEN '1 Year'
                 WHEN (current_date() - a.cpl_end_date) BETWEEN 366 AND 730 THEN '2 Year'
                 WHEN (current_date() - a.cpl_end_date) BETWEEN 731 AND 1095  THEN '3 Year'
                 ELSE 'More Than 3 Years' END as renewal_category,
          CASE
               WHEN (TO_DATE (a.cpl_end_date) - TO_DATE (CURRENT_DATE)) >   365              THEN 'Expiration > 12 Mos'
               WHEN (TO_DATE (a.cpl_end_date) - TO_DATE (CURRENT_DATE)) BETWEEN 181  AND 365 THEN  '6 Mos < Expiration < 12 Mos'
               WHEN (TO_DATE (a.cpl_end_date) - TO_DATE (CURRENT_DATE)) BETWEEN 31   AND 180 THEN   '1 Mo < Expiration < 6 Mos'
               WHEN (TO_DATE (a.cpl_end_date) - TO_DATE (CURRENT_DATE)) BETWEEN 0    AND 30  THEN   'Expiration < 1 Mo'
               ELSE  'Expired'  END  expiration_range,
           a.exclusion_flag as excluded_asset,
            CASE
                 WHEN a.exclusion_flag = 'Y' AND ib.attribute26 IS NOT NULL
                 THEN 'Cisco Hybrid Cloud as-a-Service(Athena)'
                 WHEN a.exclusion_flag = 'Y' AND ib.attribute26 IS NULL
                 THEN 'User Requested Exclusion'
                 ELSE NULL
              END as exclusion_reason,
            a.critical_flag as critical_asset,
            CASE
                 WHEN (    NVL (a.c3_matched, 'N') = 'N' AND NVL (a.cmrc_matched, 'N') = 'Y')
                 THEN  item.item_name
                ELSE  NULL
              END as cisco_mfg_pid,
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
              END as  smart_assist_line_status_summary,
            org_bill.name as bill_to_id_business_entity,
            org_ins.name as installed_at_business_entity,
           -- columns we were missing from the query in mce_src
            ENGAGEMENT_NAME,
            ENGAGEMENT_DESCRIPTION,
            NEXT_STEP,
            CURRENT_STEP,
            CONTRACT_HEALTH_SCORE,
            DEVICE_HEALTH_SCORE,
            ENRICHMENT_COUNT,
            ENRICHMENT_STATUS,
            ENRICHMMENT_START_FLAG,
            SMART_ACCOUNT_ID,
            SMART_ACCOUNT_NAME,
            VIRTUAL_ACCOUNT,
            RANKING,
            STATUS,
            ENGAGEMENT_TYPE,
            TRANSACTION_ID,
            UPDATED_VERSION,
            TOTAL_CONTRACTS,
            TOTAL_SERVICE_PARTNERS,
            COLLECTOR_EXPOSURE,
            TOTAL_COUNTRIES,
            TOTAL_CR_PARTY_IDS,
            hdr.CREATED_BY as CREATED_BY_HEADER,
            SNAPSHOT_TYPE,
            ASSESSMENT_START_FLAG,
            ERROR_MESSAGE,
            GU_DATA_FLAG,
            SNAPSHOT_NOTE,
            SNAPSHOT_OUTCOME,
            ASSESSMENT_STATUS,
            VERIFIED_STATUS,
            THEATER_NAME,
            OWN_BY,
            hdr.LAST_UPDATED_BY,
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
            CONT_BILL_TO_SITE_USE_ID
            PREV_OWNED_FLAG,
            CONFIDENCE_PRECEDENCE,
            OWNERSHIP_TAG,
            SNAPSHOT_FLAG,
            PSS_CONTRACT_FLAG,
            COLLETOR_TOP_MOST_IB,
            COLLETOR_TOP_MOST_SN,
            a.RENEWAL_ELIGIBLE_FLAG,
            GREATER_CHINA_FLAG,
            SFC_ASSET_FLAG,
            eol.END_OF_ROUTINE_FAIL_ANLYSYS_DT,
            eol.END_OF_SALE_DT,
            eol.END_OF_TAC_ENGG_SUPPORT_DT,
            eol.END_OF_SVC_CONTRACT_RNWL_DT,
            eol.EOL_SIGNATURE_RELEASE_DT,
            eol.EOL_SOFTWARE_AVAILABLE_DT,
            eol.END_OF_SOFTWARE_MAINTENANCE_DT,
            eol.END_OF_SFTWR_LICENSE_AVAIL_DT,
            hdr.engagement_number
      FROM
        SERVICES_DB.SERVICES_ENT_FBV.BV_MCE_AM_ENGAGEMENT_HDR hdr
        join SERVICES_DB.SERVICES_ENT_FBV.BV_MCE_AM_ENGAGEMENT_DATA  a on
            (
            a.ENGAGEMENT_ID=hdr.ENGAGEMENT_ID
            AND
            a.operation_code IN ('I', 'U', 'N')
            )
        join EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_INSTANCE_DETAIL ib on
            (
            ib.INSTANCE_ID=a.INSTANCE_ID
            and
            nvl(ib.EDWSF_SOURCE_DELETED_FLAG,'N') = 'N'
            )
        join EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_SITE_GU_DENORM site on
            (
            --a.INSTALL_AT_SITE_USE_ID = site.SITE_USE_ID  -- historical join!
                                                         -- vs
            ib.install_at_site_use_id = site.site_use_id
            and
            nvl(site.EDWSF_SOURCE_DELETED_FLAG,'N') = 'N'
            and
            site.site_use_code = 'SHIP_TO'
            )
            -- so I need to fix these dates also???
        left join CPS_DSCI_EBV.BV_FISCAL_QUARTER fq_ship on
            (
               coalesce(a.ship_date,to_date('1989-10-28') )  between  fq_ship.FISCAL_QUARTER_START_DATE and fq_ship.FISCAL_QUARTER_END_DATE
            )
        left join CPS_DSCI_EBV.BV_FISCAL_QUARTER fq_ldos on
            (
               coalesce(a.last_date_of_support,to_date('2040-07-27') ) between  fq_ldos.FISCAL_QUARTER_START_DATE and fq_ldos.FISCAL_QUARTER_END_DATE
            )
        left join CPS_DSCI_EBV.BV_FISCAL_QUARTER fq_cpl_end on
            (
               coalesce(a.cpl_end_date,to_date('2040-07-27') )  between  fq_cpl_end.FISCAL_QUARTER_START_DATE and fq_cpl_end.FISCAL_QUARTER_END_DATE
                and
               a.cpl_sts_code IN ('ACTIVE', 'OVERDUE', 'SIGNED')
            )
       left join CPS_DSCI_EBV.BV_FISCAL_QUARTER fq_cpl_start on
            (
               coalesce(a.cpl_start_date,to_date('2040-07-27') )  between  fq_cpl_start.FISCAL_QUARTER_START_DATE and fq_cpl_start.FISCAL_QUARTER_END_DATE
                and a.cpl_sts_code IN ('ACTIVE', 'OVERDUE', 'SIGNED')
            )
        left join EDW_SERVICE_ETL_DB.ss.CSF_XXCCS_DS_CVDPRDLINE_DETAIL cvd_line on
            (
            a.covered_line_id=cvd_line.covered_line_id  -- diff from mine
            and
            ib.instance_id = cvd_line.instance_id
            -- not in master query:  and cvd_line.sts_code in ('ACTIVE','SIGNED','OVERDUE')
            and
            nvl(cvd_line.EDWSF_SOURCE_DELETED_FLAG,'N') = 'N'
            )
        left join EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_SAIB_ITEMS item on
            (
            item.INVENTORY_ITEM_ID = a.inventory_item_id
            and
            nvl(item.EDWSF_SOURCE_DELETED_FLAG,'N') = 'N'
            )
        left join EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_SAHDR_CORE hdr_core  on
              (
                a.contract_id = hdr_core.contract_id and a.service_line_id = hdr_core.service_line_id
                --and
                --hdr_core.CONTRACT_SCS_CODE ='SERVICE' and hdr_core.SERVICE_LINE_STATUS= 'ACTIVE'
                and
                nvl(hdr_core.EDWSF_SOURCE_DELETED_FLAG,'N') = 'N'
              )
        left join EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_INSTANCE_DETAIL replace_ib on
            (
            ib.replaced_instance_id =replace_ib.instance_id
            and
            nvl(replace_ib.EDWSF_SOURCE_DELETED_FLAG,'N') = 'N'
            )
        left join hist_prices on (
            a.covered_line_id = hist_prices.covered_line_id
            )
        left join gpl on (
            gpl.inventory_item_id = a.inventory_item_id
            AND
            gpl.service_line_name = a.service_line_name
            )
       left join EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_INSTANCE_DETAIL ib_prnt on
            (
            ib.parent_instance_id = ib_prnt.instance_id
            and
            nvl(ib_prnt.EDWSF_SOURCE_DELETED_FLAG,'N') = 'N'
            )
        left join EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_SAIB_ITEMS prnt_item on
            (
            prnt_item.INVENTORY_ITEM_ID = ib_prnt.inventory_item_id
            and
            nvl(prnt_item.EDWSF_SOURCE_DELETED_FLAG,'N') = 'N'
            )
        left join EDW_SERVICE_ETL_DB.SS.CSF_HR_ALL_ORGANIZATION_UNITS org_bill on
                (
                    org_bill.organization_id = hdr_core.bill_to_org_id
                    and
                    nvl(org_bill.EDWSF_SOURCE_DELETED_FLAG,'N') = 'N'
                )
      left join EDW_SERVICE_ETL_DB.SS.CSF_HR_ALL_ORGANIZATION_UNITS org_ins on
                (
                    org_ins.organization_id = site.site_use_org_id
                    and
                    nvl(org_ins.EDWSF_SOURCE_DELETED_FLAG,'N') = 'N'
                )
      left join eol on (EOL.BK_PRODUCT_ID = item.item_name )
    where hdr.engagement_number in {in_list}

        """

    else:  # if only 1 or 0 engagement_ids
        in_list = engagement_num_list[0]
        cmd = f"""create table {snowflake_temp_loc} as 
        with hist_prices as (
                SELECT ed.covered_line_id,
                       MAX(cvd_line_hh.price_unit) as mx_price_unit,
                       MAX(cvd_line_hh.price_negotiated) as mx_price_negotiated
                       FROM EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_CVDPRDLINE_DETAIL_H cvd_line_hh
                       join SERVICES_DB.SERVICES_ENT_FBV.BV_MCE_AM_ENGAGEMENT_DATA  ed on
                       (cvd_line_hh.covered_line_id = ed.covered_line_id)
                       join SERVICES_DB.SERVICES_ENT_FBV.BV_MCE_AM_ENGAGEMENT_HDR hdr on
                       (ed.ENGAGEMENT_ID=hdr.ENGAGEMENT_ID)
                        where ed.covered_status = 'I'
                        and hdr.engagement_number = {in_list}
                       group by ed.covered_line_id
            ), gpl as ( -- BAD but replicaes the work MCE did  Ben J knows
                       SELECT aaa.inventory_item_id,aaa.service_line_name,
                       MAX(srvc_price.service_list_price_gpl_us) as mx_global_service_list_price,
                       max(srvc_price.PROD_BASED_SERVICE_ITEM)     as mx_service_sku,
                       max(srvc_price.DURATION) as mx_duration
                       FROM SERVICES_DB.SERVICES_ENT_FBV.BV_CPS_SAIB_ITEMS_PRICE srvc_price
                       join SERVICES_DB.SERVICES_ENT_FBV.BV_MCE_AM_ENGAGEMENT_DATA  aaa on
                        (srvc_price.inventory_item_id = aaa.inventory_item_id
                        AND
                        srvc_price.generic_service_item = aaa.service_line_name
                        )
                       join SERVICES_DB.SERVICES_ENT_FBV.BV_MCE_AM_ENGAGEMENT_HDR hdr on
                       (aaa.ENGAGEMENT_ID=hdr.ENGAGEMENT_ID)
                       and hdr.engagement_number = {in_list}
                       where aaa.covered_status IN ('A', 'I')
                       group by aaa.inventory_item_id,aaa.service_line_name
            ), eol as (
                select eol.BK_PRODUCT_ID,
                 max(nvl(TO_CHAR (CPS_DSCI_ARCHIVE.FIX_DATES(TO_DATE (gp.END_OF_ROUTINE_FAIL_ANLYSYS_DT)),'yyyy-mm-dd'),null)) as END_OF_ROUTINE_FAIL_ANLYSYS_DT
                ,max(nvl(TO_CHAR (CPS_DSCI_ARCHIVE.FIX_DATES(TO_DATE (gp.END_OF_SALE_DT)),'yyyy-mm-dd'),null)) as END_OF_SALE_DT
                ,max(nvl(TO_CHAR (CPS_DSCI_ARCHIVE.FIX_DATES(TO_DATE (gp.END_OF_TAC_ENGG_SUPPORT_DT)),'yyyy-mm-dd'),null)) as END_OF_TAC_ENGG_SUPPORT_DT
                ,max(nvl(TO_CHAR (CPS_DSCI_ARCHIVE.FIX_DATES(TO_DATE (gp.END_OF_SVC_CONTRACT_RNWL_DT)),'yyyy-mm-dd'),null)) as END_OF_SVC_CONTRACT_RNWL_DT
                ,max(nvl(TO_CHAR (CPS_DSCI_ARCHIVE.FIX_DATES(TO_DATE (gp.EOL_SIGNATURE_RELEASE_DT)),'yyyy-mm-dd'),null) )as EOL_SIGNATURE_RELEASE_DT
                ,max(nvl(TO_CHAR (CPS_DSCI_ARCHIVE.FIX_DATES(TO_DATE (gp.EOL_SOFTWARE_AVAILABLE_DT)),'yyyy-mm-dd'),null)) as EOL_SOFTWARE_AVAILABLE_DT
                ,max(nvl(TO_CHAR (CPS_DSCI_ARCHIVE.FIX_DATES(TO_DATE (gp.END_OF_SOFTWARE_MAINTENANCE_DT)),'yyyy-mm-dd'),null)) as END_OF_SOFTWARE_MAINTENANCE_DT
                ,max(nvl(TO_CHAR (CPS_DSCI_ARCHIVE.FIX_DATES(TO_DATE (gp.END_OF_SFTWR_LICENSE_AVAIL_DT)),'yyyy-mm-dd'),null)) as END_OF_SFTWR_LICENSE_AVAIL_DT
            from
                SERVICES_DB.SERVICES_ENT_FBV.BV_MCE_AM_ENGAGEMENT_DATA  ed
                join SERVICES_DB.SERVICES_ENT_FBV.BV_MCE_AM_ENGAGEMENT_HDR hdr on
                       (ed.ENGAGEMENT_ID=hdr.ENGAGEMENT_ID
                           and
                        ed.covered_status = 'I'
                        )
                left join EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_SAIB_ITEMS item on
                    (
                    item.INVENTORY_ITEM_ID = ed.inventory_item_id
                    and
                    nvl(item.EDWSF_SOURCE_DELETED_FLAG,'N') = 'N'
                    )
                join CPS_DB.CPS_DSCI_EBV.BV_END_OF_LIFE_PRODUCT eol on
                    (
                     EOL.BK_PRODUCT_ID = ITEM.item_name
                     AND
                     nvl(eol.EDWSF_SOURCE_DELETED_FLAG, 'N') = 'N'
                    )
                join CPS_DB.CPS_DSCI_EBV.BV_EOL_BULLETIN_MILESTONE_GROUP gp
                  ON (
                    gp.BK_END_OF_LIFE_REQUEST_NUM = eol.BK_END_OF_LIFE_REQUEST_NUM
                    and
                    nvl(gp.EDWSF_SOURCE_DELETED_FLAG,'N') = 'N'
                    )
                where  hdr.engagement_number = {in_list}
                group by eol.BK_PRODUCT_ID
             )
        SELECT
              ib.instance_number,
              ib.instance_id,
              ib.PARENT_INSTANCE_ID,
              a.serial_number,
             CASE
                   WHEN NVL(ib.duplicate_coverage_flag, 'N') = 'N' THEN 'No'
                           ELSE 'Yes'
                        END as duplicate_coverage,
              CASE
                     WHEN a.instance_status_id IN (10005, 10002, 1010041)  --Replaced-DEINSTALLED, Replace Pend-DEINSTALLED, RMA_inProgress  via : EDW_SERVICE_ETL_DB.ss.CSF_CSI_INSTANCE_STATUSES
                     THEN
                        NVL(replace_ib.serial_number, replace_ib.dup_serial_number)
                     ELSE
                        NULL
                  END as replaced_serial_number,
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
                  END as duplicate_ib_flag,
               item.item_name as device_name,
              item.DESCRIPTION as product_description,
               item.ib_product_type as product_type,
               item.product_family,
               item.business_entity_name_top as architecture,
               item.sub_business_entity_name_top as sub_architecture,
              item.BUSINESS_ENTITY_DESC_TOP as  architecture_d,
               item.SUB_BUSINESS_ENTITY_DESC_TOP as sub_architecture_d,
              -- duplicate with  Config_Type
              CASE     WHEN IB.item_type_flag = 'S' THEN 'Standalone'
                        WHEN IB.item_type_flag = 'P' THEN 'Parent'
                        WHEN IB.item_type_flag = 'C' THEN 'Child'
                        ELSE NULL
                       END product_relationship,
               a.collector_host_name,
               a.quantity,
               item.product_list_price,
              ib.INSTANCE_STATUS_DESC as install_base_status,
               NVL (ib.so_number, a.cmrc_so_number) as product_so,
               NVL (ib.so_line_id, a.cmrc_so_line_id)::bigint as product_so_line_id,
               ib.po_number as product_po,
               CPS_DSCI_ARCHIVE.FIX_DATES(a.ship_date) as ship_date_header,
               --a.ship_date,
               -- this seems not good
               --a.cpl_sts_code product_coverage_status,
               CASE
                     WHEN a.cpl_sts_code IS NOT NULL THEN a.cpl_sts_code
                     ELSE 'NEVER COVERED'
               END as product_coverage_status,
              CASE  WHEN a.covered_status = 'A' THEN 'COVERED' ELSE 'UNCOVERED' END as coverage_status,
               ----------------------
                CASE
                     WHEN (current_date() - a.ship_date) BETWEEN 0 AND 365
                     THEN 'Shipped within 1 year'
                     WHEN (current_date() - a.ship_date) BETWEEN 366 AND 730
                     THEN'Shipped within 2 year'
                     WHEN (current_date() - a.ship_date) BETWEEN 731 AND 1095
                     THEN  'Shipped within 3 year'
                     WHEN (current_date() - a.ship_date) BETWEEN 1096  AND 1460
                     THEN  'Shipped within 4 year'
                     WHEN (current_date() - a.ship_date) BETWEEN 1461 AND 1825
                     THEN 'Shipped within 5 year'
                     WHEN    (current_date() - a.ship_date) >= 1826 OR a.ship_date IS NULL
                     THEN  'Shipped more than 5 year back'
                  END as Ship_to_Category,
                 'FY' || fq_ship.BK_FISCAL_YEAR_NUMBER_INT || '-Q' || fq_ship.BK_FISCAL_QUARTER_NUMBER_INT as  Ship_Date_sortable_FQ ,
                 'CY' || Year(coalesce(a.ship_date,to_date('1989-10-28') )) || '-Q' || quarter(coalesce(a.ship_date,to_date('1989-10-28') )) as  Ship_Date_sortable_Cal_Q ,
                a.install_at_site_use_id as installed_at_site_id,
                CASE
                     WHEN a.item_type_flag = 'C'
                     THEN
                        CASE
                           WHEN A.install_at_site_use_id = ib_prnt.install_at_site_use_id
                           THEN
                              'YES'
                           ELSE
                              'NO'
                        END
                     ELSE
                        NULL
                  END as install_site_synch_in_config_flag,
                CASE
                     WHEN ib.instance_id IS NOT NULL
                     THEN
                        CASE
                           WHEN site.site_use_created_by_module LIKE '%SVO%'
                           THEN
                              'DROP_SHIP'
                           WHEN site.party_name LIKE '%UNKNOWN%'
                           THEN
                              'UNKNOWN'
                           WHEN (   site.site_use_status = 'I'
                                 OR site.cust_acct_site_status = 'I'
                                 OR site.account_status = 'I')
                           THEN
                              'INACTIVE'
                           WHEN (   site.site_use_si_flag = 'Y'
                                 OR site.cust_acct_site_si_flag = 'Y'
                                 OR site.account_si_flag = 'Y')
                           THEN
                              'ON-HOLD'
                           ELSE
                              'VALID'
                        END
                     ELSE
                        NULL
                  END as installed_at_site_status,
               site.party_name  as installed_at_customer_name,
               site.address1 || ' ' || NVL (site.address2, '') as installed_at_address_lines,
               site.city as installed_at_city,
               site.country_name as installed_at_country,
               site.postal_code as installed_at_postal_code,
               site.state as installed_at_state_province,
               site.cr_party_id as installed_at_cr_party_id,
               site.cr_party_name as installed_at_cr_party_name,
               site.gu_id as installed_at_gu_id,
               site.gu_name as installed_at_gu_name,
               CPS_DSCI_ARCHIVE.FIX_DATES(a.last_date_of_support) as product_last_date_of_support_LDOS,
               --a.last_date_of_support as product_last_date_of_support_LDOS,
                CASE
                     WHEN (a.last_date_of_support - current_date()) >=731 OR a.last_date_of_support IS NULL  THEN  'LDoS Not in 2 years'
                     WHEN (a.last_date_of_support - current_date()) BETWEEN 541 AND 730  THEN  'Within 730 Days (24 Months)'
                     WHEN (a.last_date_of_support - current_date()) BETWEEN 366 AND 540  THEN  'Within 540 Days (18 Months)'
                     WHEN (a.last_date_of_support - current_date()) BETWEEN 271 AND 365  THEN  'Within 365 Days (12 Months)'
                     WHEN (a.last_date_of_support - current_date()) BETWEEN 181 AND 270 THEN   'Within 270 Days (9 Months)'
                     WHEN (a.last_date_of_support - current_date()) BETWEEN 91 AND 180 THEN  'Within 180 Days (6 Months)'
                     WHEN (a.last_date_of_support - current_date()) BETWEEN 61 AND 90 THEN 'Within 90 Days (3 Months)'
                     WHEN (a.last_date_of_support - current_date()) BETWEEN 31  AND 60 THEN  'Within 60 Days (2 Months)'
                     WHEN (a.last_date_of_support - current_date()) BETWEEN 0  AND 30 THEN  'Within 30 Days (1 Month)'
                     WHEN (a.last_date_of_support < current_date())THEN 'Past LDoS'
                  END as LDOS_Details_in_Months,
                CASE    WHEN  a.last_date_of_support IS NULL  THEN 'LDoS Not Announced'
                          WHEN (item.last_date_of_support) < CURRENT_DATE THEN 'LDOS'
                          WHEN (item.last_date_of_support) BETWEEN CURRENT_DATE AND ADD_MONTHS ( CURRENT_DATE,12) THEN 'LDoS < 12 Mos'
                      WHEN (item.last_date_of_support) BETWEEN ADD_MONTHS (CURRENT_DATE,12) AND ADD_MONTHS (CURRENT_DATE,24) THEN  '12 Mos < LDoS < 24 Mos'
                          ELSE 'LDoS > 24 Mos'
                    END  ldos_details_months,
               'FY' || fq_ldos.BK_FISCAL_YEAR_NUMBER_INT || '-Q' || fq_ldos.BK_FISCAL_QUARTER_NUMBER_INT as LDOS_in_sortable_FQ,
               'CY' || Year(coalesce(a.last_date_of_support,to_date('2040-07-27') )) || '-Q' || quarter(coalesce(a.last_date_of_support,to_date('2040-07-27') )) as  LDOS_in_sortable_cal_Q ,
               CPS_DSCI_ARCHIVE.FIX_DATES(item.last_date_of_service_attach) as last_date_of_service_attach,
               CPS_DSCI_ARCHIVE.FIX_DATES(item.last_date_of_renewal) as last_date_of_renewal,
               item.product_list_price_gpl_us as  global_product_list_price,
              CASE WHEN item.item_status_mfg = 'E.O.L.' THEN 'YES' ELSE 'NO' END as Product_End_of_Life_Flag,
               item.serviceable_product_flag,  -- not the best answer
               case when item.mapped_to_service_flag = 'YES WITH SPM' then 'Yes' else item.mapped_to_service_flag end as  mapped_to_service_flag, -- added 11-9- alanzen
              ib.WARRANTY_TYPE ,
               CPS_DSCI_ARCHIVE.FIX_DATES(ib.warranty_end_date) as warranty_end_date,
               CPS_DSCI_ARCHIVE.FIX_DATES(ib.instance_creation_date ) as instance_creation_date,
               -- CPS_DSCI_ARCHIVE.FIX_DATES(ib.instance_last_update_date ) as instance_last_update_date,
              ib.bill_to_site_use_id hardware_bill_to_name,
              CASE
                     WHEN a.item_type_flag = 'S' THEN 'Standalone'
                     WHEN a.item_type_flag = 'P' THEN 'Major'
                     WHEN a.item_type_flag = 'C' THEN 'Minor'
                     ELSE NULL
                 END as Config_Type,
               ib_prnt.instance_number as parent_instance_number,
               NVL (ib_prnt.serial_number, ib_prnt.dup_serial_number) as Parent_serial_number,
               ib_prnt.inventory_item_id as parent_device_id,  -- really?
               ib_prnt.item_name as parent_device_name,
               hdr_core.contract_number,
               item.business_unit,
               a.SFC_FLAG,
               hdr_core.bill_to_site_use_id as contract_bill_to_id,
               hdr_core.bill_to_customer_name as contract_bill_to_customer_name,
               hdr_core.bill_to_address1 as contract_bill_to_address,
               hdr_core.bill_to_city as contract_bill_to_city,
               hdr_core.bill_to_country as contract_bill_to_country,
               hdr_core.bill_to_state_prov as contract_bill_to_province,
               hdr_core.BILL_TO_POSTAL_CODE as contract_bill_to_postal_code,
               hdr_core.billto_gu_name as contract_bill_to_customer_gu_name,
               CPS_DSCI_ARCHIVE.FIX_DATES(hdr_core.contract_start_date ) as contract_start_date,
               CPS_DSCI_ARCHIVE.FIX_DATES(hdr_core.contract_end_date ) as contract_end_date,
               CPS_DSCI_ARCHIVE.FIX_DATES(ib.instance_last_update_date ) as ib_last_update_date,
               hdr_core.contract_sts_code as contract_status,
               a.service_line_name as service_level,
               hdr_core.Coverage_template_desc as service_level_description,
               hdr_core.service_brand_code as service_brand_code,
              CPS_DSCI_ARCHIVE.FIX_DATES(hdr_core.coverage_begin_date ) as service_level_start_date,
              CPS_DSCI_ARCHIVE.FIX_DATES(hdr_core.coverage_end_date ) as service_level_end_date,
              hdr_core.service_line_sts_code as service_level_status,
              hdr_core.billto_begeo_name as service_partner,
        --                   (SELECT MAX (business_entity)
        --                      FROM CSF_HR_ALL_ORGANIZATION_ALL
        --                     WHERE organization_id = sahdr.bill_to_org_id),
        --                   '[^ () _0-9A-Za-z]')
        --                   "Bill-To-ID  Business Entity",
              a.covered_line_id as coverage_line_id_cpl_id,
              cvd_line.line_number as product_coverage_line_number,
              CPS_DSCI_ARCHIVE.FIX_DATES(a.cpl_start_date ) as product_coverage_start_date,
              CPS_DSCI_ARCHIVE.FIX_DATES(a.cpl_end_date ) as product_coverage_end_date,

            CASE WHEN    a.cpl_sts_code NOT IN ('ACTIVE', 'SIGNED')  OR a.cpl_sts_code IS NULL OR ( (a.cpl_end_date - current_date()) < 0)  THEN  'NA (Not Eligible)'
                    ELSE
                        CASE WHEN (a.cpl_end_date - current_date()) BETWEEN 0 AND 30    THEN 'Expiration within 30 Days (1 Month)'
                            WHEN (a.cpl_end_date - current_date()) BETWEEN 31 AND 60    THEN 'Expiration within 60 Days (2 Months)'
                            WHEN (a.cpl_end_date - current_date()) BETWEEN 61 AND 90    THEN 'Expiration within 90 Days (3 Months)'
                            WHEN (a.cpl_end_date - current_date()) BETWEEN 91 AND 180   THEN 'Expiration within 180 Days (6 Months)'
                            WHEN (a.cpl_end_date - current_date()) BETWEEN 181 AND 270  THEN 'Expiration within 270 Days (9 Months)'
                            WHEN (a.cpl_end_date - current_date()) BETWEEN 271 AND 365  THEN 'Expiration within 365 Days (12 Months)'
                            WHEN (a.cpl_end_date - current_date()) BETWEEN 366 AND 540  THEN 'Expiration within 540 Days (18 Months)'
                            WHEN (a.cpl_end_date - current_date()) BETWEEN 541 AND 730   THEN 'Expiration within 730 Days (24 Months)'
                            WHEN    (a.cpl_end_date - current_date()) >= 731 OR a.cpl_end_date IS NULL  THEN 'Expiring after 2 years'
                        END
              END as Coverage_Details_Months,
               'FY' || fq_cpl_end.BK_FISCAL_YEAR_NUMBER_INT || '-Q' || fq_cpl_end.BK_FISCAL_QUARTER_NUMBER_INT as coverage_ends_sortable_FQ,
               'CY' || Year(coalesce(a.cpl_end_date,to_date('2040-07-27') )) || '-Q' || quarter(coalesce(a.cpl_end_date,to_date('2040-07-27') )) as  coverage_ends_sortable_Cal_Q ,
               'FY' || fq_cpl_start.BK_FISCAL_YEAR_NUMBER_INT || '-Q' || fq_cpl_start.BK_FISCAL_QUARTER_NUMBER_INT as coverage_starts_sortable_FQ,
              'CY' || Year(coalesce(a.cpl_start_date,to_date('2040-07-27') )) || '-Q' || quarter(coalesce(a.cpl_start_date,to_date('2040-07-27') )) as  coverage_starts_sortable_Cal_Q ,
               CPS_DSCI_ARCHIVE.FIX_DATES(a.cpl_term_date ) as product_coverage_termination_date,
               ib.covered_status,
               CASE
                     WHEN a.covered_status = 'A'
                     THEN CASE  WHEN     NVL (a.meu_allowed_flag, 'N') = 'N' AND a.contract_install_gu_count > 1
                           THEN 'Y' ELSE 'N' END
                     ELSE
                        NULL
                  END as meu_polluted_contract_flag,
                CASE
                     WHEN     a.covered_status = 'A'  AND cvd_line.CLE_ID_RENEWED_TO IS NULL
                     THEN 'NO'
                     WHEN     a.covered_status = 'A'AND cvd_line.CLE_ID_RENEWED_TO IS NOT NULL
                     THEN 'YES'
                     ELSE
                        NULL
                  END as cpl_renewed,
              CASE
                          WHEN     a.cpl_sts_code IN
                                      ('OVERDUE', 'ACTIVE', 'SIGNED')
                               AND NVL (item.last_date_of_support,
                                        (CURRENT_DATE + 1)) > CURRENT_DATE
                               AND cvd_line.cvd_attribute14 IS NULL
                               AND NVL (item.last_date_of_support,
                                        (TO_DATE (a.cpl_end_date) + 1)) >
                                      a.cpl_end_date
                               AND cvd_line.cle_id_renewed IS NULL
                          THEN
                             'Renewable'
                          WHEN     a.cpl_sts_code IN ('ACTIVE', 'SIGNED')
                               AND cvd_line.cle_id_renewed IS NOT NULL
                          THEN
                             'Already Renewed'
                          WHEN     a.cpl_sts_code = 'EXPIRED'
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
                                        (TO_DATE (a.cpl_end_date) + 1)) <
                                      NVL (a.CPL_END_DATE, CURRENT_DATE)
                          THEN
                             'Not Eligible'
                          WHEN cvd_line.cvd_attribute14 IS NOT NULL
                          THEN
                             'Not Eligible'
                          ELSE
                             'Not Eligible'
                       END
                          cpl_renewable,
               cvd_line.maintenance_so_number,
               cvd_line.maintenance_po_number,
               a.service_list_price as service_list_price_raw,
               hist_prices.mx_price_unit as service_list_price_d,
               hist_prices.mx_price_negotiated as service_net_price_d,
        --       (CASE
        --         WHEN ib.covered_status = 'A' THEN cvd_line.price_unit
        --                    WHEN ib.covered_status = 'I'
        --                    THEN (SELECT MAX (cvd_line_hh.price_unit)
        --                          FROM EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_CVDPRDLINE_DETAIL_H cvd_line_hh
        --                         WHERE cvd_line_hh.covered_line_id = a.covered_line_id)
        --                    ELSE
        --                       NULL
        --                 END
        --       ) as service_list_price_d,
        --      (
        --             CASE WHEN ib.covered_status = 'A'
        --                            THEN cvd_line.price_negotiated WHEN ib.covered_status = 'I'
        --                            THEN (SELECT MAX (cvd_line_hh.price_negotiated)
        --                                  FROM EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_CVDPRDLINE_DETAIL_H cvd_line_hh
        --                                 WHERE cvd_line_hh.covered_line_id = a.covered_line_id)
        --                            ELSE
        --                               NULL
        --                         END
        --               ) as service_net_price_d,
        --        CASE
        --         WHEN ib.covered_status = 'A' THEN cvd_line.price_unit
        --         WHEN ib.covered_status = 'I' THEN MAX(hist_prices.price_unit)
        --        ELSE  NULL END as service_list_price_d,
        --
        --        CASE
        --         WHEN ib.covered_status = 'A' THEN cvd_line.price_negotiated
        --         WHEN ib.covered_status = 'I' THEN MAX (hist_prices.price_negotiated)
        --        ELSE  NULL END as service_net_price_d,
        --        ----------------------------------------------------------------------------------------------
        --       CASE
        --        WHEN ib.covered_status IN ('A', 'I')  THEN MAX(sku.service_list_price_gpl_us)
        --        ELSE item.service_list_price_gpl_us
        --        END as global_service_list_price,
              ----------------------------------------------------------------------------------------------
        --
        --         (CASE
        --           WHEN ib.covered_status IN ('A', 'I')
        --           THEN
        --                      (SELECT MAX (service_list_price_gpl_us)
        --                         FROM SERVICES_DB.SERVICES_ENT_FBV.BV_CPS_SAIB_ITEMS_PRICE srvc_price
        --                        WHERE     srvc_price.inventory_item_id =
        --                                     a.inventory_item_id
        --                              AND srvc_price.generic_service_item =
        --                                     a.service_line_name)
        --                   ELSE
        --                      item.service_list_price_gpl_us
        --                END
        --            ) as global_service_list_price,
               gpl.mx_global_service_list_price,
               gpl.mx_service_sku,
               gpl.mx_duration,
              a.dnr_flag,
               a.meu_allowed_flag as meu_allowed_contract_flag,
               hdr_core.contract_attribute16 as MSS_FLAG,
               CPS_DSCI_ARCHIVE.FIX_DATES(cvd_line.LINE_CREATION_DATE  ) as sa_creation_date,
               CPS_DSCI_ARCHIVE.FIX_DATES(cvd_line.LINE_LAST_UPDATE_DATE  ) as sa_last_update_date,
               a.collector_matched,
               CPS_DSCI_ARCHIVE.FIX_DATES(a.collection_date   ) as collection_date,
               a.customer_matched,
               a.data_input_source,
               a.c3_matched found_in_cicso_db,
               a.cmrc_matched found_collector_db,
               --found_in_cisco_shipment_history where?
               CASE
                     WHEN (a.cpl_sts_code IN ('EXPIRED', 'TERMINATED', 'OVERDUE'))
                     THEN
                        a.cpl_sts_code
                     ELSE
                        CASE
                           WHEN (a.cpl_end_date - current_date()) > 90 THEN  'Upcoming 90+ days '
                           WHEN ( (a.cpl_end_date - current_date()) BETWEEN 61 AND 90) THEN 'Upcoming 90 days'
                           WHEN (a.cpl_end_date - current_date()) BETWEEN 31 AND 60 THEN 'Upcoming 60 days'
                           WHEN (a.cpl_end_date - current_date()) BETWEEN 0  AND 30  THEN 'Upcoming 30 days'
                           ELSE a.cpl_sts_code END
                    END as contract_expired_category,
                CASE
                     WHEN a.aligned_gu_flag = 'Y' AND ib.instance_id IS NOT NULL
                     THEN
                        'GU Aligned'
                     WHEN a.aligned_gu_flag = 'N' AND ib.instance_id IS NOT NULL
                     THEN
                        'GU Not Aligned'
                     ELSE
                        NULL
                  END as gu_aligned,
                a.confidence_level as ownership_confidence,
                CASE
                        WHEN     cvd_line.cvd_attribute11 IS NOT NULL  AND ib.instance_id IS NOT NULL THEN  'Y'
                        ELSE
                           NULL  END as EXS_Number_Flag,
               CPS_DSCI_ARCHIVE.FIX_DATES(prnt_item.last_date_of_support ) as parent_last_date_of_support,
               a.verification_flag,
               a.decomm_flag as decommission_flag,
               a.approved_contract_flag,
               a.approved_site_flag,
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
                  END as zone_description,
                CASE
                     WHEN ib.instance_id IS NULL THEN NULL
                     WHEN (current_date() - a.cpl_end_date)         <= 30       THEN '30 Days '
                     WHEN (current_date() - a.cpl_end_date) BETWEEN 31 AND 60   THEN '60 Days'
                     WHEN (current_date() - a.cpl_end_date) BETWEEN 61 AND 90   THEN '90 Days'
                     WHEN (current_date() - a.cpl_end_date) BETWEEN 91 AND 180  THEN '180 Days'
                     WHEN (current_date() - a.cpl_end_date) BETWEEN 181 AND 365 THEN '1 Year'
                     WHEN (current_date() - a.cpl_end_date) BETWEEN 366 AND 730 THEN '2 Year'
                     WHEN (current_date() - a.cpl_end_date) BETWEEN 731 AND 1095  THEN '3 Year'
                     ELSE 'More Than 3 Years' END as renewal_category,
              CASE
                   WHEN (TO_DATE (a.cpl_end_date) - TO_DATE (CURRENT_DATE)) >   365              THEN 'Expiration > 12 Mos'
                   WHEN (TO_DATE (a.cpl_end_date) - TO_DATE (CURRENT_DATE)) BETWEEN 181  AND 365 THEN  '6 Mos < Expiration < 12 Mos'
                   WHEN (TO_DATE (a.cpl_end_date) - TO_DATE (CURRENT_DATE)) BETWEEN 31   AND 180 THEN   '1 Mo < Expiration < 6 Mos'
                   WHEN (TO_DATE (a.cpl_end_date) - TO_DATE (CURRENT_DATE)) BETWEEN 0    AND 30  THEN   'Expiration < 1 Mo'
                   ELSE  'Expired'  END  expiration_range,
               a.exclusion_flag as excluded_asset,
                CASE
                     WHEN a.exclusion_flag = 'Y' AND ib.attribute26 IS NOT NULL
                     THEN 'Cisco Hybrid Cloud as-a-Service(Athena)'
                     WHEN a.exclusion_flag = 'Y' AND ib.attribute26 IS NULL
                     THEN 'User Requested Exclusion'
                     ELSE NULL
                  END as exclusion_reason,
                a.critical_flag as critical_asset,
                CASE
                     WHEN (    NVL (a.c3_matched, 'N') = 'N' AND NVL (a.cmrc_matched, 'N') = 'Y')
                     THEN  item.item_name
                    ELSE  NULL
                  END as cisco_mfg_pid,
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
                  END as  smart_assist_line_status_summary,
                org_bill.name as bill_to_id_business_entity,
                org_ins.name as installed_at_business_entity,
               -- columns we were missing from the query in mce_src
                ENGAGEMENT_NAME,
                ENGAGEMENT_DESCRIPTION,
                NEXT_STEP,
                CURRENT_STEP,
                CONTRACT_HEALTH_SCORE,
                DEVICE_HEALTH_SCORE,
                ENRICHMENT_COUNT,
                ENRICHMENT_STATUS,
                ENRICHMMENT_START_FLAG,
                SMART_ACCOUNT_ID,
                SMART_ACCOUNT_NAME,
                VIRTUAL_ACCOUNT,
                RANKING,
                STATUS,
                ENGAGEMENT_TYPE,
                TRANSACTION_ID,
                UPDATED_VERSION,
                TOTAL_CONTRACTS,
                TOTAL_SERVICE_PARTNERS,
                COLLECTOR_EXPOSURE,
                TOTAL_COUNTRIES,
                TOTAL_CR_PARTY_IDS,
                hdr.CREATED_BY as CREATED_BY_HEADER,
                SNAPSHOT_TYPE,
                ASSESSMENT_START_FLAG,
                ERROR_MESSAGE,
                GU_DATA_FLAG,
                SNAPSHOT_NOTE,
                SNAPSHOT_OUTCOME,
                ASSESSMENT_STATUS,
                VERIFIED_STATUS,
                THEATER_NAME,
                OWN_BY,
                hdr.LAST_UPDATED_BY,
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
                CONT_BILL_TO_SITE_USE_ID
                PREV_OWNED_FLAG,
                CONFIDENCE_PRECEDENCE,
                OWNERSHIP_TAG,
                SNAPSHOT_FLAG,
                PSS_CONTRACT_FLAG,
                COLLETOR_TOP_MOST_IB,
                COLLETOR_TOP_MOST_SN,
                a.RENEWAL_ELIGIBLE_FLAG,
                GREATER_CHINA_FLAG,
                SFC_ASSET_FLAG,
                eol.END_OF_ROUTINE_FAIL_ANLYSYS_DT,
                eol.END_OF_SALE_DT,
                eol.END_OF_TAC_ENGG_SUPPORT_DT,
                eol.END_OF_SVC_CONTRACT_RNWL_DT,
                eol.EOL_SIGNATURE_RELEASE_DT,
                eol.EOL_SOFTWARE_AVAILABLE_DT,
                eol.END_OF_SOFTWARE_MAINTENANCE_DT,
                eol.END_OF_SFTWR_LICENSE_AVAIL_DT,
                hdr.engagement_number
          FROM
            SERVICES_DB.SERVICES_ENT_FBV.BV_MCE_AM_ENGAGEMENT_HDR hdr
            join SERVICES_DB.SERVICES_ENT_FBV.BV_MCE_AM_ENGAGEMENT_DATA  a on
                (
                a.ENGAGEMENT_ID=hdr.ENGAGEMENT_ID
                AND
                a.operation_code IN ('I', 'U', 'N')
                )
            join EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_INSTANCE_DETAIL ib on
                (
                ib.INSTANCE_ID=a.INSTANCE_ID
                and
                nvl(ib.EDWSF_SOURCE_DELETED_FLAG,'N') = 'N'
                )
            join EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_SITE_GU_DENORM site on
                (
                --a.INSTALL_AT_SITE_USE_ID = site.SITE_USE_ID  -- historical join!
                                                             -- vs
                ib.install_at_site_use_id = site.site_use_id
                and
                nvl(site.EDWSF_SOURCE_DELETED_FLAG,'N') = 'N'
                and
                site.site_use_code = 'SHIP_TO'
                )
                -- so I need to fix these dates also???
            left join CPS_DSCI_EBV.BV_FISCAL_QUARTER fq_ship on
                (
                   coalesce(a.ship_date,to_date('1989-10-28') )  between  fq_ship.FISCAL_QUARTER_START_DATE and fq_ship.FISCAL_QUARTER_END_DATE
                )
            left join CPS_DSCI_EBV.BV_FISCAL_QUARTER fq_ldos on
                (
                   coalesce(a.last_date_of_support,to_date('2040-07-27') ) between  fq_ldos.FISCAL_QUARTER_START_DATE and fq_ldos.FISCAL_QUARTER_END_DATE
                )
            left join CPS_DSCI_EBV.BV_FISCAL_QUARTER fq_cpl_end on
                (
                   coalesce(a.cpl_end_date,to_date('2040-07-27') )  between  fq_cpl_end.FISCAL_QUARTER_START_DATE and fq_cpl_end.FISCAL_QUARTER_END_DATE
                    and
                   a.cpl_sts_code IN ('ACTIVE', 'OVERDUE', 'SIGNED')
                )
           left join CPS_DSCI_EBV.BV_FISCAL_QUARTER fq_cpl_start on
                (
                   coalesce(a.cpl_start_date,to_date('2040-07-27') )  between  fq_cpl_start.FISCAL_QUARTER_START_DATE and fq_cpl_start.FISCAL_QUARTER_END_DATE
                    and a.cpl_sts_code IN ('ACTIVE', 'OVERDUE', 'SIGNED')
                )
            left join EDW_SERVICE_ETL_DB.ss.CSF_XXCCS_DS_CVDPRDLINE_DETAIL cvd_line on
                (
                a.covered_line_id=cvd_line.covered_line_id  -- diff from mine
                and
                ib.instance_id = cvd_line.instance_id
                -- not in master query:  and cvd_line.sts_code in ('ACTIVE','SIGNED','OVERDUE')
                and
                nvl(cvd_line.EDWSF_SOURCE_DELETED_FLAG,'N') = 'N'
                )
            left join EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_SAIB_ITEMS item on
                (
                item.INVENTORY_ITEM_ID = a.inventory_item_id
                and
                nvl(item.EDWSF_SOURCE_DELETED_FLAG,'N') = 'N'
                )
            left join EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_SAHDR_CORE hdr_core  on
                  (
                    a.contract_id = hdr_core.contract_id and a.service_line_id = hdr_core.service_line_id
                    --and
                    --hdr_core.CONTRACT_SCS_CODE ='SERVICE' and hdr_core.SERVICE_LINE_STATUS= 'ACTIVE'
                    and
                    nvl(hdr_core.EDWSF_SOURCE_DELETED_FLAG,'N') = 'N'
                  )
            left join EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_INSTANCE_DETAIL replace_ib on
                (
                ib.replaced_instance_id =replace_ib.instance_id
                and
                nvl(replace_ib.EDWSF_SOURCE_DELETED_FLAG,'N') = 'N'
                )
            left join hist_prices on (
                a.covered_line_id = hist_prices.covered_line_id
                )
            left join gpl on (
                gpl.inventory_item_id = a.inventory_item_id
                AND
                gpl.service_line_name = a.service_line_name
                )
           left join EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_INSTANCE_DETAIL ib_prnt on
                (
                ib.parent_instance_id = ib_prnt.instance_id
                and
                nvl(ib_prnt.EDWSF_SOURCE_DELETED_FLAG,'N') = 'N'
                )
            left join EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_SAIB_ITEMS prnt_item on
                (
                prnt_item.INVENTORY_ITEM_ID = ib_prnt.inventory_item_id
                and
                nvl(prnt_item.EDWSF_SOURCE_DELETED_FLAG,'N') = 'N'
                )
            left join EDW_SERVICE_ETL_DB.SS.CSF_HR_ALL_ORGANIZATION_UNITS org_bill on
                    (
                        org_bill.organization_id = hdr_core.bill_to_org_id
                        and
                        nvl(org_bill.EDWSF_SOURCE_DELETED_FLAG,'N') = 'N'
                    )
          left join EDW_SERVICE_ETL_DB.SS.CSF_HR_ALL_ORGANIZATION_UNITS org_ins on
                    (
                        org_ins.organization_id = site.site_use_org_id
                        and
                        nvl(org_ins.EDWSF_SOURCE_DELETED_FLAG,'N') = 'N'
                    )
          left join eol on (EOL.BK_PRODUCT_ID = item.item_name )
        where hdr.engagement_number = {in_list}

            """

    con.execute(cmd)

    con.close()
    engine.dispose()
    return snowflake_temp_loc


def extract_mce_data(root_parq, lst, dte_run, sf_warehouse, core_src_table):
    en = lst[0]
    print(f"Starting extract_mce_data for {en}")
    sql_insert_df = lst[1].to_frame().T

    engine = create_engine(sec.get_sf_pw(dn_key_name, sf_warehouse, schema))

    letters = string.ascii_letters
    snowflake_temp_loc = "culvert_stage_{}".format(
        "".join(random.choice(letters) for i in range(10))
    )

    con = engine.connect()
    resultsS = con.execute(
        "USE {db}.{schema}".format(db=snowflake_db, schema="CPS_DSCI_STG")
    )
    resultsW = con.execute("USE warehouse {}".format(sf_warehouse)).fetchall()
    cmd = "create or replace temporary stage {tmp_name} file_format=(TYPE = PARQUET compression=snappy)".format(
        tmp_name=snowflake_temp_loc
    )
    resultsT = pd.DataFrame(con.execute(cmd).fetchall())

    cmd = """copy into @{tmp_name}/ from ( select * from  {core_src_table} where engagement_number = {engagement_number}
         )
          file_format = (type = 'parquet')
          header = true;
        """.format(
        tmp_name=snowflake_temp_loc, engagement_number=en, core_src_table=core_src_table
    )


    todays_date = date.today()
    this_year = todays_date.year

    resultsQ = pd.DataFrame(con.execute(cmd).fetchall())
    print(f"finshed querying Snowflake for {en}")
    s3_path = f"""s3://canvas-data-store-dev/MCE_FILES/{this_year}/{en}/{dte_run}/{en}/""" #TODO switch 2022 fir current_year() code
    sql_insert_df["file_location"] = s3_path

    result = parse.search("s3://{bucket}/", s3_path)
    bucket = result["bucket"]

    meta_json = create_meta_json_from_df(resultsQ)
    write_dict_to_json_file_in_s3(
        meta_json, bucket, f"""MCE_FILES/{this_year}/{en}/{dte_run}/meta.json"""
    )
    # failed_path = 's3://canvas-data-store-dev/MCE_FILES/2022/1615217214843/2022-03-22_16_00_48/1615217214843/'
    # working_path = 's3://canvas-data-store-dev/MCE_FILES/2022/1599241924892/2022-02-11_12_17_32/1599241924892/'

    print(f"Starting BL for  {en}")

    letters = string.ascii_letters
    stage_name = "/tmp/culvert_stage_{}".format(
        "".join(random.choice(letters) for i in range(10))
    )
    shutil.rmtree(stage_name, ignore_errors=True)
    os.mkdir(stage_name)

    this_fn_data_out_path = file_ops.prep_data_location(
        os.path.join(stage_name, str(en)), clear_contents=False
    )

    try:
        con.execute(
            "GET @{tmp_name} file://{landing_folder}".format(
                tmp_name=snowflake_temp_loc, landing_folder=this_fn_data_out_path
            )
        )

        s3 = s3fs.S3FileSystem()
        for i in os.listdir(os.path.join(stage_name, en)):
            print("directory")
            print(os.listdir(stage_name))
            print("i in directory")
            print(i)

            local_file = os.path.join(stage_name, en, i)
            print("local file")
            print(local_file)
            wr.s3.upload(local_file=local_file, path=f"{s3_path}{i}")

        print(f"""wrote parquet to {s3_path}""")

        print(
            "returning this param from this_fn_data_output_path : {}".format(
                this_fn_data_out_path
            )
        )

        print(f"Completed BL for  {en}")
        sql_insert_df.to_sql(
            "MCE_ENGAGEMENT_TRACKING_META",
            con=con,
            schema=schema,
            if_exists="append",
            index=False,
        )

        print(f"Completed write to Meta table for  {en}")
    except:
        print(f"There was no data vailable for {en}")

    print(
        "Updating CPS_DSCI_ARCHIVE.SOURCING_LOCK  to complete this MCE sourcing batch..."
    )
    update_lock_table = f""" update CPS_DSCI_ARCHIVE.SOURCING_LOCK set CURRENTLY_PROCESSING ='' where SRC_TYPE = 'MCE' """
    print(update_lock_table)
    con.execute(update_lock_table)
    print("CPS_DSCI_ARCHIVE.SOURCING_LOCK has been updated to '' for MCE... ")

    con.close()
    engine.dispose()
    shutil.rmtree(stage_name, ignore_errors=True)


@task(log_stdout=True, tags=["snowflake_large"])
def extract_mce_data_large(root_parq, lst, dte_run, sf_warehouse, snowflake_temp_loc):
    print(f"Using extract_mce_data_large to process sourcing...")
    extract_mce_data(root_parq, lst, dte_run, sf_warehouse, snowflake_temp_loc)


@task(log_stdout=True, tags=["snowflake_medium"])
def extract_mce_data_medium(root_parq, lst, dte_run, sf_warehouse, snowflake_temp_loc):
    print(f"Using extract_mce_data_medium to process sourcing...")
    extract_mce_data(root_parq, lst, dte_run, sf_warehouse, snowflake_temp_loc)


@task(log_stdout=True, tags=["snowflake_small"])
def extract_mce_data_small(root_parq, lst, dte_run, sf_warehouse, snowflake_temp_loc):
    print(f"Using extract_mce_data_small to process sourcing...")
    extract_mce_data(root_parq, lst, dte_run, sf_warehouse, snowflake_temp_loc)


@task(log_stdout=True, tags=["snowflake_xsmall"])
def extract_mce_data_xsmall(root_parq, lst, dte_run, sf_warehouse, snowflake_temp_loc):
    print(f"Using extract_mce_data_xsmall to process sourcing...")
    extract_mce_data(root_parq, lst, dte_run, sf_warehouse, snowflake_temp_loc)


def fix_cols(df):
    cols = []
    for c in df.columns:
        cl = c.lower()
        cols.append(cl.strip().replace(" ", "_").replace("/", "_").replace("\\", "_"))
    return cols


@task(log_stdout=True, nout = 2, tags=["snowflake_small"])
def check_engagements(bucket, ext, in_list, sf_warehouse, own_by_list):
    dte_folder = str(dt.datetime.today().strftime("%Y-%m-%d_%H_%M_%S"))
    engine = create_engine(sec.get_sf_pw(dn_key_name, sf_warehouse, schema))

    con = engine.connect()
    con.execute("alter session set lock_timeout = 3600;")
    print("checking if MCE-src is already running....")
    check_acat_lock = (
        f""" select * from CPS_DSCI_ARCHIVE.SOURCING_LOCK where SRC_TYPE = 'MCE' """
    )
    lock_df = pd.read_sql(check_acat_lock, engine)

    print(lock_df["currently_processing"][0])

    if lock_df["currently_processing"][0] == "processing":
        raise signals.SKIP(
            "MCE is already running, please wait for it to complete before starting another sourcing batch.."
        )
    else:
        print(
            "MCE is not currently running , updating lock to begin this sourcing batch..."
        )
        update_lock_table = f""" update CPS_DSCI_ARCHIVE.SOURCING_LOCK set CURRENTLY_PROCESSING ='processing' where SRC_TYPE = 'MCE' """
        print(update_lock_table)
        con.execute(update_lock_table)
        print(
            "CPS_DSCI_ARCHIVE.SOURCING_LOCK has been updated to 'processing' for MCE... "
        )

    if own_by_list:
        own_by_list = tuple(own_by_list)
    else:
        own_by_list = (
            "(select distinct CAMCECID from CPS_BIA_BR.DATA_CANVAS_ENGAGEMENT_HDR_V h)"
        )

    if in_list:
        print(f"using in_list {in_list}")
        in_list = tuple(in_list)

        look_sql = f"""with last_sourced_engagement as (  -- stuf we have already sourced
      select distinct to_char(m.LAST_UPDATED_DATE, 'DD-MM-YYYY HH:MM:SS')  as src_date, m.ENGAGEMENT_NUMBER
      from CPS_DB.CPS_DSCI_ARCHIVE.MCE_ENGAGEMENT_TRACKING_META m
        ), max_date_refresh as (  --max reasonable data look out for a big future date
            select max(m.LAST_UPDATED_DATE) as max_dte
            from CPS_DB.CPS_DSCI_ARCHIVE.MCE_ENGAGEMENT_TRACKING_META m
            where m.LAST_UPDATED_DATE < current_date + 2
        )    select h.ENGAGEMENT_ID,
                   ENGAGEMENT_NUMBER,
                   ENGAGEMENT_NAME,
                   ENGAGEMENT_DESCRIPTION,
                   NEXT_STEP,
                   CURRENT_STEP,
                   CONTRACT_HEALTH_SCORE,
                   DEVICE_HEALTH_SCORE,
                   ENGAGEMENT_OUTCOME,
                   ENRICHMENT_COUNT,
                   ENRICHMENT_STATUS,
                   ENRICHMENT_DATE,
                   ENRICHMMENT_START_FLAG,
                   SMART_ACCOUNT_ID,
                   SMART_ACCOUNT_NAME,
                   VIRTUAL_ACCOUNT,
                   RANKING,
                   STATUS,
                   ENGAGEMENT_TYPE,
                   TRANSACTION_ID,
                   UPDATED_VERSION,
                   TOTAL_CONTRACTS,
                   TOTAL_SERVICE_PARTNERS,
                   COLLECTOR_EXPOSURE,
                   TOTAL_COUNTRIES,
                   TOTAL_CR_PARTY_IDS,
                   CREATION_DATE,
                   CREATED_BY,
                   LAST_UPDATED_DATE,
                   LAST_UPDATED_BY,
                   SNAPSHOT_DATE,
                   SNAPSHOT_TYPE,
                   ASSESSMENT_START_FLAG,
                   ERROR_MESSAGE,
                   GU_DATA_FLAG,
                   SNAPSHOT_NOTE,
                   SNAPSHOT_OUTCOME,
                   ASSESSMENT_STATUS,
                   VERIFIED_STATUS,
                   THEATER_NAME,
                   COVERED_MAJOR_BILL_TO,
                   '{dte_folder}' as DATE_BUCKET,
                   '' as FILE_LOCATION
            from SERVICES_DB.SERVICES_ENT_FBV.BV_MCE_AM_ENGAGEMENT_HDR H
            cross join max_date_refresh
            where ENGAGEMENT_OUTCOME = 'Asset Management'
            and H.LAST_UPDATED_DATE > DATEADD( day, -360, max_date_refresh.max_dte )  -- look back a window
            and OWN_BY in {own_by_list}
            and  ENGAGEMENT_NUMBER in {in_list}-- we dont already have it
                )"""








    else:
        look_sql = f"""with last_sourced_engagement as (  -- stuf we have already sourced
  select distinct to_char(m.LAST_UPDATED_DATE, 'DD-MM-YYYY HH:MM:SS')  as src_date, m.ENGAGEMENT_NUMBER
  from CPS_DB.CPS_DSCI_ARCHIVE.MCE_ENGAGEMENT_TRACKING_META m
        ), max_date_refresh as (  --max reasonable data look out for a big future date
            select max(m.LAST_UPDATED_DATE) as max_dte
            from CPS_DB.CPS_DSCI_ARCHIVE.MCE_ENGAGEMENT_TRACKING_META m
            where m.LAST_UPDATED_DATE < current_date + 2
        )    select h.ENGAGEMENT_ID,
                   ENGAGEMENT_NUMBER,
                   ENGAGEMENT_NAME,
                   ENGAGEMENT_DESCRIPTION,
                   NEXT_STEP,
                   CURRENT_STEP,
                   CONTRACT_HEALTH_SCORE,
                   DEVICE_HEALTH_SCORE,
                   ENGAGEMENT_OUTCOME,
                   ENRICHMENT_COUNT,
                   ENRICHMENT_STATUS,
                   ENRICHMENT_DATE,
                   ENRICHMMENT_START_FLAG,
                   SMART_ACCOUNT_ID,
                   SMART_ACCOUNT_NAME,
                   VIRTUAL_ACCOUNT,
                   RANKING,
                   STATUS,
                   ENGAGEMENT_TYPE,
                   TRANSACTION_ID,
                   UPDATED_VERSION,
                   TOTAL_CONTRACTS,
                   TOTAL_SERVICE_PARTNERS,
                   COLLECTOR_EXPOSURE,
                   TOTAL_COUNTRIES,
                   TOTAL_CR_PARTY_IDS,
                   CREATION_DATE,
                   CREATED_BY,
                   LAST_UPDATED_DATE,
                   LAST_UPDATED_BY,
                   SNAPSHOT_DATE,
                   SNAPSHOT_TYPE,
                   ASSESSMENT_START_FLAG,
                   ERROR_MESSAGE,
                   GU_DATA_FLAG,
                   SNAPSHOT_NOTE,
                   SNAPSHOT_OUTCOME,
                   ASSESSMENT_STATUS,
                   VERIFIED_STATUS,
                   THEATER_NAME,
                   COVERED_MAJOR_BILL_TO,
                   '{dte_folder}' as DATE_BUCKET,
                   '' as FILE_LOCATION
            from SERVICES_DB.SERVICES_ENT_FBV.BV_MCE_AM_ENGAGEMENT_HDR H
            cross join max_date_refresh
            where ENGAGEMENT_OUTCOME = 'Asset Management'
            and H.LAST_UPDATED_DATE > DATEADD( day, -360, max_date_refresh.max_dte )  -- look back a window
              and OWN_BY in (select distinct CAMCECID from CPS_BIA_BR.DATA_CANVAS_ENGAGEMENT_HDR_V h)
            and  -- we dont already have it
            not exists (select 1
                          from last_sourced_engagement lse
                          where lse.ENGAGEMENT_NUMBER = H.ENGAGEMENT_NUMBER
                            and lse.src_date = to_char(H.LAST_UPDATED_DATE, 'DD-MM-YYYY HH:MM:SS')
                )"""

    resultsT = pd.read_sql(look_sql, con)

    if resultsT.empty:
        print("There are currently no MCE engagements to source...")
        print(
            "Updating CPS_DSCI_ARCHIVE.SOURCING_LOCK  to complete this MCE sourcing batch..."
        )
        update_lock_table = f""" update CPS_DSCI_ARCHIVE.SOURCING_LOCK set CURRENTLY_PROCESSING ='' where SRC_TYPE = 'MCE' """
        print(update_lock_table)
        con.execute(update_lock_table)
        print("CPS_DSCI_ARCHIVE.SOURCING_LOCK has been updated to '' for MCE... ")
        con.close()
        raise signals.SKIP("Exiting this run ...")

    con.close()

    new_mce = resultsT["engagement_number"]
    lst = []
    for index, row in resultsT.iterrows():
        lst.append((row.engagement_number, row))

    return list(lst), dte_folder

@task()
def drop_core_table(core_table,sf_warehouse):
    engine = create_engine(sec.get_sf_pw(dn_key_name, sf_warehouse, schema))

    con = engine.connect()
    con.execute(f"""drop table {core_table};""")
    print(f"""Dropped core sourcing table : {core_table}""")

    return core_table




@task()
def get_sf_warehous(sf_wh_size):
    if sf_wh_size == "xsmall":
        sf_warehouse = "CPS_DSCI_ETL_EXT1_WH"  # X-Small
    elif sf_wh_size == "small":
        sf_warehouse = "CPS_DSCI_ETL_EXT2_WH"  # Small
    elif sf_wh_size == "med":
        sf_warehouse = "cps_dsci_etl_wh"  # Medium
    elif sf_wh_size == "large":
        sf_warehouse = "CPS_DSCI_ETL_EXT3_WH"
    else:
        sf_warehouse = "CPS_DSCI_ETL_EXT2_WH"  # Small
        print(
            "Defualting to Snowflake warehouse of sf_warehouse, please use one of the following options in the future : xsmall, small, med, large "
        )
    print(f"Utilizing the {sf_warehouse} Snowflake warehouse ...")
    return sf_warehouse


storage_obj = Docker(
    base_image="prefecthq/prefect:0.15.3-python3.8",
    python_dependencies=[
        "pandas==1.1.3",
        "awswrangler==2.10.0",
        "numpy==1.19.2",
        "elasticsearch==7.14.0",
        "boto3==1.18.16",
        "aiohttp",
        "hvac",
        "snowflake-sqlalchemy==1.2.4",
        "s3fs==0.4",
        "hvac>0.11.0",
        "SQLAlchemy==1.3.20",
        "awswrangler>2.10.0",
        "fastparquet>0.7.1",
        "XlsxWriter>3.0.1",
        "parse>1.19.0",
        "gcsfs",
    ],
    registry_url="837578041534.dkr.ecr.us-east-1.amazonaws.com/flows",
    files={
        """/Users/ejurotic/PycharmProjects/act-mce-src-and-prep/common/file_ops.py""": "/root/.prefect/flows/common/file_ops.py",
        """/Users/ejurotic/PycharmProjects/act-mce-src-and-prep/common/sec.py""": "/root/.prefect/flows/common/sec.py",
    },
    env_vars={"PYTHONPATH": "${PYTHONPATH}:/root/.prefect/flows/"},
)

with Flow(
    "mce_src",
    storage=storage_obj,
    run_config=KubernetesRun(),
    executor=LocalDaskExecutor(scheduler="processes", num_workers=5),
    result=S3Result(bucket="cam-prefect-results"),
) as flow:
    in_list = Parameter("in_list", default=[])
    own_by_list = Parameter("own_by_list", default=[])
    sf_wh_size = Parameter("sf_wh_size", default="med")
    sf_warehouse = get_sf_warehous(sf_wh_size)
    bucket = "s3://canvas-data-store-dev/MCE_FILES/"
    ext = ".parquet"
    root_parq = "/mnt/newmt/ERP/home/alanzen/MCE_FILES"
    etr, date_folder = check_engagements(
        bucket, ext, in_list, sf_warehouse, own_by_list
    )
    core_table = extract_mce_core_data(root_parq, etr, date_folder, sf_warehouse)
    with case(sf_wh_size, "xsmall"):
        process_files_xsmall = extract_mce_data_xsmall.map(
            root_parq=unmapped("/mnt/newmt/ERP/home/alanzen/MCE_FILES"),
            lst=etr,
            dte_run=unmapped(date_folder),
            sf_warehouse=unmapped(sf_warehouse),
            snowflake_temp_loc=unmapped(core_table),
        )
        drop_core_table = drop_core_table(core_table, sf_warehouse, upstream_tasks=[process_files_xsmall])
    with case(sf_wh_size, "small"):
        process_files_small = extract_mce_data_small.map(
            root_parq=unmapped("/mnt/newmt/ERP/home/alanzen/MCE_FILES"),
            lst=etr,
            dte_run=unmapped(date_folder),
            sf_warehouse=unmapped(sf_warehouse),
            snowflake_temp_loc=unmapped(core_table),
        )
        drop_core_table = drop_core_table(core_table, sf_warehouse, upstream_tasks=[process_files_small])
    with case(sf_wh_size, "med"):
        process_files_med = extract_mce_data_medium.map(
            root_parq=unmapped("/mnt/newmt/ERP/home/alanzen/MCE_FILES"),
            lst=etr,
            dte_run=unmapped(date_folder),
            sf_warehouse=unmapped(sf_warehouse),
            snowflake_temp_loc=unmapped(core_table),
        )
        drop_core_table = drop_core_table(core_table, sf_warehouse, upstream_tasks=[process_files_med])
    with case(sf_wh_size, "large"):
        process_files_large = extract_mce_data_large.map(
            root_parq=unmapped("/mnt/newmt/ERP/home/alanzen/MCE_FILES"),
            lst=etr,
            dte_run=unmapped(date_folder),
            sf_warehouse=unmapped(sf_warehouse),
            snowflake_temp_loc=unmapped(core_table),
        )
        drop_core_table = drop_core_table(core_table, sf_warehouse, upstream_tasks=[process_files_large])


# EXEC_ADDRESS = "tcp://172.18.138.27:41096"  #http://172.18.138.27:8087/notebooks/erp_cloud/large_cluster_creator_and_destroyer.ipynb#
# executor = DaskExecutor(address=EXEC_ADDRESS)
# executor = DaskExecutor(cluster_kwargs={
#     "n_workers": 20,
#     "host": "127.0.0.1",
#     "scheduler_port": 64822,
#     "dashboard_address": ":8786",
#     "memory_limit": "200G",
#     "threads_per_worker": 5,
# })
if __name__ == "__main__":
    flow.run(parameters=dict(sf_wh_size="med", own_by_list=["john", "eric"]))
