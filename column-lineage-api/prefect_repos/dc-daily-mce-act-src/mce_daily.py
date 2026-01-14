# All parts in a flow

import io
import json
import math
import os
import random
import shutil
import tempfile
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Generator, List
import string
import shutil
import random
import awswrangler as wr
import boto3
import numpy as np
import pandas as pd
import prefect.executors
import xlsxwriter
from prefect import Flow, Parameter, task
from prefect.tasks.aws.s3 import S3Upload
from sqlalchemy import create_engine
import s3fs
from common import data_types
from common import new_bulkload as bl
from common import sec
import psutil
from prefect import unmapped

temp_base_location = "/tmp"
from prefect.engine.results.s3_result import S3Result
from prefect.executors.dask import LocalDaskExecutor
from prefect.run_configs.kubernetes import KubernetesRun
from prefect.storage import Docker


def dict_back(tags):
    print(json.loads(tags))
    return json.loads(tags)


def get_values_in_str(tag_dict, superset_tags):
    v = ""
    if len(tag_dict) > 0:
        for k in superset_tags:
            v += f"{tag_dict.get(k, '')},"
    return v[:-1]


@task(log_stdout=True)
def clean_working_space(working_space: str):
    # sort fo safe must start with /tmp
    # horrid  code
    if working_space.startswith(temp_base_location):
        shutil.rmtree(working_space, ignore_errors=False, onerror=None)


def create_working_space():
    ws = os.path.join(temp_base_location, bl.gen_temp_stage_name())
    Path(ws).mkdir(parents=True, exist_ok=False)
    return ws


def split_dataframe(df, chunk_size=10000):
    chunks = list()
    num_chunks = math.ceil(len(df) / chunk_size)
    for i in range(num_chunks):
        chunks.append(df[i * chunk_size: (i + 1) * chunk_size])
    return chunks


pd.set_option("display.max_columns", 400)
pd.set_option("display.max_rows", 400)
pd.set_option('display.max_colwidth', 4000)


def contract_match(contract, ref_lst):
    ans = 'no_match'
    test_contract = -10
    try:
        test_contract = int(contract)
    except:
        pass
    for ref in ref_lst:
        if test_contract in ref[1]:
            ans = ref[0]
    return ans


def test_contract_list(lst, ref_lst, existing_flag):
    if existing_flag == 'no_match':
        split_contract = lst.split(' --> ')
        testable = []
        ans = 'no_match'

        if len(split_contract) == 1:  # did we get at least 2
            try:
                testable.append(int(lst))
            except:
                pass
        elif len(split_contract) > 1:
            for e in split_contract:  # we got possibly 1
                try:
                    testable.append(int(e))
                except:
                    pass

        for ref in ref_lst:  # each contract type
            for contract in testable:  # contracts i need to review in each contract set
                if contract in ref[1]:
                    ans = ref[0]
                    return ans
        return ans
    else:
        return existing_flag


def isMissigL(v):
    if pd.isna(v):
        return np.nan
    else:
        try:
            if str(v) == '<NA>':
                return np.nan
        except Exception as e:
            print(e)
    return v


def fix_numbers(s):
    try:
        s = pd.to_numeric(s.convert_dtypes(), errors='coerce')
        s = pd.to_numeric(s, errors='coerce').convert_dtypes()
    except:
        pass
    return s


def prep_data(df):
    # run after standard rename
    df = df.replace(['nan', 'None', '<NA>'], np.nan)
    for k in df.columns:
        print(k, pandas_data_type_map.get(k, 'GO DEFINE IT'))
        if pandas_data_type_map.get(k, 'xxxxx') in ["Int64", "float64", "int"]:  # "str" had this
            df[k] = fix_numbers(df[k])
        elif pandas_data_type_map.get(k, 'xxxxx') in ["datetime64[ns]"]:
            df[k] = pd.to_datetime(df[k], errors='coerce')
        elif pandas_data_type_map.get(k, 'xxxxx') in ["str"]:
            df[k] = df[k].astype("str")
        else:
            df[k] = df[k].astype("str")
    df = df.replace(['nan', 'None', '<NA>'], np.nan)
    return df


pd.set_option("display.max_columns", 400)
pd.set_option('display.max_colwidth', 4000)
pd.set_option("display.max_rows", 400)

import oyaml


def fix_cols(df):
    cols = []
    for c in df.columns:
        cl = c.lower()
        cols.append(cl.strip().replace(' ', '_').replace('/', '_').replace('\\', '_').replace('-', '_'))
    return cols


def rename_standard_cols(df):
    #         rename_map = dict(zip(standard_df.col, standard_df.real_name))  #cant use show bc wa only know wheat we DO NOT weant to sheo
    rename_map = get_json_from_s3('canvas-data-types', 'canvas_col_rename.json')
    df.rename(columns=rename_map, inplace=True)
    return df


def remove_hidden_cols(df):
    hidden_cols = get_json_from_s3('canvas-data-types', 'canvas_cols_to_be_hidden.json')
    hidden_list = list(set(df.columns).intersection(set(hidden_cols)))
    print(df.shape)
    df.drop(hidden_list, axis=1, inplace=True)
    print(df.shape)
    return df


def get_json_from_s3(bucket, key):
    s3 = boto3.resource('s3')
    obj = s3.Object(bucket, key)
    data = obj.get()['Body'].read().decode('utf-8')
    json_data = oyaml.safe_load(data)
    return json_data


pandas_data_type_map = get_json_from_s3('canvas-data-types', 'pandas_data_type_map.json')
rename_map = get_json_from_s3('canvas-data-types', 'canvas_col_rename.json')


@task(log_stdout=True, nout=3, tags=[f"snowflake_large"])
def gen_baseline_mce(str_run_date):
    dte_run_under = str_run_date.replace('-', '_')
    stage_table_name = f'cps_dsci_archive.canvas_mce_{dte_run_under}'
    stage_note_table_name = f"{stage_table_name}_notes"

    SQL_LIST = []

    SQL_LIST.append("use warehouse CPS_DSCI_ETL_EXT3_WH;")

    SQL_LIST.append(f"""create or replace table {stage_table_name} as
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
            ,  scope as (


                with avail as (
                    select h.ENGAGEMENT_NUMBER, max(LAST_UPDATED_DATE::DATE) as mx_avail_dte, h.SMART_ACCOUNT_ID
                    from SERVICES_DB.SERVICES_ENT_FBV.BV_MCE_AM_ENGAGEMENT_HDR h
                    where LAST_UPDATED_DATE > dateadd(day, -90, current_date ) -- convert to lookback window
                    and ENGAGEMENT_OUTCOME in( 'Asset Management', 'SFC')
                    group by h.ENGAGEMENT_NUMBER,h.SMART_ACCOUNT_ID
                ), existing as (
                    --select e.ENGAGEMENT_NUMBER, max(e.date_sourced::DATE) as mx_dte
                    --from CPS_DSCI_ARCHIVE.MCE_EVIDENCE e
                    --group by e.ENGAGEMENT_NUMBER
                    
                    select e.REMOTE_SYSTEM_CUSTOMER_IDENTIFIER AS  ENGAGEMENT_NUMBER, max(e.date_sourced::DATE) as mx_dte
                    from CPS_DSCI_API.DC_DATA_SOURCES e
                    WHERE E.REMOTE_SYSTEM ='mce_engagement_id'
                    group by e.REMOTE_SYSTEM_CUSTOMER_IDENTIFIER
                    
                    
                ), updated as (
                    select avail.*
                    from avail
                             join existing
                                  on (existing.ENGAGEMENT_NUMBER = avail.ENGAGEMENT_NUMBER
                                and
                                avail.mx_avail_dte > dateadd(day, 1, existing.mx_dte )
                                -- at least a day for now
                                      )
                ), new as (
                    select avail.*
                    from avail
                             left join existing
                                  on (existing.ENGAGEMENT_NUMBER = avail.ENGAGEMENT_NUMBER )
                    where existing.ENGAGEMENT_NUMBER is null
                )
                select * from updated
                union
                select * from new

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
                     WHEN ib.instance_status_desc IN ('Replace Pend-DEINSTALLED','Replaced-DEINSTALLED','RMA_inProgress')  --Replaced-DEINSTALLED, Replace Pend-DEINSTALLED, RMA_inProgress  via : EDW_SERVICE_ETL_DB.ss.CSF_CSI_INSTANCE_STATUSES
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
               isite.COUNTRY as installed_at_country,--65
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
                st_site.COUNTRY as ship_to_country, --135
                --a.ship_to_postal_code,
                st_site.postal_code as ship_to_postal_code, --142
                st_site.address1 || ' ' || NVL (st_site.address2, '') as ship_to_address_lines,
                st_site.cr_party_name as ship_to_cr_party_name,
            ------------------------------------------------------------------------------------------
                bt_site.party_name  as bill_to_customer_name,
                bt_site.address1 || ' ' || NVL (bt_site.address2, '') as bill_to_address_lines,  -- 402
                bt_site.city as bill_to_city,
                bt_site.COUNTRY as bill_to_country,
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
                cvd_line.DNR_FLAG, --231 MCE only
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
                             WHEN ib.instance_id IS NULL THEN NULL
                             WHEN datediff(day,  CURRENT_TIMESTAMP, cvd_line.END_DATE )         <= 30       THEN '30 Days '
                             WHEN datediff(day,  CURRENT_TIMESTAMP, cvd_line.END_DATE ) BETWEEN 31 AND 60   THEN '60 Days'
                             WHEN datediff(day,  CURRENT_TIMESTAMP, cvd_line.END_DATE ) BETWEEN 61 AND 90   THEN '90 Days'
                             WHEN datediff(day,  CURRENT_TIMESTAMP, cvd_line.END_DATE ) BETWEEN 91 AND 180  THEN '180 Days'
                             WHEN datediff(day,  CURRENT_TIMESTAMP, cvd_line.END_DATE ) BETWEEN 181 AND 365 THEN '1 Year'
                             WHEN datediff(day,  CURRENT_TIMESTAMP, cvd_line.END_DATE ) BETWEEN 366 AND 730 THEN '2 Year'
                             WHEN datediff(day,  CURRENT_TIMESTAMP, cvd_line.END_DATE ) BETWEEN 731 AND 1095  THEN '3 Year'
                             ELSE 'More Than 3 Years' END as renewal_category, --329
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
                    WHEN cvd_line.sts_code IS NULL 
                             THEN 
                               case when IB.covered_status = 'A' then 'ACTIVE'
                                    when IB.covered_status = 'I' then 'EXPIRED'
                                    when IB.covered_status = 'N' then 'NEVER COVERED'
                                    end
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
                   nvl(cp.FIXED_PRODUCT_TYPE,nvl(item.ib_product_type,'Unknown')) as real_product_type,
                   isite.SITE_USE_ORG_ID as site_ou_id,
                   hdr_core.VENDOR_ORGANIZATION_ID as contract_ou_id,
                   hdr_core.VENDOR_ORGANIZATION_NAME as contract_ou_name,
                   case when
                        hdr_core.VENDOR_ORGANIZATION_ID <> nvl(isite.SITE_USE_ORG_ID,-1)
                        AND hdr_core.VENDOR_ORGANIZATION_ID is not null -- is covered basically
                       then 'Y' else 'N' end as ou_conflict,
                row_number() over ( partition by h.ENGAGEMENT_NUMBER, d.INSTANCE_ID order by cvd_line.COVERED_LINE_ID  desc) as orderv_current,
                   h.ENGAGEMENT_NUMBER ,
                   h.SMART_ACCOUNT_ID,
                   h.LAST_UPDATED_DATE::DATE as snap_date,
            -----------------------------------------
            -- mce only data : goes into mce evidence
            -----------------------------------------
           d.approved_site_flag,d.approved_contract_flag, d.collection_date,d.collector_host_name,d.collector_matched,
           d.confidence_precedence, d.CRITICAL_FLAG as critical_asset, d.customer_matched,d.data_input_source,
           d.DECOMM_FLAG as decommission_flag, h.device_health_score,d.exclusion_flag as excluded_asset,
            CASE WHEN d.exclusion_flag = 'Y' AND ib.attribute26 IS NOT NULL
                     THEN 'Cisco Hybrid Cloud as-a-Service(Athena)'
                     WHEN d.exclusion_flag = 'Y' AND ib.attribute26 IS NULL
                     THEN 'User Requested Exclusion'
                     ELSE NULL
                  END as exclusion_reason
           , CASE
                WHEN     cvd_line.cvd_attribute11 IS NOT NULL  AND ib.instance_id IS NOT NULL THEN  'Y'
                ELSE
                   NULL  END as exs_number_flag,
           d.cmrc_matched as found_collector_db,
           d.c3_matched as found_in_cicso_db,
           d.greater_china_flag,
           CASE
             WHEN d.aligned_gu_flag = 'Y' AND ib.instance_id IS NOT NULL
             THEN
                'GU Aligned'
             WHEN d.aligned_gu_flag = 'N' AND ib.instance_id IS NOT NULL
             THEN
                'GU Not Aligned'
             ELSE
                NULL  END as gu_aligned,
           h.gu_data_flag,d.sfc_asset_flag,h.smart_account_name,
           h.updated_version,d.verification_flag,h.verified_status,
                    CASE
                     WHEN (    NVL (d.collector_matched, 'N') = 'Y'
                           AND NVL (d.c3_matched, 'N') = 'Y'
                           AND NVL (d.customer_matched, 'N') = 'Y')
                     THEN
                        'ZONE1.3'
                     WHEN (    NVL (d.collector_matched, 'N') = 'Y'
                           AND NVL (d.c3_matched, 'N') = 'Y'
                           AND NVL (d.customer_matched, 'N') = 'N')
                     THEN
                        'ZONE1.1'
                     WHEN (    NVL (d.collector_matched, 'N') = 'Y'
                           AND NVL (d.c3_matched, 'N') = 'N'
                           AND NVL (d.customer_matched, 'N') = 'Y')
                     THEN
                        'ZONE2.1'
                     WHEN (    NVL (d.collector_matched, 'N') = 'Y'
                           AND NVL (d.c3_matched, 'N') = 'N'
                           AND NVL (d.customer_matched, 'N') = 'N')
                     THEN
                        'ZONE2.0'
                     WHEN (    NVL (d.collector_matched, 'N') = 'N'
                           AND NVL (d.c3_matched, 'N') = 'Y'
                           AND NVL (d.customer_matched, 'N') = 'Y')
                     THEN
                        'ZONE1.2'
                     WHEN (    NVL (d.collector_matched, 'N') = 'N'
                           AND NVL (d.c3_matched, 'N') = 'Y'
                           AND NVL (d.customer_matched, 'N') = 'N')
                     THEN
                        'ZONE4.0'
                     WHEN (    NVL (d.collector_matched, 'N') = 'N'
                           AND NVL (d.c3_matched, 'N') = 'N'
                           AND NVL (d.customer_matched, 'N') = 'Y')
                     THEN
                        'ZONE3.0'
                  END as zone_id,
                CASE
                     WHEN (    NVL (d.collector_matched, 'N') = 'Y'
                           AND NVL (d.c3_matched, 'N') = 'Y'
                           AND NVL (d.customer_matched, 'N') = 'Y')
                     THEN
                        'If we have all three views'
                     WHEN (    NVL (d.collector_matched, 'N') = 'Y'
                           AND NVL (d.c3_matched, 'N') = 'Y'
                           AND NVL (d.customer_matched, 'N') = 'N')
                     THEN
                        'If we have only Cisco and Collector View'
                     WHEN (    NVL (d.collector_matched, 'N') = 'Y'
                           AND NVL (d.c3_matched, 'N') = 'N'
                           AND NVL (d.customer_matched, 'N') = 'Y')
                     THEN
                        'If we have collector and customer only'
                     WHEN (    NVL (d.collector_matched, 'N') = 'Y'
                           AND NVL (d.c3_matched, 'N') = 'N'
                           AND NVL (d.customer_matched, 'N') = 'N')
                     THEN
                        'Only Collector'
                     WHEN (    NVL (d.collector_matched, 'N') = 'N'
                           AND NVL (d.c3_matched, 'N') = 'Y'
                           AND NVL (d.customer_matched, 'N') = 'Y')
                     THEN
                        'If we have only Cisco and Customer View'
                     WHEN (    NVL (d.collector_matched, 'N') = 'N'
                           AND NVL (d.c3_matched, 'N') = 'Y'
                           AND NVL (d.customer_matched, 'N') = 'N')
                     THEN
                        'Only Cisco/C3'
                     WHEN (    NVL (d.collector_matched, 'N') = 'N'
                           AND NVL (d.c3_matched, 'N') = 'N'
                           AND NVL (d.customer_matched, 'N') = 'Y')
                     THEN
                        'Only customer'
                  END as zone_description --377
            FROM scope
                join SERVICES_DB.SERVICES_ENT_FBV.BV_MCE_AM_ENGAGEMENT_HDR  h on (h.ENGAGEMENT_NUMBER = scope.ENGAGEMENT_NUMBER)
                join SERVICES_DB.SERVICES_ENT_FBV.BV_MCE_AM_ENGAGEMENT_DATA d on (h.ENGAGEMENT_ID=d.ENGAGEMENT_ID
                                                                                      AND
                                                                            d.operation_code IN ('I', 'U', 'N'))
                join  EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_INSTANCE_DETAIL ib  on (ib.INSTANCE_ID=d.INSTANCE_ID)
                join EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_SITE_GU_DENORM isite on
                    (
                    ib.install_at_site_use_id = isite.site_use_id
                    and
                    nvl(isite.EDWSF_SOURCE_DELETED_FLAG,'N') = 'N'

                    and          isite.site_use_code = 'SHIP_TO'
                    )
               left join CPS_DSCI_ARCHIVE.CORRECTED_PIDS cp on (ib.ITEM_NAME=cp.ITEM_NAME)
               left join EDW_SERVICE_ETL_DB.ss.CSF_XXCCS_DS_CVDPRDLINE_DETAIL cvd_line on
                        (
                        d.INSTANCE_ID  = cvd_line.INSTANCE_ID  --NOT mce but live c3
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
                    dsd.DATE=CPS_DSCI_ARCHIVE.FIX_DATES(ib.ship_date)
                    )
                left join CPS_DSCI_ARCHIVE.DIM_DATE_NEW dldos on (
                    dldos.DATE=CPS_DSCI_ARCHIVE.FIX_DATES_W_DEF(item.last_date_of_support::DATE,'2150-12-31'::DATE)
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
                            );""")

    SQL_LIST.append(f"""create or replace table {stage_note_table_name} as
        with multi as (
            select distinct instance_id
            from {stage_table_name} i
            where i.orderv_current > 1  
        ), dets as (
                select distinct
                    i.INSTANCE_ID,
                    array_agg(DISTINCT i.SERVICE_LEVEL  ) OVER ( PARTITION BY i.PARENT_INSTANCE_ID) as list_of_service_levels,
                    array_agg(DISTINCT i.COVERAGE_LINE_ID_CPL_ID::bigint ) OVER ( PARTITION BY i.PARENT_INSTANCE_ID) as list_of_covered_lines,
                    array_agg(DISTINCT i.contract_number  ) OVER ( PARTITION BY i.PARENT_INSTANCE_ID) as list_of_contracts,
                   row_number() over ( partition by i.ENGAGEMENT_NUMBER, i.INSTANCE_ID order by i.COVERAGE_LINE_ID_CPL_ID  desc) as orderv_current
            from  {stage_table_name} i join multi on (i.INSTANCE_ID=multi.INSTANCE_ID)
            -- where i.ORDERV_CURRENT = 1  -- NEVER THIS
            )
        select INSTANCE_ID, OBJECT_CONSTRUCT(*) as notes from dets;""")

    SQL_LIST.append(f"""insert into CPS_DSCI_ARCHIVE.MCE_EVIDENCE
    (INSTANCE_ID, APPROVED_SITE_FLAG, COLLECTION_DATE, COLLECTOR_HOST_NAME, COLLECTOR_MATCHED, CONFIDENCE_PRECEDENCE,
      CRITICAL_ASSET, CUSTOMER_MATCHED, DATA_INPUT_SOURCE, DECOMMISSION_FLAG, DEVICE_HEALTH_SCORE, ENGAGEMENT_NUMBER, EXCLUDED_ASSET,
     EXCLUSION_REASON, EXS_NUMBER_FLAG, FOUND_COLLECTOR_DB, FOUND_IN_CICSO_DB, GREATER_CHINA_FLAG, GU_ALIGNED, GU_DATA_FLAG,
     PRODUCT_COVERAGE_LINE_NUMBER, SFC_ASSET_FLAG, SMART_ACCOUNT_ID, SMART_ACCOUNT_NAME, UPDATED_VERSION, VERIFICATION_FLAG,
     VERIFIED_STATUS, ZONE_DESCRIPTION, COVERAGE_LINE_ID_CPL_ID, SHIP_TO_SITE_USE_ID, INSTALLED_AT_SITE_ID, BILL_TO_SITE_USE_ID, ROW_NUM, DATE_SOURCED)
    select f.INSTANCE_ID,approved_site_flag,collection_date,collector_host_name,collector_matched,confidence_precedence,
       critical_asset,customer_matched,data_input_source,decommission_flag,
       device_health_score,engagement_number,excluded_asset,exclusion_reason,exs_number_flag,found_collector_db,
       found_in_cicso_db,greater_china_flag,gu_aligned,gu_data_flag,product_coverage_line_number,
       sfc_asset_flag,smart_account_id,smart_account_name,
       updated_version,verification_flag,verified_status,zone_description,
       f.COVERAGE_LINE_ID_CPL_ID,
       f.SHIP_TO_SITE_USE_ID,
       f.INSTALLED_AT_SITE_ID,
       f.BILL_TO_SITE_USE_ID,
       ORDERV_CURRENT,
       f.SNAP_DATE::timestamp_ntz as date_sourced
    from {stage_table_name} f
    where ORDERV_CURRENT = 1""")

    engine = create_engine(sec.get_sf_pw(check_env('prod'), 'CPS_DSCI_ETL_EXT3_WH', 'CPS_DSCI_ARCHIVE'))
    con = engine.connect()
    for sql in SQL_LIST:
        print(sql)
        con.execute(sql)
    con.close()
    # wii run these in the future
    # -- select distinct i.ENGAGEMENT_NUMBER, i.SMART_ACCOUNT_ID  from {stage_table_name} i
    engine = create_engine(sec.get_sf_pw('prd_cps_dsci_etl_svc', 'CPS_DSCI_ETL_EXT2_WH', 'CPS_DSCI_STG'))
    examples = f"""
    select  i.ENGAGEMENT_NUMBER, i.SMART_ACCOUNT_ID, h.ENGAGEMENT_NAME, count(0) as total_rows
    from {stage_table_name} i  join SERVICES_DB.SERVICES_ENT_FBV.BV_MCE_AM_ENGAGEMENT_HDR h
    on (h.ENGAGEMENT_NUMBER=i.ENGAGEMENT_NUMBER)
    group by i.ENGAGEMENT_NUMBER, i.SMART_ACCOUNT_ID, h.ENGAGEMENT_NAME                    


                    """
    print(examples)
    real_data = pd.read_sql(examples, engine)
    real_data['engagement_name_mod'] = real_data.apply(lambda x: clean_name(x['engagement_name']), axis=1)

    real_data['display_name'] = real_data.apply(lambda x: gen_display_name(x['engagement_name_mod'], dte_run_under),
                                                axis=1)

    return real_data, stage_table_name, stage_note_table_name


from os import listdir
from os.path import isfile, join
from common import file_ops


# lotup.append([ row.engagement_number, row.engagement_name_mod, row.prep_date  ])
@task(log_stdout=True, tags=["snowflake_small"])
def extract_mce_data(local_folder_v2_enrich, lst, dte_run, sf_warehouse, core_query):
    dte_run_under = dte_run.replace('-', '_')
    req = lst[0]
    ref_name = f"{lst[1]}".lower()
    src_tbl = f"{lst[2]}"
    notes_tbl = f"{lst[3]}"
    smart_account = f"{lst[4]}"
    total_rows = lst[5]
    this_fn_data_out_path = file_ops.prep_data_location(os.path.join(local_folder_v2_enrich, dte_run_under, str(req)),
                                                        clear_contents=False)
    dir_list = os.listdir(this_fn_data_out_path)

    print(f"Starting extract_acat_data for {req}  @@ {this_fn_data_out_path} ")
    if not os.path.isdir(this_fn_data_out_path) or len(dir_list) == 0:
        this_query = core_query.format(src_file_name=ref_name, stage_note_table_name=notes_tbl,
                                       stage_table_name=src_tbl, req=req)

        print(this_query)
        engine = create_engine(sec.get_sf_pw('prd_cps_dsci_etl_svc', sf_warehouse, 'CPS_DSCI_STG'))
        letters = string.ascii_letters
        snowflake_temp_loc = 'culvert_stage_{}'.format(''.join(random.choice(letters) for i in range(25)))

        con = engine.connect()
        resultsS = con.execute("USE {db}.{schema}".format(db=snowflake_db, schema='CPS_DSCI_STG'))
        resultsW = con.execute("USE warehouse {}".format(sf_warehouse)).fetchall()
        cmd = "create or replace temporary stage {tmp_name} file_format=(TYPE = PARQUET compression=snappy)".format(
            tmp_name=snowflake_temp_loc)
        resultsT = pd.DataFrame(con.execute(cmd).fetchall())
        print(resultsT)

        cmd = """copy into @{tmp_name}/ from ( {this_query}
             )
              file_format = (type = 'parquet')
              header = true;
            """.format(tmp_name=snowflake_temp_loc, this_query=this_query)

        resultsT = pd.DataFrame(con.execute(cmd).fetchall())
        print(resultsT)

        print(this_fn_data_out_path, snowflake_temp_loc)

        con.execute("GET @{tmp_name} file://{landing_folder}".format(tmp_name=snowflake_temp_loc,
                                                                     landing_folder=this_fn_data_out_path))
        con.close()
        engine.dispose()

        dir_list = os.listdir(this_fn_data_out_path)  # we got records

    if len(dir_list) > 0:
        write_one_file.run(dte_run, dte_run_under, req, this_fn_data_out_path, total_rows, ref_name, smart_account)

        return this_fn_data_out_path


import re


def clean_name(fld):
    return re.sub('[^0-9a-zA-Z]+', '_', fld)






@task(log_stdout=True)
def getwork(df, src, notes):
    lotup = []  # list of tuples for work to map
    for i, row in df.iterrows():
        lotup.append([row.engagement_number, row.display_name, src, notes, row.smart_account_id, row.total_rows])
    return lotup


def gen_display_name(f1, f2):
    return f"{f1}_{f2}"


def check_env(env):
    print(env)
    if env == "dev":
        cn = "dev_cps_dsci_etl_svc"
    elif env == "stage":
        cn = "stg_cps_dsci_etl_svc"
    elif env == "prod":
        cn = "prd_cps_dsci_etl_svc"
    return cn


@task(log_stdout=True, tags=["snowflake_xsmall", "canvas_data_sources"])
def write_one_file(src_date, dte_str, eng_number, loc, total_rows, display_name, smart_id):
    # loc  = os.path.join(f'/mnt/newmt/ERP/home/alanzen/MCE_FILES/{dte_str}',str(eng_number))
    s3path = f's3://canvas-data-store-prod/MCE_PREPPED_FILES/mce_{eng_number}/{dte_str}/full/'
    if os.path.isdir(loc):
        files_to_move = os.listdir(loc)
        OBJ_TO_delete = wr.s3.list_objects(s3path)
        for otd in OBJ_TO_delete:
            wr.s3.delete_objects(otd)
        for f in files_to_move:
            wr.s3.upload(local_file=os.path.join(loc, f), path=os.path.join(s3path, f))

        if len(files_to_move) > 0:
            engine = create_engine(sec.get_sf_pw(check_env('prod'), 'CPS_DSCI_ETL_EXT1_WH', 'CPS_DSCI_ARCHIVE'))
            con = engine.connect()
            engagement_id = eng_number
            file_name = '*.parquet'
            full_canvas_out_pth = s3path
            file_type = 'all'
            num_records = total_rows
            date_sourced = src_date
            last_processed_date = src_date
            display_name = display_name
            engagement_id = eng_number
            if smart_id == '<NA>' or smart_id == '0':
                smart_account_id = 0
            else:
                try:
                    smart_account_id = int(smart_id)
                except:
                    smart_account_id = 0

            update_metadata_query = f"""MERGE INTO CPS_DSCI_API.DC_DATA_SOURCES d
                  USING (
                    SELECT '{engagement_id}' AS REMOTE_SYSTEM_CUSTOMER_IDENTIFIER,
                    '{file_name}' AS FILE_NAME,
                    '{full_canvas_out_pth}' AS FOLDER_PATH,
                    'MCE' AS FILE_SOURCE,
                    'all' AS FILE_TYPE,
                    {num_records} AS NUM_RECORDS,
                    '{date_sourced}' AS DATE_SOURCED,
                    '{last_processed_date}' AS LAST_PROCESSED_DATE,
                    'mce_engagement_id' AS REMOTE_SYSTEM,
                     '{display_name}' AS DISPLAY_NAME,
                    '{engagement_id}' AS REQUEST_ID
                 ) s ON d.REMOTE_SYSTEM = s.REMOTE_SYSTEM 
                     AND 
                   d.REMOTE_SYSTEM_CUSTOMER_IDENTIFIER = s.REMOTE_SYSTEM_CUSTOMER_IDENTIFIER
                     AND
                   d.REQUEST_ID=s.REQUEST_ID
                     AND
                 d.FOLDER_PATH = s.FOLDER_PATH
              WHEN MATCHED THEN update SET
                  d.DISPLAY_NAME = s.DISPLAY_NAME, d.LAST_PROCESSED_DATE = s.LAST_PROCESSED_DATE,
                  d.DATE_SOURCED = s.DATE_SOURCED, d.NUM_RECORDS = s.NUM_RECORDS, d.FILE_TYPE = s.FILE_TYPE,
                  d.FILE_SOURCE = s.FILE_SOURCE, d.FILE_NAME = s.FILE_NAME
              WHEN NOT MATCHED THEN INSERT(REMOTE_SYSTEM_CUSTOMER_IDENTIFIER,FILE_NAME,FOLDER_PATH,FILE_SOURCE,FILE_TYPE,NUM_RECORDS,
                                          DATE_SOURCED,LAST_PROCESSED_DATE,REMOTE_SYSTEM,DISPLAY_NAME,REQUEST_ID)
              VALUES (s.REMOTE_SYSTEM_CUSTOMER_IDENTIFIER, s.FILE_NAME, s.FOLDER_PATH, s.FILE_SOURCE, s.FILE_TYPE,
                      s.NUM_RECORDS,s.DATE_SOURCED,s.LAST_PROCESSED_DATE, s.REMOTE_SYSTEM,s.DISPLAY_NAME, s.REQUEST_ID)"""

            print(update_metadata_query)
            con.execute(update_metadata_query)

            if smart_account_id > 0:
                update_metadata_query = f"""MERGE INTO CPS_DSCI_API.DC_DATA_SOURCES d
                  USING (
                    SELECT '{smart_account_id}' AS REMOTE_SYSTEM_CUSTOMER_IDENTIFIER,
                    '{file_name}' AS FILE_NAME,
                    '{full_canvas_out_pth}' AS FOLDER_PATH,
                    'MCE' AS FILE_SOURCE,
                    'all' AS FILE_TYPE,
                    {num_records} AS NUM_RECORDS,
                    '{date_sourced}' AS DATE_SOURCED,
                    '{last_processed_date}' AS LAST_PROCESSED_DATE,
                    'mce_smart_account' AS REMOTE_SYSTEM,
                     '{display_name}' AS DISPLAY_NAME,
                    '{engagement_id}' AS REQUEST_ID
                 ) s ON d.REMOTE_SYSTEM = s.REMOTE_SYSTEM 
                     AND 
                   d.REMOTE_SYSTEM_CUSTOMER_IDENTIFIER = s.REMOTE_SYSTEM_CUSTOMER_IDENTIFIER
                     AND
                   d.REQUEST_ID=s.REQUEST_ID
                     and
                 d.FOLDER_PATH = s.FOLDER_PATH
              WHEN MATCHED THEN update SET
                  d.DISPLAY_NAME = s.DISPLAY_NAME, d.LAST_PROCESSED_DATE = s.LAST_PROCESSED_DATE,
                  d.DATE_SOURCED = s.DATE_SOURCED, d.NUM_RECORDS = s.NUM_RECORDS, d.FILE_TYPE = s.FILE_TYPE,
                  d.FILE_SOURCE = s.FILE_SOURCE, d.FILE_NAME = s.FILE_NAME
              WHEN NOT MATCHED THEN INSERT(REMOTE_SYSTEM_CUSTOMER_IDENTIFIER,FILE_NAME,FOLDER_PATH,FILE_SOURCE,FILE_TYPE,NUM_RECORDS,
                                          DATE_SOURCED,LAST_PROCESSED_DATE,REMOTE_SYSTEM,DISPLAY_NAME,REQUEST_ID)
              VALUES (s.REMOTE_SYSTEM_CUSTOMER_IDENTIFIER, s.FILE_NAME, s.FOLDER_PATH, s.FILE_SOURCE, s.FILE_TYPE,
                      s.NUM_RECORDS,s.DATE_SOURCED,s.LAST_PROCESSED_DATE, s.REMOTE_SYSTEM,s.DISPLAY_NAME, s.REQUEST_ID)"""

            print(update_metadata_query)
            con.execute(update_metadata_query)
            con.close()
    return True


@task()
def get_todays_date():
    return datetime.today().strftime('%Y-%m-%d')

def get_sec_dir(pth):
    #print(os.getcwd(), pth, os.path.join(os.getcwd(), pth))
    return os.path.join(os.getcwd(), pth)

storage_obj = Docker(
    base_image="837578041534.dkr.ecr.us-east-1.amazonaws.com/bases/prefect:0.15.13-python3.9",
    python_dependencies=[
        "requests==2.27.1",
        "pandas==1.4.2",
        "awswrangler==2.12.1",
        "numpy==1.22.3",
        "boto3==1.18.16",
        "aiohttp",
        "hvac",
        "snowflake-sqlalchemy==1.2.4",
        "s3fs==0.4",
        "hvac>0.11.0",
        "SQLAlchemy==1.4.35",
        "fastparquet>0.7.2",
        "XlsxWriter>3.0.3",
        "oyaml"

        ""
    ],
    registry_url="837578041534.dkr.ecr.us-east-1.amazonaws.com/flows",
    files={
        get_sec_dir('common/sec.py') : "/root/.prefect/flows/common/sec.py",
        get_sec_dir('common/file_ops.py') : "/root/.prefect/flows/common/file_ops.py",
        get_sec_dir('common/data_types.py') : "/root/.prefect/flows/common/data_types.py",
        get_sec_dir('common/new_bulkload.py') : "/root/.prefect/flows/common/new_bulkload.py",
        get_sec_dir('common/sql_pool.py') : "/root/.prefect/flows/common/sql_pool.py",
    },
    env_vars={"PYTHONPATH": "${PYTHONPATH}:/root/.prefect/flows/"},
)

with Flow(
        "mce_daily_2022",
        storage=storage_obj,
        run_config=KubernetesRun(memory_request=60000000000),
        executor=LocalDaskExecutor(scheduler="processes", num_workers=psutil.cpu_count(logical=True)),
        # executor=LocalDaskExecutor(scheduler="processes", num_workers=10),
        result=S3Result(bucket="cam-prefect-results")
) as flow:
    dte_run = get_todays_date()
    local_folder_v2_enrich = "/mnt/newmt/ERP/home/alanzen/MCE_FILES"
    sf_warehouse = 'CPS_DSCI_ETL_EXT2_WH'  # small  MAKE SURE TO CAHNGE TAG IF YOU CHANGE THIS
    snowflake_db = "CPS_DB"

    core_query = """select distinct f.*,
            1 as canvas_source_file_{src_file_name},
            case when n.INSTANCE_ID is null then 'single'
                 else 'multi_line_fix' end as modified_record,
                 n.notes as note
            from {stage_table_name} f
            left join {stage_note_table_name} n on (n.INSTANCE_ID=f.INSTANCE_ID )
            where f.ENGAGEMENT_NUMBER ={req} and orderv_current =1
            """

    work, src_tbl, notes_tbl = gen_baseline_mce(dte_run)
    lotup = getwork(work, src_tbl, notes_tbl)
    destinations = extract_mce_data.map(
        local_folder_v2_enrich=unmapped(local_folder_v2_enrich),
        lst=lotup,
        dte_run=unmapped(dte_run),
        sf_warehouse=unmapped(sf_warehouse),
        core_query=unmapped(core_query))





