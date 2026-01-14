from pathlib import Path
import ast
from prefect.triggers import all_successful, all_failed, all_finished
from sqlalchemy import (
    create_engine,
    inspect,
    text,
    bindparam,
    table,
    column,
    VARCHAR,
    insert,
    BIGINT,
Integer,
String
)
import math
from typing import Optional, List, Dict
import requests
from prefect.engine.signals import FAIL , SUCCESS
import datetime as dt
from datetime import datetime
from datetime import date as day
import time
import ast
import string
import random
import os
from prefect.client import Client
from prefect.tasks.secrets import PrefectSecret
from prefect.tasks.prefect import RenameFlowRun
from prefect import unmapped
import prefect
from prefect.triggers import all_successful, all_failed, all_finished
from prefect.engine.signals import SKIP
import pandas as pd
import json
from common import aws_sec
from prefect.run_configs.kubernetes import KubernetesRun
import boto3
from time import sleep
from prefect import Flow, Parameter, task, case
from sqlalchemy import create_engine
from common import sec
import psutil
import oyaml

from collections import OrderedDict
from log_to_dc_job_messages import log_to_dc_job_messages, final_flow_state_message

temp_base_location = "/tmp"
from prefect.engine.results.s3_result import S3Result
from prefect.executors.dask import LocalDaskExecutor
from prefect.run_configs.docker import DockerRun
from prefect.storage import Docker

from common.config import  RunSettings

from common import sec, config
import os

from wb import package_workbook
import pandas as pd

from sqlalchemy import create_engine, text, bindparam, inspect, BIGINT
from sqlalchemy.sql import quoted_name

from common import sec

def get_sec_dir(pth):
    return os.path.join(os.getcwd(), pth)



def convert_to_string(eid, cid):
    eid = str(eid)
    # cid = f"CANVAS-{str(cid)}"
    cid = str(cid)
    return eid, cid


@task(log_stdout=True)
def get_correct_schema(env):
    if env == 'prod':
        return 'CPS_DSCI_API'
    else:
        return 'CPS_DSCI_BR'


def check_env(env):
    if env == "dev":
        cn = "dev_cps_dsci_etl_svc"
    elif env == "stage":
        cn = "stg_cps_dsci_etl_svc"
    elif env == "prod":
        cn = "prd_cps_dsci_etl_svc"
    else:
        cn = env
    return cn

@task(log_stdout=True, tags=["snowflake_small"])
def create_loader_transient_table(run_settings,flow_params):
    cn = check_env('prod')
    correct_schema = get_correct_schema.run(flow_params.sf_env)

    engine = create_engine(
        sec.get_sf_pw(cn, "CPS_DSCI_ETL_WH", correct_schema)
    )


    params_json = { "request_id": f"{flow_params.request_id}",
                    "env": f"{flow_params.sf_env}",
                    "engagement_id": flow_params.engagement_id,
                    "requested_by": f"{flow_params.requested_by}",
                    "serial_numbers" : flow_params.serial_numbers}

    print(params_json)






    serial_create = text(
        """
create or replace TRANSIENT table identifier(:transient_table_name) as
    with data as
        (
            select parse_json(:json) as json_data
          )
         select   distinct json_data:request_id::number         as request_id,
                     json_data:engagement_id::number         as dc_engagement_id,
                     trim(json_data:requested_by::varchar)        as requested_by,
                     serials.value::varchar        as serial_number
                 from data , lateral flatten(input => data.json_data:serial_numbers) serials
            """
    ).bindparams(
    bindparam("transient_table_name", quoted_name(f"{correct_schema}.SN_SERIAL_{flow_params.request_id}_TMP", False)),
    bindparam("json", json.dumps(params_json), type_=String),
    # bindparam("request_id", flow_params.request_id, type_=Integer),
    # bindparam("env", quoted_name(flow_params.sf_env, False)),
    # bindparam("dc_engagment_id", flow_params.engagement_id, type_=Integer),
    # bindparam("requested_by", quoted_name(flow_params.requested_by, False)),
    # bindparam("serial_numbers", flow_params.serial_numbers)

    )
    print(serial_create)


    loader_create = text(
        """
        CREATE OR REPLACE TRANSIENT TABLE identifier(:transient_table_name) (
            SERIAL_NUMBER VARCHAR,
            BILL_TO_SITE_USE_ID DOUBLE,
            COVERED_STATUS VARCHAR,
            DUPLICATE_IB_FLAG VARCHAR,
            INSTALL_AT_SITE_USE_ID DOUBLE,
            INSTANCE_ID DOUBLE,
            INSTANCE_STATUS_DESC VARCHAR,
            INVENTORY_ITEM_ID DOUBLE,
            ITEM_NAME VARCHAR,
            ITEM_TYPE_FLAG VARCHAR,
            PARENT_INSTANCE_ID DOUBLE,
            PO_NUMBER VARCHAR,
            QUANTITY DOUBLE,
            SHIP_DATE TIMESTAMPNTZ,
            SHIP_TO_SITE_USE_ID DOUBLE,
            SO_NUMBER VARCHAR
        )
        AS SELECT 
            NVL(t.serial_number, ib.dup_serial_number)  as serial_number,
            ib.bill_to_site_use_id,
            ib.covered_status,
            ib.duplicate_ib_flag,
            ib.install_at_site_use_id,
            ib.instance_id,
            ib.instance_status_desc,
            ib.inventory_item_id,
            ib.item_name,
            ib.item_type_flag,
            ib.parent_instance_id,
            ib.po_number,
            ib.quantity,
            ib.ship_date,
            ib.ship_to_site_use_id,
            ib.so_number
        FROM identifier(:serial_transient_table) t
        LEFT JOIN EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_INSTANCE_DETAIL ib
        ON t.serial_number = NVL(ib.serial_number, ib.dup_serial_number)
        WHERE ib.EDWSF_SOURCE_DELETED_FLAG = 'N'
        """
    ).bindparams(
        bindparam("transient_table_name", quoted_name(f"{correct_schema}.SN_LOADER_{flow_params.request_id}_TMP", False)),
        bindparam("serial_transient_table", quoted_name(f"{correct_schema}.SN_SERIAL_{flow_params.request_id}_TMP", False)),
    )

    # serial_transient_table = table(
    #     quoted_name(f"{correct_schema}.SN_SERIAL_{flow_params.request_id}_TMP", False), column("serial_number", VARCHAR)
    # )

    with engine.begin() as conn:
        print(f"Creating Temporary Table: {correct_schema}.SN_SERIAL_{flow_params.request_id}_TMP")
        conn.execute(serial_create)
        # batch_size = 5000
        # for i in range(0, len(flow_params.serial_numbers), batch_size):
        #     batch = flow_params.serial_numbers[i : i + batch_size]
        #     serial_insert_stmt = insert(serial_transient_table).values(
        #         [{"serial_number": serial} for serial in batch]
        #     )
        #     conn.execute(serial_insert_stmt)
        # print(f"Creating Transient Table: {correct_schema}.SN_LOADER_{flow_params.request_id}_TMP")
        conn.execute(
            loader_create,
        )


    print(f"{correct_schema}.SN_LOADER_{flow_params.request_id}_TMP")

    return f"{correct_schema}.SN_LOADER_{flow_params.request_id}_TMP"


@task(log_stdout=True, tags=["snowflake_small"])
def create_instance_loader_transient_table(flow_params):
    """Runs when a cam chooses instance_id on the customer upload. """


    cn = check_env('prod')
    correct_schema = get_correct_schema.run(flow_params.sf_env)

    engine = create_engine(
        sec.get_sf_pw(cn, "CPS_DSCI_ETL_WH", correct_schema)
    )


    params_json = { "request_id": f"{flow_params.request_id}",
                    "env": f"{flow_params.sf_env}",
                    "engagement_id": flow_params.engagement_id,
                    "requested_by": f"{flow_params.requested_by}",
                    "instance_ids" : flow_params.instance_ids}

    # print(params_json)






    instance_id_create = text(
        """
create or replace TRANSIENT table identifier(:transient_table_name) as
    with data as
        (
            select parse_json(:json) as json_data
          )
         select   distinct json_data:request_id::number         as request_id,
                     json_data:engagement_id::number         as dc_engagement_id,
                     trim(json_data:requested_by::varchar)        as requested_by,
                     instance_ids.value::varchar        as instance_id
                 from data , lateral flatten(input => data.json_data:instance_ids) instance_ids
            """
    ).bindparams(
    bindparam("transient_table_name", quoted_name(f"{correct_schema}.SN_INSTANCE_{flow_params.request_id}_TMP", False)),
    bindparam("json", json.dumps(params_json), type_=String),
    # bindparam("request_id", flow_params.request_id, type_=Integer),
    # bindparam("env", quoted_name(flow_params.sf_env, False)),
    # bindparam("dc_engagment_id", flow_params.engagement_id, type_=Integer),
    # bindparam("requested_by", quoted_name(flow_params.requested_by, False)),
    # bindparam("serial_numbers", flow_params.serial_numbers)

    )
    print(instance_id_create)


    loader_create = text(
        """
        CREATE OR REPLACE TRANSIENT TABLE identifier(:transient_table_name) (
            BILL_TO_SITE_USE_ID DOUBLE,
            COVERED_STATUS VARCHAR,
            DUPLICATE_IB_FLAG VARCHAR,
            INSTALL_AT_SITE_USE_ID DOUBLE,
            INSTANCE_ID DOUBLE,
            INSTANCE_STATUS_DESC VARCHAR,
            INVENTORY_ITEM_ID DOUBLE,
            ITEM_NAME VARCHAR,
            ITEM_TYPE_FLAG VARCHAR,
            PARENT_INSTANCE_ID DOUBLE,
            PO_NUMBER VARCHAR,
            QUANTITY DOUBLE,
            SHIP_DATE TIMESTAMPNTZ,
            SHIP_TO_SITE_USE_ID DOUBLE,
            SO_NUMBER VARCHAR
        )
        AS SELECT 
            ib.bill_to_site_use_id,
            ib.covered_status,
            ib.duplicate_ib_flag,
            ib.install_at_site_use_id,
            ib.instance_id,
            ib.instance_status_desc,
            ib.inventory_item_id,
            ib.item_name,
            ib.item_type_flag,
            ib.parent_instance_id,
            ib.po_number,
            ib.quantity,
            ib.ship_date,
            ib.ship_to_site_use_id,
            ib.so_number
        FROM identifier(:instance_transient_table) t
        LEFT JOIN EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_INSTANCE_DETAIL ib
        ON t.instance_id = ib.instance_id
        WHERE ib.EDWSF_SOURCE_DELETED_FLAG = 'N'
        """
    ).bindparams(
        bindparam("transient_table_name", quoted_name(f"{correct_schema}.IN_LOADER_{flow_params.request_id}_TMP", False)),
        bindparam("instance_transient_table", quoted_name(f"{correct_schema}.SN_INSTANCE_{flow_params.request_id}_TMP", False)),
    )

    # serial_transient_table = table(
    #     quoted_name(f"{correct_schema}.SN_SERIAL_{flow_params.request_id}_TMP", False), column("serial_number", VARCHAR)
    # )

    with engine.begin() as conn:
        print(f"Creating Temporary Table: {correct_schema}.SN_INSTANCE_{flow_params.request_id}_TMP")
        conn.execute(instance_id_create)
        # batch_size = 5000
        # for i in range(0, len(flow_params.serial_numbers), batch_size):
        #     batch = flow_params.serial_numbers[i : i + batch_size]
        #     serial_insert_stmt = insert(serial_transient_table).values(
        #         [{"serial_number": serial} for serial in batch]
        #     )
        #     conn.execute(serial_insert_stmt)
        # print(f"Creating Transient Table: {correct_schema}.SN_LOADER_{flow_params.request_id}_TMP")
        conn.execute(
            loader_create,
        )


    print(f"{correct_schema}.IN_LOADER_{flow_params.request_id}_TMP")

    return f"{correct_schema}.IN_LOADER_{flow_params.request_id}_TMP"



@task(log_stdout=True, tags=["snowflake_small"])
def query_engagement_resolved_serials(run_settings: RunSettings,flow_params):
    cn = check_env('prod')
    correct_schema = get_correct_schema.run(flow_params.sf_env)

    engine = create_engine(
        sec.get_sf_pw(cn, "CPS_DSCI_ETL_WH", correct_schema)
    )


#
# """        WITH resolved_instances AS (
#             SELECT distinct instance_id from identifier(:engagement_tag_table)
#             WHERE tag_id = 1411
#             ORDER BY instance_id
#             )
#         SELECT resolved_instances.instance_id::BIGINT AS instance_id, NVL(ss.serial_number, ss.dup_serial_number) AS serial_number
#         FROM resolved_instances
#         LEFT JOIN EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_INSTANCE_DETAIL ss
#         ON ss.instance_id = resolved_instances.instance_id"""


    resolved_query = text(
        """
         WITH resolved_instances AS (
            SELECT distinct t.instance_id, trx.serial_number from identifier(:engagement_tag_table) t
            join identifier(:transient_table_name) trx on ( trx.instance_id=t.instance_id)
            WHERE tag_id = 1411 and t.is_deleted = 'F'
            )
        SELECT resolved_instances.instance_id::BIGINT AS instance_id, resolved_instances.serial_number AS serial_number
        FROM resolved_instances
        """
    ).bindparams(
        bindparam("engagement_tag_table", quoted_name(f"{correct_schema}.DC_ENGAGEMENT_TAGS_{flow_params.engagement_id}".lower(), False)),
        bindparam("transient_table_name",
                  quoted_name(f"{correct_schema}.SN_LOADER_{flow_params.request_id}_TMP", False)),
    )
    with engine.connect() as conn:
        df_result = pd.read_sql(resolved_query, conn)

    print(f"Found {len(df_result)} resolved instance_ids")
    return df_result

@task(log_stdout=True)
def parse_any_serials(x: list):
    accepted = []
    for i in x:

            if len(i.strip()) > 1 and len(i.strip()) < 24:
                accepted.append(i)
            else:
                print(f"Rejected: {i}")

    return accepted

@task(log_stdout=True, tags=["snowflake_small"])
def query_unknown_serials(transient_loader_fqn, flow_params):
    cn = check_env('prod')
    correct_schema = get_correct_schema.run(flow_params.sf_env)
    engine = create_engine(
        sec.get_sf_pw(
            cn,
            "CPS_DSCI_ETL_WH",
            correct_schema
        ),
    )


    query = text(
        """
        SELECT DISTINCT serial_number
        FROM identifier(:transient_table_name)
        WHERE instance_id IS NULL
        """
    ).bindparams(
        bindparam("transient_table_name", quoted_name(transient_loader_fqn, False))
    )

    with engine.begin() as conn:
        result: list[str] = conn.execute(query).scalars().all()

    serials_parsed = parse_any_serials.run(result)

    print(f"Found {len(serials_parsed)} unknown serial numbers")
    return serials_parsed

@task(log_stdout=True, tags=["snowflake_small"],nout = 2)
def query_unknown_instance_ids(transient_loader_fqn, flow_params):
    cn = check_env('prod')
    correct_schema = get_correct_schema.run(flow_params.sf_env)
    engine = create_engine(
        sec.get_sf_pw(
            cn,
            "CPS_DSCI_ETL_WH",
            correct_schema
        ),
    )


    # query = text(
    #     """
    #     SELECT DISTINCT instance_id
    #     FROM identifier(:transient_table_name)
    #     """
    # ).bindparams(
    #     bindparam("transient_table_name", quoted_name(transient_loader_fqn, False))
    # )


    full_query = text(
        """
        SELECT *
        FROM identifier(:transient_table_name)
        """
    ).bindparams(
        bindparam("transient_table_name", quoted_name(transient_loader_fqn, False))
    )

    with engine.begin() as conn:
        df = pd.read_sql(full_query, conn)

    df['instance_id'] = df['instance_id'].astype(int)

    result = df['instance_id'].to_list()



    instance_ids_parsed = set(result)

    print('instance_ids_parsed')
    print(instance_ids_parsed)
    input_instance_ids = set(flow_params.instance_ids)
    print('input_instance_ids')
    print(input_instance_ids)
    unknown_instance_ids = input_instance_ids.difference(instance_ids_parsed)
    print('unknown_instance_ids')
    print(unknown_instance_ids)
    # found_instance_ids_df = df[['instance_id']]
    # found_instance_ids_df = found_instance_ids_df.assign(serial_numbers='NAN')

    print(f"Found {len(unknown_instance_ids)} unknown serial numbers")
    return unknown_instance_ids,df



@task(log_stdout=True, tags=["snowflake_small"])
def create_prepped_transient_table(run_settings: RunSettings,flow_params):
    cn = check_env('prod')
    correct_schema = get_correct_schema.run(flow_params.sf_env)
    engine = create_engine(
        sec.get_sf_pw(
            cn,
            "CPS_DSCI_ETL_WH",
            correct_schema,
        )
    )

    stmt = text(
        """
        CREATE OR REPLACE TRANSIENT TABLE identifier(:prepped_table) AS (
        WITH GUID_CTE  AS (
         WITH I_CTE as (
             SELECT DISTINCT GUID
             FROM CPS_BIA_BR.DATA_CANVAS_ENGAGEMENT_HDR_V
             WHERE uid = :engagement_id
         ),PARTY_IDS_CTE as (
             SELECT DISTINCT trim(value)::BIGINT as party_id
             from I_CTE, LATERAL SPLIT_TO_TABLE(REPLACE(TRIM(I_CTE.GUID), ' ', ','), ',')
             WHERE TRIM(value) != ''
               and TRY_TO_NUMBER(TRIM(value)) IS NOT NULL
         )
         SELECT DISTINCT global_ultimate_id
         FROM EDW_MASTER_ETL_DB.SS.CRT_XXNGCR_DNM_RELATIONSHIPS REL
                  JOIN PARTY_IDS_CTE ON (PARTY_IDS_CTE.PARTY_ID = REL.PARTY_ID)
        ), 
        CONTRACTS_CTE as (
            SELECT try_to_number( CONTRACT_NUMBER)::BIGINT as CONTRACT_NUMBER
            FROM CPS_BIA_BR.DATA_CANVAS_CONTRACT_DATA_V
            WHERE replace(id,'CAM-','')::BIGINT = :engagement_id
              AND try_to_number( CONTRACT_NUMBER) IS NOT NULL
        ),
        SERIALS_CTE as (
            SELECT * FROM identifier(:loader_table)
        ),
         PREPPED_DATA_CTE as
         (
             SELECT

                 SERIALS_CTE.instance_id,
                 -- NVL(ib.serial_number, ib.dup_serial_number) as serial_number, -- 125 , 334
                 SERIALS_CTE.serial_number,
                 SERIALS_CTE.duplicate_ib_flag,
                 -- NVL(ib.duplicate_ib_flag, 'N') as duplicate_ib_flag,  -- 50
                 SERIALS_CTE.item_name AS device_name,
                 --ib.item_name AS device_name, --85, 230

                 item.ib_product_type as product_type,--60, 325
                 NVL(cp.FIXED_PRODUCT_TYPE,NVL(item.ib_product_type,'Unknown')) as real_product_type,

                 item.PRODUCT_FAMILY_MFG_DESCR,-- 494 , 636
                 item.product_family_description, --111, 635
                 item.DESCRIPTION as product_description, -- 519, 316
                 item.product_family, --110 , 318
                 item.business_entity_name_top as architecture, --499 , 160
                 item.BUSINESS_ENTITY_DESC_TOP as  architecture_d,--497 , 161
                 item.SUB_BUSINESS_ENTITY_DESC_TOP as sub_architecture_d,--498 , 361
                 item.sub_business_entity_name_top as sub_architecture,--496 , 360
                 SERIALS_CTE.INSTANCE_STATUS_DESC as install_base_status,
                 --ib.INSTANCE_STATUS_DESC as install_base_status, --82 263

                 NVL(cvd_line.USD_PRICE_UNIT ,cvd_line.PRICE_UNIT) as usd_prorated_list_price, --504
                 NVL(cvd_line.USD_PRICE_UNIT ,cvd_line.PRICE_UNIT) * SERIALS_CTE.QUANTITY as usd_extended_list_price, -- 505
                 -- NVL(cvd_line.USD_PRICE_UNIT ,cvd_line.PRICE_UNIT) * ib.QUANTITY as usd_extended_list_price, -- 505
                 item.product_list_price, --113, 320
                 item.product_list_price_gpl_us as  global_product_list_price, --255, 587

                 SERIALS_CTE.so_number as product_so,
                 --ib.so_number as product_so, --323-147
                 SERIALS_CTE.po_number as product_po,
                 --ib.po_number as product_po, --597, 321
                 CPS_DSCI_ARCHIVE.FIX_DATES(SERIALS_CTE.ship_date) as ship_date_header, --132, 348
                 --CPS_DSCI_ARCHIVE.FIX_DATES(ib.ship_date) as ship_date_header, --132, 348
                 isite.SITE_USE_ID as installed_at_site_id, -- 61
                 isite.party_name  as installed_at_customer_name, --74
                 isite.address1 || ' ' || NVL (isite.address2, '') as installed_at_address_lines,--500, 265
                 isite.city as installed_at_city,--63
                 isite.COUNTRY as installed_at_country,--65
                 isite.postal_code as installed_at_postal_code, --75
                 isite.state as installed_at_state_province, --76
                 isite.gu_id as installed_at_gu_id,--68
                 isite.gu_name as installed_at_gu_name, -- 69

                 CPS_DSCI_ARCHIVE.FIX_DATES(item.last_date_of_support::date) as product_last_date_of_support_ldos, --89, 319

                 IFF(item.mapped_to_service_flag = 'YES WITH SPM', 'T', 'F') as  mapped_to_service_flag, --98, 293
                 --item.serviceable_product_flag,  --345 not replicated -- removed per Aaron request
                 CASE
                    WHEN SERIALS_CTE.item_type_flag  = 'S' THEN 'Standalone'
                    WHEN SERIALS_CTE.item_type_flag  = 'P' THEN 'Major'
                    WHEN SERIALS_CTE.item_type_flag  = 'C' THEN 'Minor'
                    END as Config_Type,
                 -- CASE
                 --     WHEN IB.item_type_flag  = 'S' THEN 'Standalone'
                 --     WHEN IB.item_type_flag  = 'P' THEN 'Major'
                 --     WHEN IB.item_type_flag  = 'C' THEN 'Minor'
                 --     END as Config_Type, -- 489, 195
                 ib_prnt.instance_id as parent_instance_id, --109
                 ib_prnt.serial_number AS parent_serial_number,


                 hdr_core.contract_number, --38
                 hdr_core.BILL_TO_CUSTOMER_NAME as contract_bill_to_customer_name, --33,
                 hdr_core.BILLTO_CR_PARTY_NAME as bill_to_party_name, --26, 395,
                 hdr_core.BILLTO_GU_NAME as contract_bill_to_customer_gu_name,--199, 36
                 hdr_core.bill_to_country as contract_bill_to_country,
                 hdr_core.service_line_name as  service_level, --128
                 cvd_line.line_number as product_coverage_line_number,--312
                 -- below suffers from 1:many
                 CPS_DSCI_ARCHIVE.FIX_DATES(cvd_line.START_DATE::date  ) as  product_coverage_start_date, -- 149 ,313
                 CPS_DSCI_ARCHIVE.FIX_DATES(cvd_line.END_DATE::date  )   as  product_coverage_end_date,  --52 , 403
                 CPS_DSCI_ARCHIVE.FIX_DATES(cvd_line.DATE_TERMINATED::date) as product_coverage_termination_date, --315,92

                 hdr_core.service_line_sts_code as service_level_status, --340, 608,
                 CASE
                     WHEN cvd_line.sts_code IS NOT NULL THEN cvd_line.sts_code
                     WHEN cvd_line.sts_code IS NULL
                         THEN
                         case when SERIALS_CTE.covered_status = 'A' then 'ACTIVE'
                         -- case when IB.covered_status = 'A' then 'ACTIVE'
                                 when SERIALS_CTE.covered_status = 'I' then 'EXPIRED'
                              -- when IB.covered_status = 'I' then 'EXPIRED'
                                  when SERIALS_CTE.covered_status = 'N' then 'NEVER COVERED'
                              -- when IB.covered_status = 'N' then 'NEVER COVERED'
                             end
                     ELSE 'NEVER COVERED'
                     END as product_coverage_status,
                 SERIALS_CTE.COVERED_STATUS,
                 -- IB.covered_status, --219 42
                 IFF(SERIALS_CTE.COVERED_STATUS = 'A', 'COVERED', 'UNCOVERED') as coverage_status,
                 -- IFF(ib.covered_status = 'A', 'COVERED', 'UNCOVERED') as coverage_status,

                 cvd_line.MAINTENANCE_SO_NUMBER, --96
                 cvd_line.maintenance_po_number, -- 492, 291

                 cvd_line.PRICE_NEGOTIATED, --495  637 alt location vs nasty cte
                 item.service_list_price as service_list_price_raw,--130 , 342
                 cvd_line.DNR_FLAG, --231 MCE only
                 -----------------------------------------------------------------
                 IFF(isite.gu_id  in          (select global_ultimate_id from GUID_CTE), 'Y', 'N') as is_guid,
                 IFF(hdr_core.contract_number::BIGINT in ( select  CONTRACT_NUMBER from CONTRACTS_CTE), 'Y', 'N') as is_contract,
                 IFF(SERIALS_CTE.INSTANCE_STATUS_DESC = 'Latest-INSTALLED', 'Y', 'N') as is_good_status,
                 -- IFF(ib.INSTANCE_STATUS_DESC = 'Latest-INSTALLED', 'Y', 'N') as is_good_status,
                 IFF(cvd_line.MAINTENANCE_SO_NUMBER is not null, 'Y', 'N') as has_mso ,
                 -----------------------------------------------------------------
                 row_number() over ( partition by SERIALS_CTE.INSTANCE_ID order by cvd_line.COVERED_LINE_ID  desc) as orderv_current
                 -- row_number() over ( partition by ib.INSTANCE_ID order by cvd_line.COVERED_LINE_ID  desc) as orderv_current
                FROM SERIALS_CTE
                      -- JOIN EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_INSTANCE_DETAIL ib on
                 -- (NVL(ib.serial_number, ib.dup_serial_number)=SERIALS_CTE.serial_number)
                      join EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_SITE_GU_DENORM isite on
                 (
                        SERIALS_CTE.install_at_site_use_id = isite.site_use_id
                             --ib.install_at_site_use_id = isite.site_use_id
                         and
                             NVL(isite.EDWSF_SOURCE_DELETED_FLAG,'N') = 'N'

                         and          isite.site_use_code = 'SHIP_TO'
                     )
                     left join CPS_DSCI_ARCHIVE.CORRECTED_PIDS cp on (SERIALS_CTE.ITEM_NAME=cp.ITEM_NAME)
                      --left join CPS_DSCI_ARCHIVE.CORRECTED_PIDS cp on (ib.ITEM_NAME=cp.ITEM_NAME)
                      left join EDW_SERVICE_ETL_DB.ss.CSF_XXCCS_DS_CVDPRDLINE_DETAIL cvd_line on
                 (
                             SERIALS_CTE.INSTANCE_ID = cvd_line.INSTANCE_ID
                             --ib.INSTANCE_ID  = cvd_line.INSTANCE_ID  --NOT mce but live c3
                         and
                             NVL(cvd_line.EDWSF_SOURCE_DELETED_FLAG,'N') = 'N'
                     )
                      left join EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_SAHDR_CORE hdr_core  on
                 (
                             cvd_line.contract_id = hdr_core.contract_id and cvd_line.service_line_id = hdr_core.service_line_id
                         and
                             NVL(hdr_core.EDWSF_SOURCE_DELETED_FLAG,'N') = 'N'
                     )
                      left join EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_SAIB_ITEMS item on
                 (
                        item.INVENTORY_ITEM_ID = SERIALS_CTE.INVENTORY_ITEM_ID
                             -- item.INVENTORY_ITEM_ID = ib.inventory_item_id
                         and
                             NVL(item.EDWSF_SOURCE_DELETED_FLAG,'N') = 'N'
                     )
                 --ship_to_site_use_id -> ship tp  and          site.site_use_code = 'SHIP_TO'
                      left join EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_SITE_GU_DENORM st_site on
                 (
                        SERIALS_CTE.ship_to_site_use_id = st_site.site_use_id
                             --ib.ship_to_site_use_id = st_site.site_use_id
                         and
                             NVL(st_site.EDWSF_SOURCE_DELETED_FLAG,'N') = 'N'
                         and          st_site.site_use_code = 'SHIP_TO'
                     )
                 --bill_to_site_use_id -> bill to  and          site.site_use_code = 'BILL_TO'
                      left join EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_SITE_GU_DENORM bt_site on
                 (
                        SERIALS_CTE.bill_to_site_use_id = bt_site.site_use_id
                             --ib.bill_to_site_use_id = bt_site.site_use_id
                         and
                             NVL(bt_site.EDWSF_SOURCE_DELETED_FLAG,'N') = 'N'
                         and          bt_site.site_use_code = 'BILL_TO'
                     )
                      -- LEFT JOIN EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_INSTANCE_DETAIL ib_prnt on
                      LEFT JOIN SERIALS_CTE AS ib_prnt on SERIALS_CTE.parent_instance_id = ib_prnt.instance_id
                 )

         SELECT * from PREPPED_DATA_CTE where orderv_current = 1
        )  
    """
    ).bindparams(
        bindparam("prepped_table", quoted_name(f"{correct_schema}.SN_PREPPED_{flow_params.request_id}_TMP", False)),
        bindparam("loader_table", quoted_name( f"{correct_schema}.SN_LOADER_{flow_params.request_id}_TMP", False)),
        bindparam("engagement_id", flow_params.engagement_id, type_=BIGINT),
    )


    with engine.begin() as conn:
        print(f"Creating Transient Table : {correct_schema}.SN_PREPPED_{flow_params.request_id}_TMP")
        conn.execute(stmt)

    return f"{correct_schema}.SN_PREPPED_{flow_params.request_id}_TMP"

@task(log_stdout=True, tags=["snowflake_small"])
def query_unscoped_serials(
    flow_params, run_settings: RunSettings
) :
    """
    Query our transient loader table for serial numbers that could not be resolved to an instance

    Parameters
    ----------
    flow_params: FlowParams
    run_settings : RunSettings
    Returns
    -------
    set[str]
        Set of serial numbers that could not be resolved to an instance
    """
    cn = check_env('prod')
    correct_schema = get_correct_schema.run(flow_params.sf_env)
    engine = create_engine(
        sec.get_sf_pw(
            cn,
            "CPS_DSCI_ETL_WH",
            correct_schema
        ),
    )



    query_unscoped = text(
        """
        WITH SCOPED_SERIALS_CTE AS (
            SELECT DISTINCT serial_number FROM identifier(:prepped_table)
            WHERE serial_number IS NOT NULL
        ),
        VALID_SERIALS_CTE AS (
            SELECT DISTINCT serial_number FROM identifier(:loader_table)
            WHERE serial_number IS NOT NULL
            AND instance_id IS NOT NULL
        )
        SELECT serial_number FROM VALID_SERIALS_CTE
        WHERE serial_number NOT IN (SELECT serial_number FROM SCOPED_SERIALS_CTE)
        """
    ).bindparams(
        bindparam("prepped_table", quoted_name(f"{correct_schema}.SN_PREPPED_{flow_params.request_id}_TMP", False)),
        bindparam("loader_table", quoted_name(f"{correct_schema}.SN_LOADER_{flow_params.request_id}_TMP", False)),
    )

    with engine.connect() as conn:
        result: list[str] = conn.execute(query_unscoped).scalars().all()

    result_parsed = parse_any_serials.run(result)

    print(f"Found {len(result_parsed)} unscoped serial numbers")
    return result_parsed


@task(log_stdout=True, tags=["snowflake_small"])
def create_decision_data_table(
    flow_params, run_settings: RunSettings
) -> pd.DataFrame:
    """
    Get the output data from the prepped table
    Parameters
    ----------
    flow_params : FlowParams
    run_settings : RunSettings

    Notes
    -----
    Checkpointing is disabled for this task because it is a rather large result.

    Returns
    -------
    pd.DataFrame
    """

    cn = check_env('prod')
    correct_schema = get_correct_schema.run(flow_params.sf_env)
    engine = create_engine(
        sec.get_sf_pw(
            cn,
            "CPS_DSCI_ETL_WH",
            correct_schema
        ),
    )

    # query = text(
    #     """
    #     SELECT * ,
    #     IFF(is_guid = 'Y', .6, 0) + IFF(is_contract = 'Y', .2, 0) + IFF(has_mso = 'Y', .1, 0) + IFF(is_good_status = 'Y', .1, -.1) as score,
    #     row_number() over ( partition by serial_number order by score desc) as score_rank,
    #    -- If a partition has more than one row, flag it as a multi, otherwise, flag it as a single
    #     IFF(count(*) over (partition by serial_number) > 1, 'Y', 'N') as is_multi
    #     FROM identifier(:prepped_table) prepped
    #     -- Remove any that we don't have a serial number for
    #     WHERE prepped.instance_id IS NOT NULL
    #     """
    # ).bindparams(
    #     bindparam("prepped_table", quoted_name(f"{correct_schema}.SN_PREPPED_{flow_params.request_id}_TMP", False)),
    # )

    create_sql = text(
                """CREATE OR REPLACE TRANSIENT TABLE identifier(:resolved_table) AS  -- this is the set that ranks the options of multis we are going to send 1411s at some point
                SELECT * ,
                        IFF(is_guid = 'Y', .6, 0) + IFF(is_contract = 'Y', .2, 0) + IFF(has_mso = 'Y', .1, 0) + IFF(is_good_status = 'Y', .1, -.1) as score,
                        row_number() over ( partition by serial_number order by score desc) as score_rank,
                        -- If a partition has more than one row, flag it as a multi, otherwise, flag it as a single
                        IFF(count(*) over (partition by serial_number) > 1, 'Y', 'N') as is_multi
                    FROM identifier(:prepped_table) prepped
                    -- Remove any that we don't have a serial number for
                WHERE prepped.instance_id IS NOT NULL
                and SERIAL_NUMBER not in (
                    WITH resolved_instances AS
                        (
                        SELECT distinct instance_id from identifier(:engagement_tag_table)
                        WHERE tag_id = 1411 and is_deleted = 'F'
                        ORDER BY instance_id
                        )
                            SELECT distinct serial_number AS serial_number
                                FROM resolved_instances
                                LEFT JOIN identifier(:prepped_table) ss
                                ON ss.instance_id = resolved_instances.instance_id
                                where  ss.serial_number is not null
                    )"""
                ).bindparams(
                bindparam("resolved_table", quoted_name(f"{correct_schema}.SN_RESOLVED_{flow_params.request_id}_TMP", False)),
                bindparam("prepped_table", quoted_name(f"{correct_schema}.SN_PREPPED_{flow_params.request_id}_TMP", False)),
                bindparam("engagement_tag_table", quoted_name(f"{correct_schema}.DC_ENGAGEMENT_TAGS_{flow_params.engagement_id}".lower(), False)),
                )

    # query = text(
    #             """        SELECT * ,
    #                     IFF(is_guid = 'Y', .6, 0) + IFF(is_contract = 'Y', .2, 0) + IFF(has_mso = 'Y', .1, 0) + IFF(is_good_status = 'Y', .1, -.1) as score,
    #                     row_number() over ( partition by serial_number order by score desc) as score_rank,
    #                    -- If a partition has more than one row, flag it as a multi, otherwise, flag it as a single
    #                     IFF(count(*) over (partition by serial_number) > 1, 'Y', 'N') as is_multi
    #                     FROM identifier(:prepped_table) prepped
    #                     -- Remove any that we don't have a serial number for
    #                     WHERE prepped.instance_id IS NOT NULL
    #                     and SERIAL_NUMBER not in (                WITH resolved_instances AS (
    #                         SELECT distinct instance_id from identifier(:engagement_tag_table)
    #                         WHERE tag_id = 1411
    #                         ORDER BY instance_id
    #                         )
    #                     SELECT NVL(ss.serial_number, ss.dup_serial_number) AS serial_number
    #                     FROM resolved_instances
    #                     LEFT JOIN EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_INSTANCE_DETAIL ss
    #                     ON ss.instance_id = resolved_instances.instance_id)"""
    #
    #     # where multi is true , filter on this.
    #             ).bindparams(
    #             bindparam("prepped_table", quoted_name(f"{correct_schema}.SN_PREPPED_{flow_params.request_id}_TMP", False)),
    #             bindparam("engagement_tag_table", quoted_name(f"{correct_schema}.DC_ENGAGEMENT_TAGS_{flow_params.engagement_id}".lower(), False)),
    #             )





    with engine.begin() as conn:
        # df = pd.read_sql(query, conn)
        conn.execute(create_sql)

    # print(f"Query Complete: [Rows: {df.shape[0]}], [Cols: {df.shape[1]}]")
    return f"{correct_schema}.SN_RESOLVED_{flow_params.request_id}_TMP"

@task(log_stdout=True)
def get_run_settings() -> RunSettings:
    return config.RunSettings()

@task(log_stdout=True)
def get_flow_params(sf_env,engagement_id,serial_numbers,request_id,requested_by, instance_ids,notification_id) -> RunSettings:
    return config.FlowParams(engagement_id,serial_numbers,sf_env,request_id,requested_by,instance_ids,notification_id)


@task(log_stdout=True)
def get_next_seq_val(sf_env,run_settings):
    cn = check_env('prod')
    correct_schema = get_correct_schema.run(sf_env)

    engine = create_engine(
        sec.get_sf_pw(cn, run_settings.wh_xsmall, correct_schema)
    )


    qry = f"""
        select {correct_schema}.{run_settings.serial_resolution_seq}.NEXTVAL
    """


    df  = pd.read_sql(qry, engine)
    print('request_id = ',int(df['nextval'][0]) )

    return int(df['nextval'][0])




# @task(log_stdout=True,trigger=all_finished, tags=["snowflake_xsmall"])
@task(log_stdout=True, tags=["snowflake_xsmall"])
def log_to_serial_resolution_table(status,qry_type, run_settings,flow_params, api_response=None):
    cn = check_env('prod')
    correct_schema = get_correct_schema.run(flow_params.sf_env)

    engine = create_engine(
        sec.get_sf_pw(cn, run_settings.wh_xsmall, correct_schema)
    )

    con = engine.connect()
    date_created = datetime.now().isoformat()




    if qry_type == 'insert':
        qry = f"""
        insert into {correct_schema}.{run_settings.serial_resolution_tbl}(
                                                        dc_engagement_id,
                                                        request_id,
                                                        status,
                                                        CREATED_BY,
                                                        CREATE_DTM
                                                        ) values ({flow_params.engagement_id},
                                                                    {flow_params.request_id},
                                                                    '{status}',
                                                                    '{flow_params.requested_by}',
                                                                    '{date_created}'
                                                                    );
        """
    elif qry_type == 'update':
        qry = f"""
        UPDATE CPS_DB.{correct_schema}.{run_settings.serial_resolution_tbl} 
        set STATUS = '{status}' , 
            JSON_PATH = '{flow_params.json_output_uri}', 
            EXCEL_PATH = '{flow_params.excel_output_uri}'  ,
             TAG_RESPONSE = '{json.dumps(api_response)}'        
        where REQUEST_ID  = '{int(flow_params.request_id)}' ;
        """

    con.execute(qry)



    return True


@task(nout = 4)
def get_df_for_api_call(df_resolved_result,flow_params,run_settings):
    cn = check_env('prod')
    correct_schema = get_correct_schema.run(flow_params.sf_env)

    engine = create_engine(
        sec.get_sf_pw(cn, run_settings.wh_xsmall, correct_schema)
    )

    con = engine.connect()
    date_created = datetime.now().isoformat()

    #gets all winning lines with same parent_id
    multi_same_parent_query = text(
                """select * from identifier(:resolved_table) as a
                    JOIN(select Max(SERIAL_NUMBER), max(PARENT_INSTANCE_ID) as PARENT_INSTANCE_ID,1411 as TAG_ID
                         from identifier(:resolved_table)
                         where PARENT_INSTANCE_ID is not null
                         group by SERIAL_NUMBER, PARENT_INSTANCE_ID
                         having count(*) > 1 -- get serial numbers that have duplicate parent instance ids
                            and min(SCORE_RANK) = 1 -- only get the ones that are ranked 1
                    ) as b
                    on a.PARENT_INSTANCE_ID = b.PARENT_INSTANCE_ID

                """
                ).bindparams(
                bindparam("resolved_table", quoted_name(f"{correct_schema}.SN_RESOLVED_{flow_params.request_id}_TMP", False)),
                )

    # gets all remaining that were not in the above query
    multi_resolved_query = text(
                """select *,1411 as TAG_ID from identifier(:resolved_table) 
                --where SCORE_RANK = 1 
                where INSTANCE_ID 
                not in (select INSTANCE_ID from identifier(:resolved_table) as a
                                JOIN(select Max(SERIAL_NUMBER), max(PARENT_INSTANCE_ID) as PARENT_INSTANCE_ID
                                     from identifier(:resolved_table)
                                     where PARENT_INSTANCE_ID is not null
                                     group by SERIAL_NUMBER, PARENT_INSTANCE_ID
                                     having count(*) > 1 -- get serial numbers that have duplicate parent instance ids
                                        and min(SCORE_RANK) = 1 -- only get the ones that are ranked 1
                                ) as b
                                on a.PARENT_INSTANCE_ID = b.PARENT_INSTANCE_ID)
                                -- and  SCORE_RANK = 1
                """
                ).bindparams(
                bindparam("resolved_table", quoted_name(f"{correct_schema}.SN_RESOLVED_{flow_params.request_id}_TMP", False)),
                )

    print("multi_same_parent_query *************************")
    print(multi_same_parent_query)
    print("multi_resolved_query *************************")
    print(multi_resolved_query)



    with engine.begin() as conn:
        multi_same_parent_df = pd.read_sql(multi_same_parent_query, conn)
        multi_resolved_df = pd.read_sql(multi_resolved_query, conn)

    # df_query=multi_resolved_df
    # df_resolved=df_resolved_result

    # has all ranked lines
    multi_resolved_df = multi_resolved_df.loc[
        ~multi_resolved_df[run_settings.serial_col_name].isin(
            df_resolved_result[run_settings.serial_col_name]
        )
    ]
    # has all ranked lines
    multi_same_parent_df = multi_same_parent_df.loc[
        ~multi_same_parent_df[run_settings.serial_col_name].isin(
            df_resolved_result[run_settings.serial_col_name]
        )
    ]

    multi_resolved_picks_df = multi_resolved_df[multi_resolved_df['score_rank'] ==1]
    multi_same_parent_picks__df = multi_same_parent_df[multi_same_parent_df['score_rank'] ==1]

    return multi_same_parent_df,multi_resolved_df, multi_resolved_picks_df, multi_same_parent_picks__df



# def log_to_dc_job_messages(sf_env,request_id, log_message):
#     ##### COMMENTED OUT UNTILL AN ACTUAL ENDPOINT IS CALLING IT AND ITS PASSED A DC REQUEST ID
#     # cn = check_env('prod')
#     # correct_schema = get_correct_schema.run(sf_env)
#     #
#     # engine = create_engine(
#     #     sec.get_sf_pw(cn, "CPS_DSCI_ETL_EXT2_WH", correct_schema)
#     # )
#     #
#     # con = engine.connect()
#     #
#     #
#     # bia_qry = f"""
#     # insert into {correct_schema}.dc_job_messages(request_id,logged_message) values ({request_id},'{log_message}')
#     # """
#     #
#     # try:
#     #     con.execute(bia_qry)
#     # except Exception as e:
#     #     print(e)
#     #     print(
#     #         f"Failed while attempting to log message to : {correct_schema}.dc_job_messages"
#     #     )
#
#
#
#
#     return True


@task(log_stdout=True)
def make_api_call(run_settings,dc_engagement_id,auth_token, logged_user, df_chunks, env,request_id,flow_params):
    print(env)
    res_log = []
    print("&&&&&&&&&&&&&&&&&&&&")
    print(logged_user)
    print(df_chunks)
    iter = 1
    for chunk in df_chunks:
        if not chunk.empty:
            print(f"{dc_engagement_id}, {run_settings.tag_resolved}, {chunk['instance_id'].tolist()}")

            tag_request_json = {
                                "tag_id":   int(run_settings.tag_resolved),
                                "instance_ids":  chunk['instance_id'].tolist(),
                                "engagement_id": int(dc_engagement_id)
                                }

            logged_user_request_param = logged_user.replace('@','%40' )
            dev_endpoint = "devdatacanvaswf.cisco.com"
            prod_endpoint = "datacanvaswf.cisco.com"

            if env == 'prod':
                endpoint = prod_endpoint
            elif env == 'dev':
                endpoint = dev_endpoint

            full_request_uri = f'https://{endpoint}/api/v2/thought_spot/actions/set?logged_user={logged_user_request_param}'

            print(full_request_uri)
            headers = {'Authorization': f'Bearer {auth_token}', 'Content-Type': 'application/json'}



            r = requests.post(full_request_uri,
                                headers=headers, verify=False, json =tag_request_json )

            log_to_dc_job_messages(env, request_id,
                                   f"INFO: Completed API call {iter} for {dc_engagement_id} with response : Status Code: {r.status_code}",
                                   flow_params.requested_by, flow_params.notification_id)
            print(f"Status Code: {r.status_code}, Response: {r.json()}")
            res = r.json()
            res_log.append(res)
            iter += 1

    print(res_log)
    return res_log


@task(log_stdout=True)
def demo_cognito_api_auth(env,service_name,region_name):
    secret_id = f"{env}/Cognito"


    session = boto3.session.Session(aws_access_key_id=aws_sec.ACCESS_KEY, aws_secret_access_key=aws_sec.SECRET_KEY)
    client_ssm = session.client(
        service_name=service_name,
        region_name=region_name,
        aws_access_key_id=aws_sec.ACCESS_KEY,
        aws_secret_access_key=aws_sec.SECRET_KEY,
    )
    cognito_secret_raw = json.loads(
        client_ssm.get_secret_value(SecretId=secret_id)["SecretString"]
    )


    cognito_client = session.client(
        service_name="cognito-idp",
        region_name=region_name,
    )




    response_raw = cognito_client.admin_initiate_auth(
        UserPoolId=cognito_secret_raw['UserPoolId'],
        ClientId=cognito_secret_raw['ClientId'],
        AuthFlow=cognito_secret_raw['AuthFlow'],
        AuthParameters={
            "USERNAME": cognito_secret_raw['USERNAME'],
            "PASSWORD": cognito_secret_raw['PASSWORD'],
        },
    )


    AuthenticationResult = response_raw['AuthenticationResult']
    Access_Token = AuthenticationResult['AccessToken']

    return Access_Token


@task(log_stdout=True)
def looped_task(run_settings,flow_params,demo_cognito_api_auth_result,df_chunks,env, request_id):
    responses = []
    for i in df_chunks:
        api_response = make_api_call.run(run_settings,
                                        dc_engagement_id = flow_params.engagement_id,
                                     auth_token = demo_cognito_api_auth_result,
                                     logged_user = flow_params.requested_by,
                                     df_chunks = i,
                                     env=env,
                                         request_id=request_id,
                                         flow_params=flow_params,
                                         )
        responses.append(api_response)

    return responses

@task()
def split_df_by_tag_id(cleaned_df):
    gb = cleaned_df.groupby(['Tag_ID'])
    split_dfs = [gb.get_group(x) for x in gb.groups]


    return split_dfs


def chunk_df_to_size(data_df, chunk_size):
    if len(data_df) < chunk_size:
        print('df less than chunk_size ')
        return [data_df]

    else:
        total_length = len(data_df)
        total_chunk_num = math.ceil(total_length / chunk_size)
        normal_chunk_num = math.floor(total_length / chunk_size)
        chunks = []
        for i in range(normal_chunk_num):
            chunk = data_df[(i * chunk_size):((i + 1) * chunk_size)]
            chunks.append(chunk)
        if total_chunk_num > normal_chunk_num:
            chunk = data_df[(normal_chunk_num * chunk_size):total_length]
            chunks.append(chunk)
        return chunks

@task(log_stdout=True)
def prepare_df_for_api_call(split_dfs):
    """
    This returns a list of lists, where each sublist contains one or more dfs having one tag_id,
    depending on the size of requested split.

    :param split_dfs:
    :return:
    """
    final_list = []
    for i in split_dfs:
        final_list.append(chunk_df_to_size(i, 100000))

    print(final_list)
    return final_list



@task(log_stdout=True)
def build_error_response(api_response,flow_params, request_id):
    response_json = {}

    if api_response:
        if 'success' in api_response[0]:
            log_to_dc_job_messages(flow_params.sf_env, request_id, f"SUCCESS: Completed tag upload with no errors.",
                                   flow_params.requested_by, flow_params.notification_id)
            print("No Errors")
        # else:
            # response_json['Errors'] = "USER NOT A MEMBER OF ENGAGEMENT"
            # log_to_dc_job_messages(flow_params.sf_env, request_id, f"FAILED: User not a member of the engagement",
            #                        flow_params.requested_by, flow_params.notification_id)
    else:
        log_to_dc_job_messages(flow_params.sf_env, request_id, f"FAILED: Run failed, nothing was added to your data.",
                               flow_params.requested_by, flow_params.notification_id)

    print(response_json)

    return response_json


@task(log_stdout=True,trigger=all_finished )
def make_api_call_to_notifications(run_settings,dc_engagement_id,auth_token, logged_user, metrics_json, env,request_id,user_id,flow_params,all_logged_len,run_type):
    print(env)
    res_log = []
    print("&&&&&&&&&&&&&&&&&&&&")
    print(logged_user)


    # print(metrics_json)
    # print(type(metrics_json))
    if isinstance(metrics_json, str):
        metrics_json = ast.literal_eval(metrics_json)

    if run_type == 'serial_number':

        stats_json = {
            "previously_resolved": len(metrics_json['resolved']),
            "multi_resolved": len(metrics_json['multi']),
            "multi_with_same_parent_id_resolved": len(metrics_json['multi_same_parent']),
            "single_resolved": len(metrics_json['single']),
            "excel_location": metrics_json['excel_location']
        }

    else:
        stats_json = {
            "unknown": len(metrics_json['df_unknown']),
            "previously_resolved": len(metrics_json['resolved']),
            "excel_location": metrics_json['excel_location']
        }


    print(type(metrics_json))



    params_list = [
        {
            "tree_id": 402,
            "notification_category": "result",
            "subject": "Customer File",
            "data": stats_json,
            "dc_user_id": user_id,
            "dc_engagement_id": dc_engagement_id
        }
    ]

    logged_user_request_param = logged_user.replace('@','%40' )
    dev_endpoint = "devdatacanvaswf.cisco.com"
    prod_endpoint = "datacanvaswf.cisco.com"

    if env == 'prod':
        endpoint = prod_endpoint
    elif env == 'dev':
        endpoint = dev_endpoint

    full_request_uri = f'https://{endpoint}/api/v2/workflows/notifications?logged_user={logged_user_request_param}'

    print(full_request_uri)
    headers = {'Authorization': f'Bearer {auth_token}', 'Content-Type': 'application/json'}



    r = requests.post(full_request_uri,
                        headers=headers, verify=False, json =params_list )

    log_to_dc_job_messages(env, request_id,
                           f"INFO: Completed API call for {dc_engagement_id} with response : Status Code: {r.status_code}",
                           flow_params.requested_by, flow_params.notification_id)
    print(f"Status Code: {r.status_code}, Response: {r.json()}")
    res = r.json()
    res_log.append(res)

    print(res_log)
    build_error_response.run(res_log, flow_params, request_id)

    cleaned = delete_all_temp_tables(flow_params,request_id)

    raise SUCCESS()
    return res_log



def delete_all_temp_tables(flow_params,request_id):
    cn = check_env('prod')
    correct_schema = get_correct_schema.run(flow_params.sf_env)

    engine = create_engine(
        sec.get_sf_pw(cn, "CPS_DSCI_ETL_EXT2_WH", correct_schema)
    )

    con = engine.connect()


    serial_table = f"SN_SERIAL_{flow_params.request_id}_TMP"
    loader_table = f"SN_LOADER_{flow_params.request_id}_TMP"
    prepped_table = f"SN_PREPPED_{flow_params.request_id}_TMP"
    resolved_table = f"SN_RESOLVED_{flow_params.request_id}_TMP"
    to_delete_list = [serial_table,loader_table,prepped_table,resolved_table]

    for table in to_delete_list:
        del_query = f"DROP TABLE {correct_schema}.{table}"
        con.execute(del_query)
        print(del_query)

    return True

#
# storage_obj = Docker(
#     # base_image="containers.cisco.com/ejurotic/prefect15-3_3-8-8",
#     base_image="837578041534.dkr.ecr.us-east-1.amazonaws.com/bases/prefect:0.15.13-python3.9",
#     python_dependencies=[
#         "pandas==1.3.3",
#         "awswrangler==2.12.1",
#         "numpy==1.25.1",
#         "boto3",
#         "aiohttp==3.8.4",
#         "hvac==0.11.2",
#         "snowflake-sqlalchemy==1.2.4",
#         "s3fs==0.4",
#         "SQLAlchemy===1.4.41",
#         "awswrangler==2.12.1",
#         "fastparquet==0.7.2",
#         "XlsxWriter==3.1.2",
#         "oyaml==1.0",
#         "thoughtspot_rest_api_v1==1.3.1",
#         "thoughtspot_tml==1.2.0",
#         "cloudpickle==2.0.0"
#
#
#     ],
#     # registry_url="containers.cisco.com/ejurotic/",
#     registry_url="837578041534.dkr.ecr.us-east-1.amazonaws.com/dc/ts/p1",
#     files={
#         get_sec_dir('common/new_bulkload.py') : "/root/.prefect/flows/common/new_bulkload.py",
#         get_sec_dir('common/sec.py') : "/root/.prefect/flows/common/sec.py",
#         get_sec_dir('common/config.py'): "/root/.prefect/flows/common/config.py",
#         get_sec_dir('common/aws_sec.py') : "/root/.prefect/flows/common/aws_sec.py",
#         get_sec_dir('wb.py'): "/root/.prefect/flows/wb.py",
#         get_sec_dir('common/sql_pool.py'): "/root/.prefect/flows/common/sql_pool.py",
#
#
#     },
#     env_vars={"PYTHONPATH": "${PYTHONPATH}:/root/.prefect/flows/"},
# )
#
#
#
# with Flow(
#     "dc-gu-p1-serial-resolution",
#     storage = storage_obj,
#     run_config=KubernetesRun(memory_request=60000000000),
#     # executor=LocalDaskExecutor(scheduler="threads", num_workers=(psutil.cpu_count(logical=True)-1)),
#     executor=LocalDaskExecutor(scheduler="processes", num_workers=4),
#     result=S3Result(bucket="cam-prefect-results"),
# ) as flow:
#     # request_id = Parameter("request_id", required=True)
#     env = Parameter("env", required=True)
#     engagement_id = Parameter("engagement_id", required=True)
#     serial_numbers = Parameter("serial_numbers", required=True)
#     requested_by = Parameter("requested_by", required=True)
#
#
#
#
#     run_settings = get_run_settings()
#
#     #this is only happening for testing, this should be happening in the API layer
#     request_id = get_next_seq_val(env, run_settings)
#
#     flow_params = get_flow_params(env,engagement_id,serial_numbers,request_id,requested_by)
#
#     #this is only happening for testing, this should be happening in the API layer
#
#     log_to_serial_resolution_table("Running","insert", run_settings=run_settings,flow_params=flow_params,
#         )
#
#
#     transient_loader_table_result = create_loader_transient_table(
#          run_settings=run_settings, flow_params = flow_params
#     )
#
#     df_resolved_result = query_engagement_resolved_serials(
#         flow_params=flow_params, run_settings=run_settings, upstream_tasks=[transient_loader_table_result]
#     )
#     #
#     unknown_serials_result = query_unknown_serials(
#         transient_loader_fqn=transient_loader_table_result, run_settings=run_settings,flow_params=flow_params,upstream_tasks=[df_resolved_result],
#     )
#     #
#     prepped_transient_table_result = create_prepped_transient_table(
#         flow_params=flow_params,
#         run_settings=run_settings,
#         upstream_tasks=[transient_loader_table_result,unknown_serials_result],
#     )
#
#
#     unscoped_serials_result = query_unscoped_serials(
#         flow_params=flow_params,
#         run_settings=run_settings,
#         upstream_tasks=[prepped_transient_table_result, transient_loader_table_result],
#     )
#
#
#     resolved_table_uri = create_decision_data_table(
#         flow_params=flow_params,
#         run_settings=run_settings,
#         upstream_tasks=[unscoped_serials_result],
#     )
#
#
#
#     demo_cognito_api_auth_result = demo_cognito_api_auth(env,
#         region_name = "us-east-1", service_name="secretsmanager", upstream_tasks=['df_chunks']
#     )
#
#     multi_same_parent_df,multi_resolved_df, multi_resolved_picks_df, multi_same_parent_picks__df = get_df_for_api_call(
#         df_resolved_result,
#         flow_params=flow_params,
#         run_settings=run_settings,
#         upstream_tasks=[resolved_table_uri],
#     )
#
#
#     # split_df = split_df_by_tag_id(cleaned_df, upstream_tasks=['cleaned_df'])
#
#     df_chunks = prepare_df_for_api_call([multi_same_parent_df,multi_resolved_df], upstream_tasks=[multi_same_parent_df,multi_resolved_df ])
#
#     api_response = looped_task(run_settings,flow_params,demo_cognito_api_auth_result,df_chunks,env,request_id,upstream_tasks=[demo_cognito_api_auth_result,df_chunks])
#     #
#     #
#     response_to_log = build_error_response(api_response,flow_params, request_id ,upstream_tasks=[api_response])
#
#
#
#     excel_uri, json_uri,metrics_json = package_workbook(
#         multi_same_parent_df = multi_same_parent_df,
#         df_query=multi_resolved_df,
#         df_resolved=df_resolved_result,
#         #
#         unscoped=unscoped_serials_result,
#         unknown=unknown_serials_result,
#         flow_params=flow_params,
#         run_settings=run_settings,
#         upstream_tasks=[multi_same_parent_df,multi_resolved_df],
#     )
#
#     log_to_serial_resolution_table("Success","update", run_settings=run_settings,flow_params=flow_params,api_response = response_to_log, upstream_tasks=[excel_uri, json_uri],
#         )
#


# if __name__ == "__main__":
#     flow.run(
#         parameters=                                    {
#                 "env": "dev",
#                 "engagement_id": 43,
#                 "requested_by": "ejurotic@cisco.com",
#                 "serial_numbers" : [
#                     #unkown
#             "FTX1745K1AS123",
#             "FTX1430S38A123",
#             "DCH1827V0GM123",
#             "FTT201101HE123",
#             "FCH2045FYCB123",
#             "FDO1747Y1TE123",
#             "NWG0804007B123",
#             "SAL1424KACS123",
#                     # single
#                     "FTX1745K1AS",
#                     "FTX1430S38A",
#                     "DCH1827V0GM",
#                     "FTT201101HE",
#                     "FCH2045FYCB",
#                     "FDO1747Y1TE",
#                     "NWG0804007B",
#                     "SAL1424KACS",
#                     #multi
#                         "FCH170289FE",
#                         "13A38305",
#                         "K0OM000847244903F8",
#                         "S0RU00044739A86169",
#                         "FOC1126Y2BW",
#                         "FTX1909K0SH",
#                         "LIT18290RQ4",
#                     #resolved
#                             "FNS22271B8M",
#                             "SPC16520A9Y",
#                             "INL26340SM0",
#                             "MSY26012179",
#                             "MTC16060097",
#                             "MTC160600DB",
#                             "MTC160600GD",
#                             "MTC1606008Z",
#                             "MTC160600RJ",
#                             "MTC1606008V",
#                             "MTC160600V3",
#
#                 ]
#
# }
#   )