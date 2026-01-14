from __future__ import annotations

import json
import logging
import os
import re
import sys
import warnings
from datetime import datetime
from functools import partial, wraps
from io import BytesIO
from pathlib import Path
from typing import TypedDict, Optional

import awswrangler as wr
import boto3
import pandas as pd
import prefect
from common_tasks.dataframes import DataFrameUploadTask
from dateutil.relativedelta import relativedelta
from prefect import Flow, task, Parameter
from prefect.engine.results import LocalResult, S3Result
from prefect.engine.signals import FAIL
from prefect.executors import LocalDaskExecutor, LocalExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import Docker
from prefect.triggers import any_failed
from sqlalchemy import create_engine, text, bindparam, Date, inspect
from sqlalchemy.sql import quoted_name
from sqlalchemy.types import VARCHAR

from common import sec
from common.config import TemplateConfig, Config
from common.sec import T_ENV, T_FLOW
from common.utils import add_wheels


class GenArgsOutput(TypedDict):
    company_name: str
    run_identifier: str
    c_type_collection: list
    pricing_model: str
    end_of_sfc_date: pd.Timestamp
    atr_goaling_date: pd.Timestamp
    atr_iteration: str
    engagement_id: list[str]
    end_customer_gu_id: list[str]
    contract_bill_to_guid: list[str]
    atr_table_name: str
    coverage_table_name: str
    multis_table_name: str
    notes_table_name: str
    flat_table_name: str
    core_table_name: str
    output_uri: str


class SnowflakeQueryFilter(logging.Filter):
    """
    Filter for snowflake query logs. Written as class to make checks for if this is already added to a logger easier.
    """

    substr_pattern = re.compile(
        "^(?:rollback|commit|desc table)|(?:current_database|current_schema)",
        re.IGNORECASE,
    )

    def filter(self, record: logging.LogRecord) -> bool:
        """
        Filter for snowflake query logs. Hardcoded parameters for now, but could be made more flexible in __init__.
        """
        if not record.msg.startswith("query:"):
            return False
        record_args = record.args
        try:
            query = record_args[0]
        except (IndexError, TypeError):
            return False
        if not isinstance(query, str):
            return False
        if self.substr_pattern.search(query):
            return False
        record.msg = "query:\n%s"
        return True


def log_queries(func):
    @wraps(func)
    def wrapped_func(*args, **kwargs):
        stream_handler = logging.StreamHandler(sys.stdout)
        sf_logger = logging.getLogger("snowflake.connector.cursor")
        sf_logger.setLevel(logging.INFO)
        if not sf_logger.filters:
            sf_logger.addFilter(SnowflakeQueryFilter())
        if not sf_logger.handlers:
            sf_logger.addHandler(stream_handler)
        return func(*args, **kwargs)

    return wrapped_func


def clean_string(s: str) -> str:
    return Config.RE_CLEAN_STRING.sub("_", s).lower()


def fix_numbers(s: pd.Series) -> pd.Series:
    s = pd.to_numeric(s.convert_dtypes(), errors="coerce")
    s = pd.to_numeric(s, errors="coerce").convert_dtypes()
    return s


@task(log_stdout=True)
def get_json_from_s3(bucket, key):
    s3 = boto3.resource("s3")
    obj = s3.Object(bucket, key)
    data = obj.get()["Body"].read().decode("utf-8")
    return json.loads(data)


def prep_data(df: pd.DataFrame, mapping: dict) -> pd.DataFrame:
    # run after standard rename
    # pandas_data_type_map = get_json_from_s3(
    #     "canvas-data-types", "pandas_data_type_map.json"
    # )
    for k in df.columns:
        d_type = mapping.get(k)
        if d_type in ("Int64", "float64", "int"):
            df[k] = fix_numbers(df[k])
        elif d_type in ("datetime64[ns]",):
            df[k] = pd.to_datetime(df[k], errors="coerce")
        elif d_type in ("str",):
            df[k] = df[k].astype("str")
        else:
            df[k] = df[k].astype("str")
    df = df.replace(["nan", "None", "<NA>"], pd.NA)
    return df


@task(log_stdout=True)
@log_queries
def gen_args(
    file_location: str,
    bucket_name: Optional[list[str]],
    env: T_ENV,
    flow_env: T_FLOW,
    request_id: Optional[str],
    schema: str,
    wh: str,
    data_mapping: dict,
) -> GenArgsOutput:
    def fetch_template(fp: str) -> BytesIO:
        """
        Depending on flow_env, read from S3 or local file system
        """
        file_io = BytesIO()
        if flow_env == "dev":
            with open(fp, "rb") as f:
                file_io.write(f.read())
            file_io.seek(0)
            return file_io
        wr.s3.download(fp, file_io)
        file_io.seek(0)
        return file_io

    if isinstance(bucket_name, list):
        bucket_name = bucket_name[0]

    def assign_engine(f: str):
        if f.endswith(".xlsb"):
            return "pyxlsb"
        else:
            return "openpyxl"

    file_obj = fetch_template(file_location)

    atr_engine = assign_engine(file_location)
    reader = pd.ExcelFile(file_obj, engine=atr_engine)

    # Filter UserWarning about Data Validation
    warnings.filterwarnings(
        action="ignore",
        message="Data Validation",
        module="openpyxl",
        category=UserWarning,
    )
    df_kv = reader.parse(
        sheet_name=TemplateConfig.KV_SHEET,
        usecols=TemplateConfig.KV_SHEET_COLS,
    )
    df_cisco_ready = reader.parse(
        sheet_name=TemplateConfig.CISCO_READY_SHEET,
        usecols=TemplateConfig.CISCO_READY_SHEET_COLS,
    )

    key_to_idx = {str(v).lower(): k for k, v in df_kv["key"].to_dict().items()}
    company_name = df_kv.at[key_to_idx["name of company"], "value"]
    company_name = clean_string(company_name).replace(" ", "_").lower()

    if flow_env == "dev":
        flow_name = prefect.context.get("flow_name", "atr-cam-flow")

        result_uri = (
            Path.home()
            / ".prefect"
            / flow_name
            / f"{request_id}_{company_name}_atr.xlsx"
        )
    else:
        if not bucket_name:
            raise FAIL(msg="No bucket name provided")
        result_uri = f"s3://{bucket_name}/{env}/output_files/atr_flow/{request_id}_{company_name}_atr.xlsx"

    df_cisco_ready["instance_id"] = fix_numbers(df_cisco_ready.instance_id)
    df_cisco_ready.dropna(how="all", inplace=True)
    df_cisco_ready = prep_data(df_cisco_ready, mapping=data_mapping)
    # specific to not mess with data canvas definitions
    df_cisco_ready["atr_dollars"] = df_cisco_ready["atr_dollars"].fillna(0)
    df_cisco_ready["atr_dollars"] = pd.to_numeric(
        df_cisco_ready["atr_dollars"], errors="raise"
    )

    # remove dups by aggregating data by instance_id
    df_cisco_ready = df_cisco_ready.groupby("instance_id").sum().reset_index()
    df_cisco_ready["is_atr_data"] = "Y"
    # IF there are dups in CR data let it fail
    df_cisco_ready = df_cisco_ready.set_index("instance_id")

    df_cisco_contracts = reader.parse(
        sheet_name=TemplateConfig.CONTRACTS_SHEET,
        usecols=TemplateConfig.CONTRACTS_SHEET_COLS,
    )

    df_cisco_contracts = df_cisco_contracts.drop_duplicates(
        subset=["contract_number", "contract_type"]
    )
    df_cisco_contracts = prep_data(df_cisco_contracts, mapping=data_mapping)
    df_cisco_contracts["contract_number"] = fix_numbers(
        df_cisco_contracts.contract_number
    )
    df_cisco_contracts.dropna(how="any", subset=["contract_number"], inplace=True)
    ctypes = df_cisco_contracts.contract_type.unique()
    c_type_collection = []

    for i, t in enumerate(ctypes):
        c_type_collection.append(
            [
                clean_string(ctypes[i]),
                list(
                    df_cisco_contracts.contract_number[
                        df_cisco_contracts.contract_type == t
                    ].values
                ),
            ]
        )

    engine = create_engine(
        sec.get_sf_pw(sec.check_env(env), wh, schema),
        connect_args={"log_max_query_length": 10_000},
    )

    atr_table_name = Config.atr_table_name(request_id)

    inspector = inspect(engine)
    if inspector.has_table(atr_table_name, schema=schema):
        print(f"Dropping table {atr_table_name}")
        with engine.connect() as conn:
            conn.execute(
                text("DROP TABLE identifier(:tbl)").bindparams(
                    bindparam("tbl", quoted_name(atr_table_name, False))
                )
            )

    df_cisco_ready.reset_index(inplace=True, drop=False)
    print(f"Writing {len(df_cisco_ready)} rows to {atr_table_name}")
    with engine.connect() as conn:
        df_cisco_ready.to_sql(
            atr_table_name,
            schema=schema,
            con=conn,
            index=False,
            if_exists="replace",
            chunksize=15000,
        )

    pricing_model = df_kv.at[key_to_idx["pricing_model"], "value"]
    if pd.isna(pricing_model) or pricing_model is None or pricing_model.strip() == "":
        pricing_model = "NONE"

    pricing_model = clean_string(pricing_model).replace("_", " ").upper()

    end_of_sfc_date = df_kv.at[key_to_idx["end_of_sfc_date"], "value"]
    end_of_sfc_date = pd.to_datetime(end_of_sfc_date)

    atr_goaling_date = df_kv.at[key_to_idx["atr_goaling_date"], "value"]
    atr_goaling_date = pd.to_datetime(atr_goaling_date)
    atr_iteration = df_kv.at[key_to_idx["atr_iteration"], "value"]
    atr_iteration = "" if pd.isna(atr_iteration) else atr_iteration.strip()

    engagement_id = str(df_kv.at[key_to_idx["mce_engagement_id"], "value"]).split(",")
    engagement_id = [re.sub(r"[^0-9]", "", i) for i in engagement_id]
    engagement_id = [i.strip() for i in engagement_id]

    end_customer_gu_id = str(df_kv.at[key_to_idx["end_customer_gu_id"], "value"]).split(
        ","
    )
    end_customer_gu_id = [re.sub(r"[^0-9]", "", i) for i in end_customer_gu_id]
    end_customer_gu_id = [i.strip() for i in end_customer_gu_id]

    contract_bill_to_guid = str(
        df_kv.at[key_to_idx["contract_bill_to_guid"], "value"]
    ).split(",")
    contract_bill_to_guid = [re.sub(r"[^0-9]", "", i) for i in contract_bill_to_guid]
    contract_bill_to_guid = [i.strip() for i in contract_bill_to_guid]

    result = GenArgsOutput(
        company_name=company_name,
        run_identifier=request_id,
        c_type_collection=c_type_collection,
        pricing_model=pricing_model,
        end_of_sfc_date=end_of_sfc_date,
        atr_goaling_date=atr_goaling_date,
        atr_iteration=atr_iteration,
        engagement_id=engagement_id,
        end_customer_gu_id=end_customer_gu_id,
        contract_bill_to_guid=contract_bill_to_guid,
        atr_table_name=atr_table_name,
        coverage_table_name=Config.coverage_table_name(request_id),
        multis_table_name=Config.multis_table_name(request_id),
        notes_table_name=Config.notes_table_name(request_id),
        flat_table_name=Config.flat_table_name(request_id),
        core_table_name=Config.core_table_name(request_id),
        output_uri=result_uri,
    )
    print("ATR Flow Params".center(60, "="))
    print("Parameters".center(40, "-"))
    print(f"Company Name: {result['company_name']}".ljust(8))
    print(f"Run Identifier: {result['run_identifier']}".ljust(8))
    print(f"Pricing Model: {result['pricing_model']}".ljust(8))
    print(f"Output URI: {result['output_uri']}".ljust(8))
    print("Table Names".center(40, "-"))
    print(f"ATR Table: {result['atr_table_name']}".ljust(8))
    print(f"Coverage Table: {result['coverage_table_name']}".ljust(8))
    print(f"Multis Table: {result['multis_table_name']}".ljust(8))
    print(f"Notes Table: {result['notes_table_name']}".ljust(8))
    print(f"Flat Table: {result['flat_table_name']}".ljust(8))
    print(f"Core Table: {result['core_table_name']}".ljust(8))
    print("".center(60, "-"))
    return result


@task(log_stdout=True)
@log_queries
def gen_coverage_data(
    scope_instance_tbl_name: str,
    coverage_table_name: str,
    env: T_ENV,
    wh: str,
    schema: str,
) -> str:
    coverage_fqn = f"{schema}.{coverage_table_name}".lower()
    coverage_table_param = bindparam("coverage_table", quoted_name(coverage_fqn, False))

    scoped_inst_fqn = f"{schema}.{scope_instance_tbl_name}".lower()
    scoped_inst_param = bindparam("scoped_inst", quoted_name(scoped_inst_fqn, False))

    core_sql = text(
        f"""
    create or replace transient table identifier(:coverage_table) as
    
    with scope as (select   distinct instance_id from identifier(:scoped_inst)
    ),coverage as(
                select PARENT_INSTANCE_ID, COVERED_LINE_ID, cvd_line.INSTANCE_ID, cvd_line.CONTRACT_ID, cvd_line.SERVICE_LINE_ID, STS_CODE, START_DATE, END_DATE, CLE_ID_RENEWED, CLE_ID_RENEWED_TO,
                DNR_FLAG, PRICE_NEGOTIATED, PRICE_UNIT, MAINTENANCE_PO_NUMBER, MAINTENANCE_SO_NUMBER, DATE_TERMINATED,
                LINE_NUMBER, LINE_CREATION_DATE, LINE_LAST_UPDATE_DATE, LINE_CREATED_BY, LINE_LAST_UPDATED_BY, cvd_line.CURRENCY_CODE, DATE_RENEWED,
                cvd_line.CVD_ATTRIBUTE14, cvd_line.CVD_ATTRIBUTE15, cvd_line.DUPLICATE_COVERAGE_FLAG, DUPLICATE_CVG_REF_LINE_ID, USD_PRICE_NEGOTIATED, USD_PRICE_UNIT,
                cvd_line.USD_CONVERSION_RATE, COVERED_LINE_MOVED_FROM, COVERED_LINE_MOVED_TO, cvd_line.SAVA, MAPPED_SKU, OFFER_TYPE, ACTUAL_PRICE_NEGOTIATED,
                 QUOTE_NUMBER, OFFER_ATO_SUITE_NAME,   TERMINATION_CREDIT, last_coverage_date ,
                case
                when current_date between cvd_line.START_DATE and last_coverage_date then 'L'
                when current_date < last_coverage_date then 'Z'
                when current_date > last_coverage_date then 'P'
                end as flag_we_use,
               contract_header.VENDOR_ORGANIZATION_ID                                                    as contract_ou_id,
               contract_header.VENDOR_ORGANIZATION_NAME                                                  as contract_ou_name,
               contract_header.MEU_ALLOWED_FLAG                                                          as meu_allowed_contract_flag,
               contract_header.CONTRACT_INSTALL_GU_COUNT ,
               CPS_DSCI_ARCHIVE.FIX_DATES(contract_header.contract_start_date)                              as contract_start_date,                --408
               CPS_DSCI_ARCHIVE.FIX_DATES(contract_header.contract_end_date)                             as contract_end_date,                  --204
               contract_header.CXEA_FLAG                                                                 as cx_ea_flag,                         --37 , 638
               contract_header.BILLTO_CR_PARTY_NAME                                                      as product_bill_to_party_name,         --26, 395
               contract_header.BILLTO_PARENT_PARTY_ID                                                    as product_bill_to_party_id_parent,    --24
               contract_header.bill_to_site_use_id                                                       as contract_bill_to_id,--27
               contract_header.bill_to_address1                                                          as contract_bill_to_address,
               contract_header.bill_to_city                                                              as contract_bill_to_city,
               contract_header.bill_to_country                                                           as contract_bill_to_country,
               contract_header.bill_to_state_prov                                                        as contract_bill_to_province,
               contract_header.BILL_TO_POSTAL_CODE                                                       as contract_bill_to_postal_code,
               contract_header.contract_number,                                                                                                 --38
               contract_header.service_line_name                                                         as service_level,                      --128
               contract_header.contract_sts_code                                                         as contract_status,                    --39
               contract_header.BILL_TO_CUSTOMER_NAME                                                     as contract_bill_to_name,              --33
               contract_header.BILLTO_GU_ID                                                              as contract_bill_to_gu_id,             --35, 575
               contract_header.BILLTO_GU_NAME                                                            as contract_bill_to_gu_name,--199, 36
               contract_header.BILLTO_PARENT_PARTY_NAME                                                  as contract_bill_to_party_name_parent, -- 32, 569
               contract_header.Coverage_template_desc                                                    as service_level_description,
               contract_header.service_brand_code                                                        as cisco_branded_service_tag,
               CPS_DSCI_ARCHIVE.FIX_DATES(contract_header.coverage_begin_date)                          as sla_start_date,                     --338, 606
               CPS_DSCI_ARCHIVE.FIX_DATES(contract_header.coverage_end_date)                            as sla_end_date,                       -- 339, 607
               contract_header.contract_attribute16                                                      as mss_contract_flag,                  --298, 596
               contract_header.service_line_sts_code                                                     as sla_status,                         --340, 608,
               contract_header.billto_begeo_name                                                         as service_partner,                    --344, 609
               contract_header.SERVICES_FULL_COVERAGE                                                    as SFC_FLAG,                           --131
               contract_header.BILL_TO_ORG_ID
                from   identifier(:scoped_inst) scope join  CPS_DSCI_BR.CAM_DS_INSTANCE_DETAIL  ib  on (ib.INSTANCE_ID=scope.INSTANCE_ID)
                left join   CPS_DSCI_BR.CAM_DS_CVDPRDLINE_DETAIL  cvd_line on
                    (
                    ib.INSTANCE_ID  = cvd_line.INSTANCE_ID  --NOT mce but live c3
                    and
                    nvl(cvd_line.EDWSF_SOURCE_DELETED_FLAG, 'N') = 'N'
                    AND
                    NVL(IB.EDWSF_SOURCE_DELETED_FLAG , 'N') = 'N'
                    )
                left join EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_SAHDR_CORE contract_header on
                ( cvd_line.contract_id = contract_header.contract_id and
                  cvd_line.service_line_id = contract_header.service_line_id   and
                  nvl(contract_header.EDWSF_SOURCE_DELETED_FLAG, 'N') = 'N'
                )

                union
                select PARENT_INSTANCE_ID, COVERED_LINE_ID, cvd_lineh.INSTANCE_ID, cvd_lineh.CONTRACT_ID, cvd_lineh.SERVICE_LINE_ID, STS_CODE, START_DATE, END_DATE, CLE_ID_RENEWED, CLE_ID_RENEWED_TO,
                DNR_FLAG, PRICE_NEGOTIATED, PRICE_UNIT, MAINTENANCE_PO_NUMBER, MAINTENANCE_SO_NUMBER, DATE_TERMINATED,
                LINE_NUMBER, LINE_CREATION_DATE, LINE_LAST_UPDATE_DATE, LINE_CREATED_BY, LINE_LAST_UPDATED_BY, cvd_lineh.CURRENCY_CODE, DATE_RENEWED,
                cvd_lineh.CVD_ATTRIBUTE14,cvd_lineh.CVD_ATTRIBUTE15, cvd_lineh.DUPLICATE_COVERAGE_FLAG, DUPLICATE_CVG_REF_LINE_ID, USD_PRICE_NEGOTIATED, USD_PRICE_UNIT,
                cvd_lineh.USD_CONVERSION_RATE, COVERED_LINE_MOVED_FROM, COVERED_LINE_MOVED_TO, cvd_lineh.SAVA, MAPPED_SKU, OFFER_TYPE, ACTUAL_PRICE_NEGOTIATED,
                QUOTE_NUMBER, OFFER_ATO_SUITE_NAME,   TERMINATION_CREDIT,  last_coverage_date ,
               'P' as flag_we_use,
               contract_header.VENDOR_ORGANIZATION_ID                                                    as contract_ou_id,
               contract_header.VENDOR_ORGANIZATION_NAME                                                  as contract_ou_name,
               contract_header.MEU_ALLOWED_FLAG                                                          as meu_allowed_contract_flag,
               contract_header.CONTRACT_INSTALL_GU_COUNT ,
               CPS_DSCI_ARCHIVE.FIX_DATES(contract_header.contract_start_date)                           as contract_start_date,                --408
               CPS_DSCI_ARCHIVE.FIX_DATES(contract_header.contract_end_date)                             as contract_end_date,                  --204
               contract_header.CXEA_FLAG                                                                 as cx_ea_flag,                         --37 , 638
               contract_header.BILLTO_CR_PARTY_NAME                                                      as product_bill_to_party_name,         --26, 395
               contract_header.BILLTO_PARENT_PARTY_ID                                                    as product_bill_to_party_id_parent,    --24
               contract_header.bill_to_site_use_id                                                       as contract_bill_to_id,--27
               contract_header.bill_to_address1                                                          as contract_bill_to_address,
               contract_header.bill_to_city                                                              as contract_bill_to_city,
               contract_header.bill_to_country                                                           as contract_bill_to_country,
               contract_header.bill_to_state_prov                                                        as contract_bill_to_province,
               contract_header.BILL_TO_POSTAL_CODE                                                       as contract_bill_to_postal_code,
               contract_header.contract_number,                                                                                                 --38
               contract_header.service_line_name                                                         as service_level,                      --128
               contract_header.contract_sts_code                                                         as contract_status,                    --39
               contract_header.BILL_TO_CUSTOMER_NAME                                                     as contract_bill_to_name,              --33
               contract_header.BILLTO_GU_ID                                                              as contract_bill_to_gu_id,             --35, 575
               contract_header.BILLTO_GU_NAME                                                            as contract_bill_to_gu_name,--199, 36
               contract_header.BILLTO_PARENT_PARTY_NAME                                                  as contract_bill_to_party_name_parent, -- 32, 569
               contract_header.Coverage_template_desc                                                    as service_level_description,
               contract_header.service_brand_code                                                        as cisco_branded_service_tag,
               CPS_DSCI_ARCHIVE.FIX_DATES(contract_header.coverage_begin_date)                           as sla_start_date,                     --338, 606
               CPS_DSCI_ARCHIVE.FIX_DATES(contract_header.coverage_end_date)                             as sla_end_date,                       -- 339, 607
               contract_header.contract_attribute16                                                      as mss_contract_flag,                  --298, 596
               contract_header.service_line_sts_code                                                     as sla_status,                         --340, 608,
               contract_header.billto_begeo_name                                                         as service_partner,                    --344, 609
               contract_header.SERVICES_FULL_COVERAGE                                                    as SFC_FLAG,                           --131
               contract_header.BILL_TO_ORG_ID
                from   identifier(:scoped_inst) scope join CPS_DSCI_BR.CAM_DS_INSTANCE_DETAIL ib  on (ib.INSTANCE_ID=scope.INSTANCE_ID)
                join   CPS_DSCI_BR.CAM_DS_CVDPRDLINE_DETAIL_H  cvd_lineh on(
                    ib.INSTANCE_ID  = cvd_lineh.INSTANCE_ID
                    and  NVL(cvd_lineh.EDWSF_SOURCE_DELETED_FLAG, 'N') = 'N'
                    AND  NVL(IB.EDWSF_SOURCE_DELETED_FLAG , 'N') = 'N'
                    )
                left join EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_SAHDR_CORE contract_header on
                    ( cvd_lineh.contract_id = contract_header.contract_id and
                      cvd_lineh.service_line_id = contract_header.service_line_id   and
                      nvl(contract_header.EDWSF_SOURCE_DELETED_FLAG, 'N') = 'N'
                    )
                    
            )
        select  COVERED_LINE_ID, PARENT_INSTANCE_ID, INSTANCE_ID, CONTRACT_ID, SERVICE_LINE_ID, STS_CODE, START_DATE, END_DATE, CLE_ID_RENEWED, CLE_ID_RENEWED_TO,
                DNR_FLAG, PRICE_NEGOTIATED, PRICE_UNIT, MAINTENANCE_PO_NUMBER, MAINTENANCE_SO_NUMBER, DATE_TERMINATED,
                LINE_NUMBER, LINE_CREATION_DATE, LINE_LAST_UPDATE_DATE, LINE_CREATED_BY, LINE_LAST_UPDATED_BY, coverage.CURRENCY_CODE, DATE_RENEWED,
                CVD_ATTRIBUTE14,CVD_ATTRIBUTE15, DUPLICATE_COVERAGE_FLAG, DUPLICATE_CVG_REF_LINE_ID, USD_PRICE_NEGOTIATED, USD_PRICE_UNIT,
                USD_CONVERSION_RATE, COVERED_LINE_MOVED_FROM, COVERED_LINE_MOVED_TO, SAVA, MAPPED_SKU, OFFER_TYPE, ACTUAL_PRICE_NEGOTIATED,
                 QUOTE_NUMBER, OFFER_ATO_SUITE_NAME,   TERMINATION_CREDIT, last_coverage_date ,
               flag_we_use,
               contract_ou_id,
               contract_ou_name,
               meu_allowed_contract_flag,
               CONTRACT_INSTALL_GU_COUNT ,
               contract_start_date ,                --408
               contract_end_date ,                  --204
               cx_ea_flag,                         --37 , 638
               product_bill_to_party_name,         --26, 395
               product_bill_to_party_id_parent,    --24
               contract_bill_to_id,--27
               contract_bill_to_address,
               contract_bill_to_city,
               contract_bill_to_country,
               contract_bill_to_province,
               contract_bill_to_postal_code,
               contract_number,                                                                                                 --38
               service_level,                      --128
               contract_status,                    --39
               contract_bill_to_name,              --33
               contract_bill_to_gu_id,             --35, 575
               contract_bill_to_gu_name,--199, 36
               contract_bill_to_party_name_parent, -- 32, 569
               service_level_description,
               cisco_branded_service_tag,
               sla_start_date   ,                 --338, 606
               sla_end_date  ,                            -- 339, 607
               mss_contract_flag,                  --298, 596
               sla_status,                         --340, 608,
               service_partner,                    --344, 609
               SFC_FLAG,                           --131
              BILL_TO_ORG_ID,
             row_number() over ( partition by coverage.INSTANCE_ID order by coverage.flag_we_use, coverage.COVERED_LINE_ID  desc) as orderv_current
            from coverage where COVERED_LINE_ID is not null;

    """
    )
    engine = create_engine(
        sec.get_sf_pw(sec.check_env(env), wh, schema),
        connect_args={"log_max_query_length": 10_000},
    )
    stmt = core_sql.bindparams(coverage_table_param, scoped_inst_param)

    with engine.connect() as conn:
        pd.read_sql(stmt, conn)

    return coverage_table_name


@task(log_stdout=True)
@log_queries
def coverage_enrichments(
    coverage_table_name: str,
    eid: int,
    env: T_ENV,
    wh: str,
    schema: str,
) -> str:
    coverage_table_fqn = f"{schema}.{coverage_table_name}".upper()
    sql = f"""
    
    alter table {coverage_table_fqn} add column   is_mss_available varchar default 'NA';
    alter table {coverage_table_fqn} add column   existing_mss_coverage varchar default 'NA';
    alter table {coverage_table_fqn}  add column  mss_available_to_date date;
    alter table {coverage_table_fqn}  add column  mss_service_available varchar default 'NA';
    
    --this gets the actual MSS SL available (CURRENT MSS is NA which is an issue)aron TODO 
    update {coverage_table_fqn} c
         set  c.existing_mss_coverage = case when o.MSS_SERVICE_LEVEL_GROUP is null then '-' else 'MSS_COVERAGE' end,
              c.mss_service_available = o.MSS_SERVICE_LEVEL_GROUP
         from (     select arrayagg(distinct standard_service_level_group) within group (order by standard_service_level_group) as sslg,
                           mss_service_level_group
                    from CPS_DSCI_ARCHIVE.MSS_PCODE_MAPPING mss
                    group by mss_service_level_group
              ) o
         where ARRAYS_OVERLAP(array_construct(c.service_level ), o.sslg);

    update {coverage_table_fqn}   c
             set  c.is_mss_available = o.assessment_status ,
                  c.mss_available_to_date = o.assessment_support_end_date
             from SERVICES_DB.SERVICES_MSS_BR.MSS_OPPORTUNITIES o
             where c.instance_id = o.instance_id;
      alter table {coverage_table_fqn} add column  am_service_contract_type varchar default 'NA';
      alter table {coverage_table_fqn} add column  am_offer_type varchar default 'NA';
      alter table {coverage_table_fqn} add column  am_contract_allowed_srv_lvl varchar default 'NA';

    update {coverage_table_fqn} c
         set  c.am_service_contract_type = o.CONTRACT_TYPE,
              c.am_offer_type = o.AM_SERVICE_TYPE,
              c.am_contract_allowed_srv_lvl = o.SERVICE_LEVEL
         from (
                  with i as (
                      select distinct  replace(c.contract_number, ' ',',') as contract_number, c.CONTRACT_TYPE, c.AM_SERVICE_TYPE, c.SERVICE_LEVEL
                      from  CPS_BIA_BR.DATA_CANVAS_CONTRACT_DATA_V c
                      where id= concat('CAM-',{eid}::int) and nvl(contract_del_flag, 'N') != 'Y'
                  )
                  select  try_to_number(trim(value))::bigint as this_value ,
                          listagg(distinct i.CONTRACT_TYPE,',')  as CONTRACT_TYPE ,
                          listagg(distinct i.AM_SERVICE_TYPE,',') as AM_SERVICE_TYPE ,
                          listagg(distinct i.SERVICE_LEVEL,',') as SERVICE_LEVEL,
                         'contract_number' as src
                  from i , lateral split_to_table(i.contract_number, ',')
                  where trim(value)  != ''   and try_to_number(trim(value)) is not null
                    group by try_to_number(trim(value))::bigint
              ) o
         where try_to_number(c.CONTRACT_NUMBER) = o.this_value and try_to_number(c.CONTRACT_NUMBER) is not null 
         ;
    
    
    
    

    -- WE ARE REALLY OVER WRITING THIS VALUE BC IT NEEDED TO LOOK ACROSS FUTURE AND CURRENT VS THE VIEW DEF OF MAX(END | TERM)
    
    UPDATE {coverage_table_fqn} T SET  T.last_coverage_date=CORRECT.MX
    FROM (
        SELECT max(LAST_COVERAGE_DATE) MX, INSTANCE_ID
        FROM {coverage_table_fqn}
        WHERE FLAG_WE_USE IN ('L', 'Z')
        GROUP BY INSTANCE_ID
    ) CORRECT
    WHERE T.INSTANCE_ID = CORRECT.INSTANCE_ID;

    alter table {coverage_table_fqn}  add column last_coverage_fiscal_quarter varchar(40);
    update  {coverage_table_fqn}  i set last_coverage_fiscal_quarter = d.FISCAL_QTR_SORTED_NAME
        from CPS_DSCI_ARCHIVE.DIM_DATE_NEW d where d.DATE = i.last_coverage_date;
         """

    engine = create_engine(
        sec.get_sf_pw(sec.check_env(env), wh, schema),
        connect_args={"log_max_query_length": 10_000},
    )
    print("Run coverage_enrichments")
    with engine.begin() as conn:
        for s in sql.split(";"):
            conn.execute(text(s))
    return coverage_table_name


@task(log_stdout=True)
@log_queries
def gen_current_data(
    multis_table_name: str,
    scope_instance_table_name: str,
    coverage_table_name: str,
    env: T_ENV,
    wh: str,
    schema: str,
) -> str:
    multis_table_fqn = f"{schema}.{multis_table_name}".upper()
    scope_instance_fqn = f"{schema}.{scope_instance_table_name}".upper()
    coverage_table_fqn = f"{schema}.{coverage_table_name}".upper()
    core_sql = f"""
        create or replace Transient table {multis_table_fqn} as
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
             ), scope as (
               select   instance_id
              --listagg(distinct source, ',') within group (order by source ) as sources,
              --listagg(distinct evidence, ',') within group (order by evidence ) as evidence
                from {scope_instance_fqn}
                group by instance_id
            )
        SELECT IB.instance_id,                                                                                                           --280
               IB.instance_number,                                                                                                       -- 282
               --a.deal_id, --377
               ib.deal_id,                                                                                                               --377 , 45
               nvl(coverage.USD_PRICE_UNIT, coverage.PRICE_UNIT)                                  as usd_prorated_list_price,            --504
               nvl(coverage.USD_PRICE_UNIT, coverage.PRICE_UNIT) * ib.QUANTITY                    as usd_extended_list_price,            -- 505
               ib.PARENT_INSTANCE_ID,                                                                                                    --108 309
               --IB.covered_status, --219 42
               --CASE  WHEN ib.covered_status = 'A' THEN 'COVERED' ELSE 'UNCOVERED' END as coverage_status,
               case
                   when IB.covered_status = 'A' then 'ACTIVE'
                   when IB.covered_status = 'I' then 'EXPIRED'
                   when IB.covered_status = 'N' then 'NEVER COVERED'
                   end                                                                            as coverage_status,


               ib.INSTANCE_STATUS_DESC                                                            as installed_base_status,              --82 263
               case when ib.serial_number is null then 'F' else 'T' end                           as serialized_flag,                    --126, 602
               nvl(ib.serial_number, ib.dup_serial_number)                                        as serial_number,                      -- 125 , 334
               CASE
                   WHEN NVL(ib.duplicate_coverage_flag, 'N') = 'N' THEN 'No'
                   ELSE 'Yes'
                   END                                                                            as duplicate_coverage_flag,            --578 , 232
               CASE
                   WHEN ib.instance_status_desc IN ('Replace Pend-DEINSTALLED', 'Replaced-DEINSTALLED',
                                                    'RMA_inProgress') --Replaced-DEINSTALLED, Replace Pend-DEINSTALLED, RMA_inProgress  via : EDW_SERVICE_ETL_DB.ss.CSF_CSI_INSTANCE_STATUSES
                       THEN
                       NVL(replace_ib.serial_number, replace_ib.dup_serial_number)
                   ELSE
                       NULL
                   END                                                                            as replaced_serial_number,             --601 , 331
               ib.dup_serial_number,                                                                                                     -- 490, 491
               coverage.maintenance_po_number,                                                                                           -- 492, 291
               NVL(ib.duplicate_ib_flag, 'N')                                                     as duplicate_ib_flag,                  -- 50
               ib.duplicate_ib_ref_instance_id,                                                                                          --518, 634
               CASE
                   WHEN IB.item_type_flag = 'S' THEN 'Standalone'
                   WHEN IB.item_type_flag = 'P' THEN 'Parent'
                   WHEN IB.item_type_flag = 'C' THEN 'Child'
                   ELSE NULL
                   END                                                                               product_relationship,               --493, 322 -- resolve to add to feed as new metric vs dynamic creation in canvas


               CASE
                   WHEN IB.item_type_flag = 'S' THEN 'Standalone'
                   WHEN IB.item_type_flag = 'P' THEN 'Major'
                   WHEN IB.item_type_flag = 'C' THEN 'Minor'
                   ELSE NULL
                   END                                                                            as Config_Type,                        --489,195,

               ib.item_name                                                                       AS pid,                                --85, 230
               --a.item_type,
               item.item_type,                                                                                                           --87
               CPS_DSCI_ARCHIVE.FIX_DATES(coverage.END_DATE::date)                                as last_coverage_end_date,             --52 , 403
               CPS_DSCI_ARCHIVE.FIX_DATES(coverage.START_DATE::date)                              as coverage_start_date,                -- 149 ,313
               CASE
                   WHEN coverage.STS_CODE NOT IN ('ACTIVE', 'SIGNED')
                       OR coverage.STS_CODE IS NULL OR ((coverage.last_coverage_date::date - current_date()) < 0)
                       THEN 'NA (Not Eligible)'
                   ELSE
                       CASE
                           WHEN datediff(day, CURRENT_TIMESTAMP, coverage.last_coverage_date) BETWEEN 0 AND 30
                               THEN 'Expiration within 30 Days (1 Month)'
                           WHEN datediff(day, CURRENT_TIMESTAMP, coverage.last_coverage_date) BETWEEN 31 AND 60
                               THEN 'Expiration within 60 Days (2 Months)'
                           WHEN datediff(day, CURRENT_TIMESTAMP, coverage.last_coverage_date) BETWEEN 61 AND 90
                               THEN 'Expiration within 90 Days (3 Months)'
                           WHEN datediff(day, CURRENT_TIMESTAMP, coverage.last_coverage_date) BETWEEN 91 AND 180
                               THEN 'Expiration within 180 Days (6 Months)'
                           WHEN datediff(day, CURRENT_TIMESTAMP, coverage.last_coverage_date) BETWEEN 181 AND 270
                               THEN 'Expiration within 270 Days (9 Months)'
                           WHEN datediff(day, CURRENT_TIMESTAMP, coverage.last_coverage_date) BETWEEN 271 AND 365
                               THEN 'Expiration within 365 Days (12 Months)'
                           WHEN datediff(day, CURRENT_TIMESTAMP, coverage.last_coverage_date) BETWEEN 366 AND 540
                               THEN 'Expiration within 540 Days (18 Months)'
                           WHEN datediff(day, CURRENT_TIMESTAMP, coverage.last_coverage_date) BETWEEN 541 AND 730
                               THEN 'Expiration within 730 Days (24 Months)'
                           WHEN datediff(day, CURRENT_TIMESTAMP, coverage.last_coverage_date) >= 731 OR
                                coverage.last_coverage_date IS NULL THEN 'Expiring after 2 years'
                           END
                   END                                                                            as Coverage_Details_Months,            --209, 576
               CPS_DSCI_ARCHIVE.FIX_DATES(coverage.DATE_TERMINATED::date)                         as product_coverage_termination_date,  --315,92
               --CPS_DSCI_ARCHIVE.FIX_DATES(a.last_date_of_support) as product_last_date_of_support_ldos,
               CPS_DSCI_ARCHIVE.FIX_DATES(item.last_date_of_support::date)                        as last_date_of_support,               --89, 319
               case
                   when item.mapped_to_service_flag = 'YES WITH SPM' then 'T'
                   else 'F' end                                                                   as mapped_to_service_flag,             --98, 293
               item.PRODUCT_FAMILY_MFG_DESCR,-- 494 , 636
               item.product_family_description,                                                                                          --111, 635
               item.DESCRIPTION                                                                   as product_description,                -- 519, 316
               item.product_family,                                                                                                      --110 , 318
               item.ib_product_type                                                               as product_type,--60, 325
               ib.QUANTITY,
               coverage.PRICE_NEGOTIATED,                                                                                                --495  637 alt location vs nasty cte
               item.service_list_price                                                            as service_list_price,--130 , 342
               item.product_list_price,                                                                                                  --113, 320
               item.technology_group,                                                                                                    --156. 618
               item.business_entity_name_top                                                      as architecture,                       --499 , 160
               item.sub_business_entity_name_top                                                  as sub_architecture,--496 , 360
               item.BUSINESS_ENTITY_DESC_TOP                                                      as architecture_d,--497 , 161
               item.SUB_BUSINESS_ENTITY_DESC_TOP                                                  as sub_architecture_d,--498 , 361
               ------------------------------------------------------------------------------------------
               --a.install_party_name,
               isite.party_name                                                                   as installed_at_customer_name,         --74
               --a.install_address1, a.install_address2  as installed_at_address_lines,--500
               isite.address1 || ' ' || NVL(isite.address2, '')                                   as installed_at_address_lines,--500, 265
               --a.install_state_province,
               isite.state                                                                        as installed_at_province,              --76
               ---a.install_city,
               isite.city                                                                         as installed_at_city,--63
               --a.install_postal_code,
               isite.postal_code                                                                  as installed_at_postal_code,           --75
               --a.install_country,
               isite.COUNTRY                                                                      as installed_at_country,--65
               --a.install_gu_id,
               isite.gu_id                                                                        as installed_at_gu_id,--68
               -- a.install_gu_name,
               isite.gu_name                                                                      as installed_at_gu_name,               -- 69
               isite.PARENT_PARTY_ID                                                              as installed_at_party_id_parent,       --72
               isite.PARENT_PARTY_NAME                                                            as installed_at_party_name_parent,     --73
               isite.cr_party_id                                                                  as installed_at_party_id,              --501
               isite.cr_party_name                                                                as installed_at_party_id_name,         --502
               --a.install_at_site_use_id,
               isite.SITE_USE_ID                                                                  as installed_at_site_id,               -- 61
               ------------------------------------------------------------------------------------------
               coverage.product_bill_to_party_name                                                      as product_bill_to_party_name,         --26, 395
               -- a.bill_to_parent_party_id,
               --coverage.bill_to_parent_party_id as bill_to_parent_party_id, --24
               coverage.product_bill_to_party_id_parent                                                    as product_bill_to_party_id_parent,    --24
               -- a.bill_to_parent_party_name,
               -- a.bill_to_site_use_id,
               coverage.contract_bill_to_id                                                       as contract_bill_to_id,--27
               coverage.contract_bill_to_address                                                          as contract_bill_to_address,
               coverage.contract_bill_to_city                                                              as contract_bill_to_city,
               coverage.contract_bill_to_country                                                           as contract_bill_to_country,
               coverage.contract_bill_to_province                                                        as contract_bill_to_province,
               coverage.contract_bill_to_postal_code                                                       as contract_bill_to_postal_code,


               coverage.contract_number,                                                                                                 --38
               coverage.service_level                                                         as service_level,                      --128
               coverage.contract_status                                                         as contract_status,                    --39
               coverage.contract_bill_to_name                                                     as contract_bill_to_name,              --33
               coverage.contract_bill_to_gu_id                                                              as contract_bill_to_gu_id,             --35, 575
               coverage.contract_bill_to_gu_name                                                            as contract_bill_to_gu_name,--199, 36
               coverage.contract_bill_to_party_name_parent                                                  as contract_bill_to_party_name_parent, -- 32, 569

               coverage.service_level_description                                                    as service_level_description,

               coverage.cisco_branded_service_tag                                                        as cisco_branded_service_tag,
               CPS_DSCI_ARCHIVE.FIX_DATES(coverage.sla_start_date)                           as sla_start_date,                     --338, 606
               CPS_DSCI_ARCHIVE.FIX_DATES(coverage.sla_end_date)                             as sla_end_date,                       -- 339, 607

               coverage.mss_contract_flag                                                      as mss_contract_flag,                  --298, 596
               coverage.sla_status                                                     as sla_status,                         --340, 608,
               coverage.service_partner                                                         as service_partner,                    --344, 609

               coverage.line_number                                                               as coverage_line_number,--312
               coverage.SFC_FLAG                                                    as SFC_FLAG,                           --131

               CPS_DSCI_ARCHIVE.FIX_DATES(coverage.LINE_CREATION_DATE)                            as sa_creation_date,                   --332 mce onlu
               CPS_DSCI_ARCHIVE.FIX_DATES(coverage.LINE_LAST_UPDATE_DATE)                         as sa_last_update_date,                -- 333 moc only
               ------------------------------------------------------------------------------------------
               --a.ship_to_site_use_id,
               st_site.site_use_id                                                                as ship_to_site_id,                    --143
               --a.ship_to_party_name,
               st_site.party_name                                                                 as ship_to_party_name,                 --141
               st_site.PARTY_ID                                                                   as ship_to_party_id,                   --389, 616
               --a.ship_to_gu_id,
               st_site.gu_id                                                                      as ship_to_gu_id,                      --137
               --a.ship_to_gu_name,
               st_site.gu_name                                                                    as ship_to_gu_name,                    --138
               --a.ship_to_parent_party_id,
               st_site.PARENT_PARTY_ID                                                            as ship_to_party_id_parent,            --139
               --a.ship_to_parent_party_name,
               st_site.PARENT_PARTY_NAME                                                          as ship_to_party_name_parent,          --140
               -- a.ship_to_city,
               st_site.city                                                                       as ship_to_city,                       -- 133
               --a.ship_to_state_province,
               st_site.state                                                                      as ship_to_state_province,             -- 145
               --a.ship_to_country,
               st_site.COUNTRY                                                                    as ship_to_country,                    --135
               --a.ship_to_postal_code,
               st_site.postal_code                                                                as ship_to_postal_code,                --142
               st_site.address1 || ' ' || NVL(st_site.address2, '')                               as ship_to_address_lines,
               st_site.cr_party_name                                                              as ship_to_cr_party_name,
               ------------------------------------------------------------------------------------------
               bt_site.party_name                                                                 as bill_to_customer_name,
               bt_site.address1 || ' ' || NVL(bt_site.address2, '')                               as bill_to_address_lines,              -- 402
               bt_site.city                                                                       as bill_to_city,
               bt_site.COUNTRY                                                                    as bill_to_country,
               bt_site.postal_code                                                                as bill_to_postal_code,
               bt_site.state                                                                      as bill_to_state_province,
               bt_site.cr_party_id                                                                as bill_to_cr_party_id,
               bt_site.cr_party_name                                                              as bill_to_cr_party_name,
               --a.bill_to_gu_id,
               bt_site.gu_id                                                                      as product_bill_to_gu_id,              --22, 391
               bt_site.gu_name                                                                    as product_bill_to_gu_name,            -- 23
               bt_site.site_use_id                                                                as product_bill_to_id,                 --27
               ------------------------------------------------------------------------------------------
               coverage.COVERED_LINE_ID                                                           as coverage_line_id,                   --212, 41
               coverage.sts_code,                                                                                                        --151
               coverage.MAINTENANCE_SO_NUMBER                                                     as mso,                                --96


               item.ldos_flag                                                                     as past_ldos,--93 , 639

               CASE WHEN item.item_status_mfg = 'E.O.L.' THEN 'YES' ELSE 'NO' END                 as Product_End_of_Life_Flag,


               item.msa_flag                                                                      as msa_flagged,--359 ,102
               --a.service_billing_sku,
               coverage.MAPPED_SKU                                                                as service_level_sku,                  --603-127
               -- s.contract_cxea_flag,
               coverage.cx_ea_flag                                                                 as cx_ea_flag,                         --37 , 638
               item.business_unit                                                                 as business_entity,                    --186,567,
               coverage.DNR_FLAG,                                                                                                        --231 MCE only
               CASE
                   WHEN (coverage.STS_CODE IN ('EXPIRED', 'TERMINATED', 'OVERDUE'))
                       THEN
                       coverage.STS_CODE
                   ELSE
                       CASE
                           WHEN datediff(day, CURRENT_TIMESTAMP, coverage.last_coverage_date) > 90
                               THEN 'Upcoming 90+ days '
                           WHEN datediff(day, CURRENT_TIMESTAMP, coverage.last_coverage_date) BETWEEN 61 AND 90
                               THEN 'Upcoming 90 days'
                           WHEN datediff(day, CURRENT_TIMESTAMP, coverage.last_coverage_date) BETWEEN 31 AND 60
                               THEN 'Upcoming 60 days'
                           WHEN datediff(day, CURRENT_TIMESTAMP, coverage.last_coverage_date) BETWEEN 0 AND 30
                               THEN 'Upcoming 30 days'
                           ELSE coverage.STS_CODE END
                   END                                                                            as contract_expired_category,          --205 mce only
               CASE
                   WHEN ib.instance_id IS NULL THEN NULL
                   WHEN datediff(day, CURRENT_TIMESTAMP, coverage.last_coverage_date) <= 30 THEN '30 Days '
                   WHEN datediff(day, CURRENT_TIMESTAMP, coverage.last_coverage_date) BETWEEN 31 AND 60 THEN '60 Days'
                   WHEN datediff(day, CURRENT_TIMESTAMP, coverage.last_coverage_date) BETWEEN 61 AND 90 THEN '90 Days'
                   WHEN datediff(day, CURRENT_TIMESTAMP, coverage.last_coverage_date) BETWEEN 91 AND 180 THEN '180 Days'
                   WHEN datediff(day, CURRENT_TIMESTAMP, coverage.last_coverage_date) BETWEEN 181 AND 365 THEN '1 Year'
                   WHEN datediff(day, CURRENT_TIMESTAMP, coverage.last_coverage_date) BETWEEN 366 AND 730 THEN '2 Year'
                   WHEN datediff(day, CURRENT_TIMESTAMP, coverage.last_coverage_date) BETWEEN 731 AND 1095 THEN '3 Year'
                   ELSE 'More Than 3 Years' END                                                   as renewal_category,                   --329
               ----------------------------------------------------------------------------------
               ib.delist_flag,                                                                                                           --48
               --a.offer_ato_suite_description as offer_ato_suite_description_acat,-- 105
               item.DESCRIPTION                                                                   as offer_ato_suite_description,        -- 105
               -- a.offer_ato_suite_name as offer_ato_suite_name_acat, --106
               coverage.OFFER_ATO_SUITE_NAME,                                                                                            --106
               -- CPS_DSCI_ARCHIVE.FIX_DATES(a.ship_date) as ship_date,
               CPS_DSCI_ARCHIVE.FIX_DATES(ib.ship_date)                                           as ship_date,                          --132, 348
               ib_prnt.instance_number                                                            as parent_instance,                    --109
               NVL(ib_prnt.serial_number, ib_prnt.dup_serial_number)                              as parent_serial_number,               --407
               ib_prnt.inventory_item_id                                                          as parent_device_id,                   -- 405
               --??????????
               ib_prnt.item_name                                                                  as parent_pid,                         -- 404
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
                   END                                                                            as install_site_synch_in_config_flag,  -- 503 , 433

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
                           WHEN (isite.site_use_status = 'I'
                               OR isite.cust_acct_site_status = 'I'
                               OR isite.account_status = 'I')
                               THEN
                               'INACTIVE'
                           WHEN (isite.site_use_si_flag = 'Y'
                               OR isite.cust_acct_site_si_flag = 'Y'
                               OR isite.account_si_flag = 'Y')
                               THEN
                               'ON-HOLD'
                           ELSE
                               'VALID'
                           END
                   ELSE
                       NULL
                   END                                                                            as sid_status,                         --277, 591


               --    CPS_DSCI_ARCHIVE.FIX_DATES(a.last_update_date) as last_update_date, --90
               CPS_DSCI_ARCHIVE.FIX_DATES(ib.INSTANCE_LAST_UPDATE_DATE)                           as INSTANCE_LAST_UPDATE_DATE,          --664, 665
               -- this is ship
               dsd.FISCAL_WEEK_SORTED_NAME                                                        as ship_date_fiscal_week,
               dsd.FISCAL_QTR_SORTED_NAME                                                         as ship_date_fiscal_qtr,
               dsd.FISCAL_MTH_SORTED_NAME                                                         as ship_date_fiscal_mon,
               dsd.FISCAL_YEAR_NUMBER                                                             as ship_date_fiscal_yr,
               dsd.CAL_WEEK_SORTED_NAME                                                           as ship_date_cal_week,
               dsd.CAL_QTR_SORTED_NAME                                                            as ship_date_cal_qtr,

               dldos.FISCAL_WEEK_SORTED_NAME                                                      as ldos_date_fiscal_week,
               dldos.FISCAL_QTR_SORTED_NAME                                                       as ldos_date_fiscal_qtr,
               dldos.FISCAL_MTH_SORTED_NAME                                                       as ldos_date_fiscal_mon,
               dldos.FISCAL_YEAR_NUMBER                                                           as ldos_date_fiscal_yr,
               dldos.CAL_WEEK_SORTED_NAME                                                         as ldos_date_cal_week,
               dldos.CAL_QTR_SORTED_NAME                                                          as ldos_date_cal_qtr,

               dcvd.FISCAL_WEEK_SORTED_NAME                                                       as cdv_to_date_fiscal_week,
               dcvd.FISCAL_QTR_SORTED_NAME                                                        as cdv_to_date_fiscal_qtr,
               dcvd.FISCAL_MTH_SORTED_NAME                                                        as cdv_to_date_fiscal_mon,
               dcvd.FISCAL_YEAR_NUMBER                                                            as cdv_to_date_fiscal_yr,
               dcvd.CAL_WEEK_SORTED_NAME                                                          as cdv_to_date_cal_week,
               dcvd.CAL_QTR_SORTED_NAME                                                           as cdv_to_date_cal_qtr,

               CASE
                   WHEN coverage.sts_code IS NOT NULL THEN coverage.sts_code
                   when coverage.sts_code IS NULL
                       THEN
                       case
                           when IB.covered_status = 'A' then 'ACTIVE'
                           when IB.covered_status = 'I' then 'EXPIRED'
                           when IB.covered_status = 'N' then 'NEVER COVERED'
                           end
                   ELSE 'NEVER COVERED'
                   END                                                                            as product_coverage_status,

               case
                   when IB.covered_status = 'A' then 'ACTIVE'
                   when IB.covered_status = 'I' then 'EXPIRED'
                   when IB.covered_status = 'N' then 'NEVER COVERED'
                   end                                                                            as covered_status,                     -- 215, 262
               CASE
                   WHEN datediff(day, ib.ship_date, CURRENT_TIMESTAMP) BETWEEN 0 AND 365
                       THEN 'Shipped within 1 year'
                   WHEN datediff(day, ib.ship_date, CURRENT_TIMESTAMP) BETWEEN 366 AND 730
                       THEN 'Shipped within 2 year'
                   WHEN datediff(day, ib.ship_date, CURRENT_TIMESTAMP) BETWEEN 731 AND 1095
                       THEN 'Shipped within 3 year'
                   WHEN datediff(day, ib.ship_date, CURRENT_TIMESTAMP) BETWEEN 1096 AND 1460
                       THEN 'Shipped within 4 year'
                   WHEN datediff(day, ib.ship_date, CURRENT_TIMESTAMP) BETWEEN 1461 AND 1825
                       THEN 'Shipped within 5 year'
                   WHEN datediff(day, ib.ship_date, CURRENT_TIMESTAMP) >= 1826 OR ib.ship_date IS NULL
                       THEN 'Shipped more than 5 year back'
                   END                                                                            as ship_to_category,                   --351, 613
               CPS_DSCI_ARCHIVE.FIX_DATES(coverage.contract_start_date)                           as contract_start_date,                --408
               CPS_DSCI_ARCHIVE.FIX_DATES(coverage.contract_end_date)                             as contract_end_date,                  --204
               CASE
                   WHEN datediff(day, CURRENT_TIMESTAMP, item.last_date_of_support) >= 731 OR
                        item.last_date_of_support IS NULL THEN 'LDoS Not in 2 years'
                   WHEN datediff(day, CURRENT_TIMESTAMP, item.last_date_of_support) BETWEEN 541 AND 730
                       THEN 'Within 730 Days (24 Months)'
                   WHEN datediff(day, CURRENT_TIMESTAMP, item.last_date_of_support) BETWEEN 366 AND 540
                       THEN 'Within 540 Days (18 Months)'
                   WHEN datediff(day, CURRENT_TIMESTAMP, item.last_date_of_support) BETWEEN 271 AND 365
                       THEN 'Within 365 Days (12 Months)'
                   WHEN datediff(day, CURRENT_TIMESTAMP, item.last_date_of_support) BETWEEN 181 AND 270
                       THEN 'Within 270 Days (9 Months)'
                   WHEN datediff(day, CURRENT_TIMESTAMP, item.last_date_of_support) BETWEEN 91 AND 180
                       THEN 'Within 180 Days (6 Months)'
                   WHEN datediff(day, CURRENT_TIMESTAMP, item.last_date_of_support) BETWEEN 61 AND 90
                       THEN 'Within 90 Days (3 Months)'
                   WHEN datediff(day, CURRENT_TIMESTAMP, item.last_date_of_support) BETWEEN 31 AND 60
                       THEN 'Within 60 Days (2 Months)'
                   WHEN datediff(day, CURRENT_TIMESTAMP, item.last_date_of_support) BETWEEN 0 AND 30
                       THEN 'Within 30 Days (1 Month)'
                   else 'Past LDoS'
                   END                                                                            as LDOS_Details_in_Months,

               CASE
                   WHEN item.last_date_of_support IS NULL THEN 'LDoS Not Announced'
                   WHEN (item.last_date_of_support) < CURRENT_DATE THEN 'LDOS'
                   WHEN (item.last_date_of_support) BETWEEN CURRENT_DATE AND ADD_MONTHS(CURRENT_DATE, 12)
                       THEN 'LDoS < 12 Mos'
                   WHEN (item.last_date_of_support) BETWEEN ADD_MONTHS(CURRENT_DATE, 12) AND ADD_MONTHS(CURRENT_DATE, 24)
                       THEN '12 Mos < LDoS < 24 Mos'
                   ELSE 'LDoS > 24 Mos'
                   END                                                                               ldos_details_months,
               coverage.meu_allowed_contract_flag                                                          as meu_allowed_contract_flag,
               CASE
                   WHEN ib.covered_status = 'A'
                       THEN CASE
                                WHEN NVL(coverage.meu_allowed_contract_flag, 'N') = 'N' AND
                                     coverage.CONTRACT_INSTALL_GU_COUNT > 1
                                    THEN 'Y'
                                ELSE 'N' END
                   ELSE
                       NULL
                   END                                                                            as meu_polluted_contract_flag,

               CASE
                   WHEN ib.covered_status = 'A' AND coverage.CLE_ID_RENEWED_TO IS NULL
                       THEN 'NO'
                   WHEN ib.covered_status = 'A' AND coverage.CLE_ID_RENEWED_TO IS NOT NULL
                       THEN 'YES'
                   ELSE
                       NULL
                   END                                                                            as cpl_renewed,                        -- -- 641, 222

               CASE
                   WHEN coverage.STS_CODE IN
                        ('OVERDUE', 'ACTIVE', 'SIGNED')
                       AND NVL(item.last_date_of_support,
                               (CURRENT_DATE + 1)) > CURRENT_DATE
                       AND coverage.cvd_attribute14 IS NULL
                       AND NVL(item.last_date_of_support,
                               (TO_DATE(coverage.LAST_COVERAGE_DATE) + 1)) > coverage.LAST_COVERAGE_DATE
                       AND coverage.cle_id_renewed IS NULL
                       THEN
                       'Renewable'
                   WHEN coverage.STS_CODE IN ('ACTIVE', 'SIGNED')
                       AND coverage.cle_id_renewed IS NOT NULL
                       THEN
                       'Already Renewed'
                   WHEN coverage.STS_CODE = 'EXPIRED'
                       AND NVL(item.last_date_of_support,
                               (CURRENT_DATE + 1)) > CURRENT_DATE
                       AND NVL(item.last_date_of_support,
                               (CURRENT_DATE + 1)) > CURRENT_DATE
                       AND coverage.cvd_attribute14 IS NULL
                       THEN
                       'Uncovered but Eligible'
                   WHEN NVL(item.last_date_of_support,
                            (CURRENT_DATE + 1)) < CURRENT_DATE
                       AND NVL(item.last_date_of_support,
                               (TO_DATE(coverage.LAST_COVERAGE_DATE) + 1)) < NVL(coverage.LAST_COVERAGE_DATE, CURRENT_DATE)
                       THEN
                       'Not Eligible'
                   WHEN coverage.cvd_attribute14 IS NOT NULL
                       THEN
                       'Not Eligible'
                   ELSE
                       'Not Eligible'
                   END
                                                                                                     cpl_renewable,                      --221

               ib.so_number                                                                       as so_number,                          --323-147
               ib.so_line_id                                                                      as line_id,                            --632, 324
               ib.po_number                                                                       as product_po,                         --597, 321

               CPS_DSCI_ARCHIVE.FIX_DATES(p_item.last_date_of_support)                            as parent_last_date_of_support,
               eol.END_OF_CHANGE_DT,
               eol.END_OF_MANUFACTURING_DT,
               eol.END_OF_NEW_SVC_ATTACHMENT_DT,
               eol.END_OF_SOFTWARE_MAINTENANCE_DT                                                 as end_of_software_maintenance_date,   -- 237,582
               eol.END_OF_ROUTINE_FAIL_ANLYSYS_DT,
               eol.END_OF_SALE_DT                                                                 as end_of_sale_date,                   --235, 580,
               eol.EOL_SOFTWARE_AVAILABLE_DT,
               eol.EOL_SIGNATURE_RELEASE_DT,
               eol.END_OF_SVC_CONTRACT_RNWL_DT,
               eol.END_OF_TAC_ENGG_SUPPORT_DT                                                     as end_of_tac_support_date,            --239,584,
               eol.END_OF_SFTWR_LICENSE_AVAIL_DT                                                  as end_of_sw_license_date,             --581, 236

               CPS_DSCI_ARCHIVE.FIX_DATES(item.last_date_of_service_attach)                       as last_date_of_service_attached,      --285, 593
               CPS_DSCI_ARCHIVE.FIX_DATES(item.last_date_of_renewal)                              as last_date_of_renewal,               -- 592, 284

               item.product_list_price_gpl_us                                                     as global_product_list_price,          --255, 587
               ib.WARRANTY_TYPE,                                                                                                         -- 376, 621
               CPS_DSCI_ARCHIVE.FIX_DATES(ib.warranty_end_date)                                   as warranty_end_date,                  -- 375, 620
               CPS_DSCI_ARCHIVE.FIX_DATES(ib.instance_creation_date)                              as instance_number_creation_date,      -- 78, 279
               org_bill.name                                                                      as bill_to_id_business_entity,         --564, 185
               org_ins.name                                                                       as sid_business_entity,                --590, 266
               nvl(cp.FIXED_PRODUCT_TYPE, nvl(item.ib_product_type, 'Unknown'))                   as device_level_real_product_type,
               isite.SITE_USE_ORG_ID                                                              as site_ou_id,
               coverage.contract_ou_id,
               coverage.contract_ou_name,
               case
                   when
                               coverage.contract_ou_id <> nvl(isite.SITE_USE_ORG_ID, -1)
                           AND coverage.contract_ou_id is not null -- is covered basically
                       then 'Y'
                   else 'N' end                                                                   as ou_conflict,
               case
                   when CPS_DSCI_ARCHIVE.FIX_DATES(item.last_date_of_support::date) <= CURRENT_DATE() then 'Y'
                   else 'N' end                                                                   as device_level_is_ldos_flag,
               case
                   when CPS_DSCI_ARCHIVE.FIX_DATES(p_item.last_date_of_support::date) <= CURRENT_DATE() then 'Y'
                   else 'N' end                                                                   as device_level_is_parent_ldos_flag,
               case
                   when product_coverage_status in ('ACTIVE', 'SIGNED', 'OVERDUE') then 'Covered'
                   when product_coverage_status in ('NEVER COVERED', 'EXPIRED', 'TERMINATED') then 'Uncovered'
                   else 'Not Sure' end                                                            as simple_covered,

               case
                   WHEN datediff(day, CURRENT_TIMESTAMP, nvl(item.last_date_of_support,
                                                             dateadd(years, 6, CURRENT_TIMESTAMP))) BETWEEN 0 AND 365
                       THEN 'b.LDoS <1 year'
                   WHEN datediff(day, CURRENT_TIMESTAMP, nvl(item.last_date_of_support,
                                                             dateadd(years, 6, CURRENT_TIMESTAMP))) BETWEEN 366 AND 730
                       THEN 'c.LDoS <2 years'
                   WHEN datediff(day, CURRENT_TIMESTAMP, nvl(item.last_date_of_support,
                                                             dateadd(years, 6, CURRENT_TIMESTAMP))) BETWEEN 731 AND 1095
                       THEN 'd.LDoS <3 years'
                   WHEN datediff(day, CURRENT_TIMESTAMP, nvl(item.last_date_of_support,
                                                             dateadd(years, 6, CURRENT_TIMESTAMP))) BETWEEN 1096 AND 1460
                       then 'e.LDoS <4 years'
                   WHEN datediff(day, CURRENT_TIMESTAMP, nvl(item.last_date_of_support,
                                                             dateadd(years, 6, CURRENT_TIMESTAMP))) BETWEEN 1461 AND 1825
                       THEN 'f.LDoS <5 years'
                   WHEN datediff(day, CURRENT_TIMESTAMP,
                                 nvl(item.last_date_of_support, dateadd(years, 6, CURRENT_TIMESTAMP))) >= 1826
                       THEN 'g.LDoS more >5 years'
                   WHEN datediff(day, CURRENT_TIMESTAMP,
                                 nvl(item.last_date_of_support, dateadd(years, 6, CURRENT_TIMESTAMP))) < 0
                       THEN 'a.Past LDoS'
                   else 'h.LDoS Not Known' end                                                    as LDOS_ANNUAL_DURATION,

               case
                   WHEN datediff(day, coverage.START_DATE, CURRENT_TIMESTAMP) BETWEEN 0 AND 365
                       THEN 'g.Coverage Started <1 year'
                   WHEN datediff(day, coverage.START_DATE, CURRENT_TIMESTAMP) BETWEEN 366 AND 730
                       THEN 'f.Coverage Started <2 years'
                   WHEN datediff(day, coverage.START_DATE, CURRENT_TIMESTAMP) BETWEEN 731 AND 1095
                       THEN 'e.Coverage Started <3 years'
                   WHEN datediff(day, coverage.START_DATE, CURRENT_TIMESTAMP) BETWEEN 1096 AND 1460
                       then 'd.Coverage Started <4 years'
                   WHEN datediff(day, coverage.START_DATE, CURRENT_TIMESTAMP) BETWEEN 1461 AND 1825
                       THEN 'c.Coverage Started <5 years'
                   WHEN datediff(day, coverage.START_DATE, CURRENT_TIMESTAMP) >= 1826
                       THEN 'b.Coverage Started >5 years'
                   WHEN datediff(day, coverage.START_DATE, CURRENT_TIMESTAMP) < 0 THEN 'h.Future Coverage'
                   else 'a.Never Covered' end                                                     as COVERAGE_START_ANNUAL_DURATION,

               case
                   WHEN datediff(day, CURRENT_TIMESTAMP, coverage.LAST_COVERAGE_DATE) BETWEEN 0 AND 365
                       THEN 'h.Coverage Ends <1 year'
                   WHEN datediff(day, CURRENT_TIMESTAMP, coverage.LAST_COVERAGE_DATE) BETWEEN 366 AND 730
                       THEN 'i.Coverage Ends <2 years'
                   WHEN datediff(day, CURRENT_TIMESTAMP, coverage.LAST_COVERAGE_DATE) BETWEEN 731 AND 1095
                       THEN 'j.Coverage Ends <3 years'
                   WHEN datediff(day, CURRENT_TIMESTAMP, coverage.LAST_COVERAGE_DATE) BETWEEN 1096 AND 1460
                       then 'k.Coverage Ends <4 years'
                   WHEN datediff(day, CURRENT_TIMESTAMP, coverage.LAST_COVERAGE_DATE) BETWEEN 1461 AND 1825
                       THEN 'l.Coverage Ends <5 years'
                   WHEN datediff(day, CURRENT_TIMESTAMP, coverage.LAST_COVERAGE_DATE) BETWEEN -1 AND -365
                       THEN 'm.Coverage Ended <1 year ago'
                   WHEN datediff(day, CURRENT_TIMESTAMP, coverage.LAST_COVERAGE_DATE) BETWEEN -366 AND -730
                       THEN 'e.Coverage Ended <2 years ago'
                   WHEN datediff(day, CURRENT_TIMESTAMP, coverage.LAST_COVERAGE_DATE) BETWEEN -731 AND -1095
                       THEN 'd.Coverage Ended <3 years ago'
                   WHEN datediff(day, CURRENT_TIMESTAMP, coverage.LAST_COVERAGE_DATE) BETWEEN -1096 AND -1460
                       then 'c.Coverage Ended <4 years ago'
                   WHEN datediff(day, CURRENT_TIMESTAMP, coverage.LAST_COVERAGE_DATE) BETWEEN -1461 AND -1825
                       THEN 'b.Coverage Ended <5 years ago'
                   WHEN datediff(day, CURRENT_TIMESTAMP, coverage.LAST_COVERAGE_DATE) >= -1826
                       THEN 'a.Coverage Ended >5 years ago'
                   else 'g.Never Covered' end                                                     as COVERAGE_END_ANNUAL_DURATION,
               coverage.DUPLICATE_COVERAGE_FLAG                                                   as cvd_DUPLICATE_COVERAGE_FLAG,
               DUPLICATE_CVG_REF_LINE_ID,
               --scope.sources,
               --scope.evidence,
               ib.sava                                                                            as smart_account_virtual_account,       -- 749,
               nvl(coverage.ORDERV_CURRENT,1) as ORDERV_CURRENT,
                   coverage.flag_we_use,
               coverage.last_coverage_date,
               coverage.is_mss_available , 
               coverage.existing_mss_coverage , 
               coverage.mss_available_to_date, 
               coverage.mss_service_available ,
               coverage.am_service_contract_type,
               coverage.am_offer_type ,
               coverage.am_contract_allowed_srv_lvl ,
               coverage.last_coverage_fiscal_quarter


        FROM scope
             join CPS_DSCI_BR.CAM_DS_INSTANCE_DETAIL ib   on
                (
                ib.INSTANCE_ID = scope.INSTANCE_ID
                AND
                NVL(IB.EDWSF_SOURCE_DELETED_FLAG , 'N') = 'N'
                )
             join EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_SITE_GU_DENORM isite on
                (
                    ib.install_at_site_use_id = isite.site_use_id
                and
                    nvl(isite.EDWSF_SOURCE_DELETED_FLAG, 'N') = 'N'
                and isite.site_use_code = 'SHIP_TO'
                )
            left join CPS_DSCI_ARCHIVE.CORRECTED_PIDS cp on (ib.ITEM_NAME = cp.ITEM_NAME)
            left join  {coverage_table_fqn} coverage on (scope.INSTANCE_ID = coverage.INSTANCE_ID)
            left join EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_SAIB_ITEMS item on
            (
                        item.INVENTORY_ITEM_ID = ib.inventory_item_id
                    and
                        nvl(item.EDWSF_SOURCE_DELETED_FLAG, 'N') = 'N'
                )
            --ship_to_site_use_id -> ship tp  and  site.site_use_code = 'SHIP_TO'
                 left join EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_SITE_GU_DENORM st_site on
            (
                        ib.ship_to_site_use_id = st_site.site_use_id
                    and
                        nvl(st_site.EDWSF_SOURCE_DELETED_FLAG, 'N') = 'N'
                    and st_site.site_use_code = 'SHIP_TO'
                )
            --bill_to_site_use_id -> bill to  and          site.site_use_code = 'BILL_TO'
                 left join EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_SITE_GU_DENORM bt_site on
            (
                        ib.bill_to_site_use_id = bt_site.site_use_id
                    and
                        nvl(bt_site.EDWSF_SOURCE_DELETED_FLAG, 'N') = 'N'
                    and bt_site.site_use_code = 'BILL_TO'
                )
                 left join CPS_DSCI_BR.CAM_DS_INSTANCE_DETAIL ib_prnt on
            (
                        ib.parent_instance_id = ib_prnt.instance_id
                    and
                        nvl(ib_prnt.EDWSF_SOURCE_DELETED_FLAG, 'N') = 'N'
                )
                 left join CPS_DSCI_ARCHIVE.DIM_DATE_NEW dsd on (
            dsd.DATE = CPS_DSCI_ARCHIVE.FIX_DATES(ib.ship_date)
            )
                 left join CPS_DSCI_ARCHIVE.DIM_DATE_NEW dldos on (
                dldos.DATE =
                CPS_DSCI_ARCHIVE.FIX_DATES_W_DEF(item.last_date_of_support::DATE, '2150-12-31'::DATE)
            )
                 left join CPS_DSCI_ARCHIVE.DIM_DATE_NEW dcvd on (
            dcvd.DATE = CPS_DSCI_ARCHIVE.FIX_DATES(coverage.last_coverage_date::DATE)
            )
                 left join EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_SAIB_ITEMS p_item on
            (
                        p_item.INVENTORY_ITEM_ID = ib_prnt.inventory_item_id
                    and
                        nvl(p_item.EDWSF_SOURCE_DELETED_FLAG, 'N') = 'N')
                 left join resolved_eol eol on (eol.BK_PRODUCT_ID = item.ITEM_NAME and eol.orderv = 1)
                 left join CPS_DSCI_BR.CAM_DS_INSTANCE_DETAIL replace_ib on
            (
                        ib.replaced_instance_id = replace_ib.instance_id
                    and
                        nvl(replace_ib.EDWSF_SOURCE_DELETED_FLAG, 'N') = 'N'
                )
                 left join EDW_SERVICE_ETL_DB.SS.CSF_HR_ALL_ORGANIZATION_UNITS org_bill on
            (
                        org_bill.organization_id = coverage.bill_to_org_id
                    and
                        nvl(org_bill.EDWSF_SOURCE_DELETED_FLAG, 'N') = 'N'
                )
                 left join EDW_SERVICE_ETL_DB.SS.CSF_HR_ALL_ORGANIZATION_UNITS org_ins on
            (
                        org_ins.organization_id = isite.site_use_org_id
                    and
                        nvl(org_ins.EDWSF_SOURCE_DELETED_FLAG, 'N') = 'N'
                )
                        ;
    """
    engine = create_engine(
        sec.get_sf_pw(sec.check_env(env), wh, schema),
        connect_args={"log_max_query_length": 10_000},
    )
    core_sql = text(core_sql)
    with engine.begin() as conn:
        result = conn.execute(core_sql)
        result = result.fetchall()

    return multis_table_name


@task(log_stdout=True)
@log_queries
def gen_notes(
    multi_row_table_name: str,
    notes_table_name: str,
    env: T_ENV,
    wh: str,
    schema: str,
) -> str:
    multi_tbl_fqn = f"{schema}.{multi_row_table_name}".upper()
    notes_tbl_fqn = f"{schema}.{notes_table_name}".upper()
    notes_sql = f"""
    create or replace Transient table {notes_tbl_fqn} as
    with flat as (
          select INSTANCE_ID,
               PARENT_INSTANCE_ID,
                array_agg(DISTINCT service_level) OVER ( PARTITION BY PARENT_INSTANCE_ID) as list_of_service_levels,
                array_agg(DISTINCT coverage_line_id::bigint ) OVER ( PARTITION BY PARENT_INSTANCE_ID) as list_of_covered_lines,
                array_agg(DISTINCT contract_number  ) OVER ( PARTITION BY PARENT_INSTANCE_ID ) as list_of_contracts,
                row_number() over ( partition by  f.INSTANCE_ID order by f.coverage_line_id  desc) as row_num_cli
                from {multi_tbl_fqn} f
    ), multi as (
        select instance_id, max(i.ROW_NUM_CLI) as mx_ord
        from flat i
        group by  instance_id
        having max(i.ROW_NUM_CLI)> 1
    ), dets as (
        select multi.INSTANCE_ID::bigint as INSTANCE_ID
       , flat.LIST_OF_CONTRACTS
       , flat.LIST_OF_COVERED_LINES
       , flat.LIST_OF_SERVICE_LEVELS
        from flat join multi on (flat.INSTANCE_ID=multi.INSTANCE_ID)
        where flat.row_num_cli = 1
        )
    select INSTANCE_ID, OBJECT_CONSTRUCT(*) as notes from dets;
    """
    notes_sql = text(notes_sql)
    engine = create_engine(
        sec.get_sf_pw(sec.check_env(env), wh, schema),
        connect_args={"log_max_query_length": 10_000},
    )
    with engine.begin() as conn:
        result = conn.execute(notes_sql)
        result = result.fetchall()
    return notes_table_name


@task(log_stdout=True)
@log_queries
def flatten_data(
    flat_table_name: str,
    multi_row_tbl: str,
    env: T_ENV,
    wh: str,
    schema: str,
) -> str:
    multi_row_tbl_fqn = f"{schema}.{multi_row_tbl}".upper()
    flat_tbl_fqn = f"{schema}.{flat_table_name}".upper()
    flat_sql = f"""
      create or replace Transient table {flat_tbl_fqn} as
         with flat_table as (
         select INSTANCE_ID,
           PARENT_INSTANCE_ID,
           CASE WHEN INSTANCE_ID = PARENT_INSTANCE_ID THEN 'Y' ELSE NULL END                      AS is_actual_parent,
           coverage_line_id,
           nvl(coverage_line_id, -1)                                                       as this_covered_line_id,
           orderv_current,
           installed_base_status,
           device_level_real_product_type,
           STS_CODE,
           product_relationship,
           mso,
           service_list_price,
           PRICE_NEGOTIATED,        --495  637 alt location vs nasty cte
           product_list_price,
           installed_at_site_id,
           QUANTITY,
           usd_prorated_list_price, --504
           usd_extended_list_price  -- 505
         from {multi_row_tbl_fqn}
        )
        select
            PARENT_INSTANCE_ID,
            count(distinct INSTANCE_ID)                                                             as device_level_total_config_lines,
            sum(g.QUANTITY )                                                                        as device_level_quantity_total,
            sum(g.USD_EXTENDED_LIST_PRICE)                                                          as device_level_extended_list_price,
            sum(g.USD_PRORATED_LIST_PRICE)                                                          as device_level_prorated_list_price,
            sum(g.PRODUCT_LIST_PRICE)                                                               as device_level_product_list_price_total,
            sum(g.service_list_price)                                                               as device_level_service_list_price_raw_total,
            sum(case when g.PRODUCT_RELATIONSHIP in ('Parent', 'Standalone') then 1 else 0 end )    as device_level_total_parents,
            -- business rule from Athul  no more that 1
            least(1,sum( case when g.device_level_real_product_type ='CHASSIS' then 1 else 0 end )) as device_level_total_chassis,
            sum( case when g.installed_base_status ='Latest-INSTALLED' then 1 else 0 end )          as device_level_total_latest_installed,
            sum( case when g.installed_base_status !='Latest-INSTALLED' then 1 else 0 end )         as device_level_not_total_latest_installed,
            sum( case when g.device_level_real_product_type ='SOFTWARE'then 1 else 0 end )          as device_level_total_sw_product_type,
            sum( case when g.device_level_real_product_type !='SOFTWARE'then 1 else 0 end )         as device_level_total_non_sw_product_type,
            count(distinct g.installed_base_status)                                                 as device_level_install_base_status_length,
            count(distinct g.mso)                                                                   as device_level_maintenance_so_number_list_length,
            count(distinct g.INSTALLED_AT_SITE_ID)                                                  as device_level_installed_at_site_id_total
        from flat_table g
        where ORDERV_CURRENT = 1  -- bc we are not filtering to curent = 1 we woudl dbl count
        
       --     and (
       --     INSTALLED_BASE_STATUS not in
        --     ('Replaced-DEINSTALLED', 'Returned-UNMATCHED', 'Latest', 'Decommission',
        --      'Terminated-Scrapped', 'Returned-AUTO_DEINSTALLED', 'Replaced', 'Installed', 'Returned',
        --      'Returned-DEINSTALLED', 'Replace Pend-DEINSTALLED', 'Terminated',
        --      'Replace Pend-AUTO_DEINSTALLED', 'Terminated-AUTO_DEINSTALLED', 'Terminated-Duplicate',
        --      'Replaced-AUTO_DEINSTALLED', 'COD-DESTROYED', 'EXPIRED', 'Terminated-UNMATCHED'
        --     )
        -- or replaced_ib
        -- 
        -- )

        group by PARENT_INSTANCE_ID

    """
    flat_sql = text(flat_sql)
    engine = create_engine(
        sec.get_sf_pw(sec.check_env(env), wh, schema),
        connect_args={"log_max_query_length": 10_000},
    )
    with engine.begin() as conn:
        result = conn.execute(flat_sql)
        result = result.fetchall()
    return flat_table_name


@task(log_stdout=True)
@log_queries
def prep_final(
    core_table_name: str,
    multis_table_name: str,
    notes_tbl_name: str,
    flat_tbl: str,
    env: T_ENV,
    wh: str,
    schema: str,
) -> str:
    multis_table_fqn = f"{schema}.{multis_table_name}".upper()
    notes_table_fqn = f"{schema}.{notes_tbl_name}".upper()
    flat_table_fqn = f"{schema}.{flat_tbl}".upper()
    core_table_fqn = f"{schema}.{core_table_name}".upper()
    sql = f"""
        create or replace transient table {core_table_fqn}   as
        select d.*,
               case when d.instance_id = d.parent_instance_id then 'Y' else 'N' end as device_level_is_actual_parent,
               case when d.instance_id = d.parent_instance_id then 1 else 0 end as actual_parent_count,
               -- list from aggs:
               nvl(device_level_total_config_lines,0) as device_level_total_config_lines,
               nvl(device_level_quantity_total,0) as device_level_quantity_total,
               nvl(device_level_extended_list_price,0) as device_level_extended_list_price,
               nvl(device_level_product_list_price_total,0) as device_level_product_list_price_total,
               nvl(device_level_prorated_list_price,0) as device_level_prorated_list_price,
               nvl(device_level_service_list_price_raw_total,0) as device_level_service_list_price_raw_total,
               nvl(device_level_total_parents,0) as device_level_total_parents,
               nvl(device_level_total_chassis,0) as device_level_total_chassis,
               nvl(device_level_total_latest_installed,0) as device_level_total_latest_installed,
               nvl(device_level_not_total_latest_installed,0) as device_level_not_total_latest_installed,
               nvl(device_level_total_sw_product_type,0) as device_level_total_sw_product_type,
               nvl(device_level_total_non_sw_product_type,0) as device_level_total_non_sw_product_type,
               nvl(device_level_install_base_status_length,0) as device_level_install_base_status_length,
               nvl(device_level_maintenance_so_number_list_length,0) as device_level_maintenance_so_number_list_length,
               nvl(device_level_installed_at_site_id_total,0) as device_level_installed_at_site_id_total,
               case when n.INSTANCE_ID is null then 'single'
                                    else 'multi_line_fix' end as modified_record,
                    TO_VARCHAR(n.notes) as note
                    from {multis_table_fqn} d
                    left join {notes_table_fqn}  n on (n.INSTANCE_ID=d.INSTANCE_ID)
                    left join {flat_table_fqn} f on (d.instance_id = f.parent_instance_id)
                    where nvl(d.ORDERV_CURRENT,1) =1"""

    engine = create_engine(
        sec.get_sf_pw(sec.check_env(env), wh, schema),
        connect_args={"log_max_query_length": 10_000},
    )
    sql = text(sql)
    with engine.begin() as conn:
        result = conn.execute(sql)
        result = result.fetchall()
    return core_table_name


@task(
    log_stdout=True,
)
@log_queries
def fix_missing_parents(
    thought_spot_table: str, flat_table: str, env: T_ENV, wh: str, schema: str
) -> bool:
    dest_tbl = f"{thought_spot_table}_missing_parents".upper()
    dest_tbl_param = bindparam("dest_tbl", quoted_name(dest_tbl, False))
    tst = bindparam("tst", quoted_name(thought_spot_table, False))
    flat_table_param = bindparam("flat_table", quoted_name(flat_table, False))

    dest_tbl_stmt = text(
        """
        create or replace Transient table identifier(:dest_tbl) as
        with sub as (
            select distinct c.parent_instance_id
            from identifier(:tst) c
        ) , missing as
            (
            select sub.PARENT_INSTANCE_ID from sub
            left join identifier(:tst) c on (sub.PARENT_INSTANCE_ID=c.instance_id)
            where c.instance_id is null
            ),kids as
                (
                select min(c.instance_id) as mn_ins, c.PARENT_INSTANCE_ID
                from identifier(:tst) c
                    join missing on (c.PARENT_INSTANCE_ID = missing.PARENT_INSTANCE_ID)
                group by c.PARENT_INSTANCE_ID
                )
            select * from kids;
        """
    ).bindparams(dest_tbl_param, tst)

    fix_aggs_stmt = text(
        """
        update  identifier(:tst) c
        set c.DEVICE_LEVEL_IS_ACTUAL_PARENT = 'E',
            c.ACTUAL_PARENT_COUNT  = 1,
            c.device_level_total_config_lines = nvl(fix.device_level_total_config_lines,0),
            c.device_level_quantity_total = nvl(fix.device_level_quantity_total,0) ,
            c.device_level_extended_list_price = nvl(fix.device_level_extended_list_price,0) ,
            c.device_level_product_list_price_total = nvl(fix.device_level_product_list_price_total,0),
            c.device_level_prorated_list_price = nvl(fix.device_level_prorated_list_price,0) ,
            c.device_level_service_list_price_raw_total = nvl(fix.device_level_service_list_price_raw_total,0) ,
            c.device_level_total_parents = nvl(fix.device_level_total_parents,0) ,
            c.device_level_total_chassis = nvl(fix.device_level_total_chassis,0) ,
            c.device_level_total_latest_installed = nvl(fix.device_level_total_latest_installed,0)  ,
            c.device_level_not_total_latest_installed = nvl(fix.device_level_not_total_latest_installed,0) ,
            c.device_level_total_sw_product_type = nvl(fix.device_level_total_sw_product_type,0) ,
            c.device_level_total_non_sw_product_type = nvl(fix.device_level_total_non_sw_product_type,0),
            c.device_level_install_base_status_length = nvl(fix.device_level_install_base_status_length,0) ,
            c.device_level_maintenance_so_number_list_length = nvl(fix.device_level_maintenance_so_number_list_length,0),
            c.device_level_installed_at_site_id_total = nvl(fix.device_level_installed_at_site_id_total,0)
        FROM (select p.MN_INS , f.*  from identifier(:dest_tbl) p
           join identifier(:flat_table) f on (f.PARENT_INSTANCE_ID=p.PARENT_INSTANCE_ID  )
           ) AS fix
            where fix.MN_INS = c.instance_id ;
        """
    ).bindparams(tst, dest_tbl_param, flat_table_param)

    drop_stmt = text("""DROP TABLE identifier(:dest_tbl);""").bindparams(dest_tbl_param)

    engine = create_engine(
        sec.get_sf_pw(sec.check_env(env), wh, schema),
        connect_args={"log_max_query_length": 10_000},
    )
    with engine.begin() as conn:
        for stmt in (dest_tbl_stmt, fix_aggs_stmt, drop_stmt):
            conn.execute(stmt)
    return True


@task(log_stdout=True)
@log_queries
def canvas_enrichments(ts_table: str, env: T_ENV, wh: str, schema: str) -> bool:
    sql = f"""
    alter table {ts_table}  add column covered_to_ldos varchar(40) default 'NO';
    update {ts_table}  i set i.covered_to_ldos = o.identifier
    from (
            select distinct INSTANCE_ID,
            case
                    when nvl(last_coverage_date,'2000-12-31'::DATE) = CPS_DSCI_ARCHIVE.FIX_DATES_W_DEF( last_date_of_support::DATE,'2150-12-31'::DATE)
                      then 'YES'
                    when nvl(last_coverage_date,'2000-12-31'::DATE) > CPS_DSCI_ARCHIVE.FIX_DATES_W_DEF( last_date_of_support::DATE,'2150-12-31'::DATE)
                            AND
                         nvl(last_coverage_date,'2000-12-31'::DATE) > current_date

                        then 'COVERED_PAST_LDOS'
                    when CPS_DSCI_ARCHIVE.FIX_DATES_W_DEF( last_date_of_support::DATE,'2150-12-31'::DATE) = '2150-12-31'::DATE 
                        then  'LDOS_NOT_ANNOUNCED' 


                    when nvl(last_coverage_date,'2000-12-31'::DATE) < current_date
                          AND
                         CPS_DSCI_ARCHIVE.FIX_DATES_W_DEF( last_date_of_support::DATE,'2150-12-31'::DATE) < current_date
                        then 'IGNORE_PAST_DATA'

                    else 'NO'
                        end  identifier

                    from {ts_table} 
         ) o where o.INSTANCE_ID= i.INSTANCE_ID;



    alter table {ts_table}  add column dl_parent_product_family varchar(5000);
    update {ts_table}  i set i.dl_parent_product_family = o.product_family
    from (
            select distinct parent_INSTANCE_ID,product_family
            from {ts_table}
            where parent_INSTANCE_ID =INSTANCE_ID

         ) o where o.parent_INSTANCE_ID= i.parent_INSTANCE_ID;
        alter table {ts_table} add column virtual_accounts varchar(5000);
        alter table {ts_table} add column smart_accounts varchar(5000);

        update {ts_table}  d set d.virtual_accounts=i.virtual_accounts, d.smart_accounts=i.smart_accounts
        from (
                 with interesting as (
                     select instance_id,
                            smart_account_virtual_account,
                            nvl(try_to_number(trim(SPLIT_PART(o.value, ':', 1))),-1) as smart_account,
                            nvl(try_to_number(trim(SPLIT_PART(o.value, ':', 2))),-1) as virtual_account
                     from {ts_table} ,
                          lateral split_to_table(smart_account_virtual_account, '|') o
                     where smart_account_virtual_account is not null
                 ),
                      almost as (
                          select interesting.*,
                                 concat(va.VIRTUAL_ACCOUNT_NAME, '_(', va.VIRTUAL_ACCOUNT_KEY, ')') as virtual_acct,
                                 concat(sa.SMART_ACCOUNT_NAME, '_(', sa.SMART_ACCOUNT_KEY, ')')     as smart_acct
                          from interesting
                                   join CPS_DSCI_EBV.BV_VIRTUAL_ACCOUNTS_D va
                                        on (va.SMART_ACCOUNT_KEY = interesting.smart_account and
                                            va.VIRTUAL_ACCOUNT_KEY = interesting.virtual_account)
                                   join CPS_DSCI_EBV.BV_SMART_ACCOUNTS_D sa
                                        on (sa.SMART_ACCOUNT_KEY = interesting.smart_account)
                      )
                 select distinct almost.INSTANCE_ID,
                        listagg(almost.virtual_acct, '|') as virtual_accounts,
                        listagg(almost.smart_acct, '|')   as smart_accounts
                 from almost
                 group by almost.INSTANCE_ID
             ) i
        where i.INSTANCE_ID = d.INSTANCE_ID
        ;
        
    alter table {ts_table} add column missing_mso VARCHAR(1) default 'N';
    update {ts_table} c
        set missing_mso = 'Y'
        from (  select INSTANCE_ID
                        from {ts_table}
                        where MSO IS NULL
                        AND STS_CODE IN ('ACTIVE', 'SIGNED')
             ) o 
        where c.INSTANCE_ID=o.INSTANCE_ID
        ;
         """
    engine = create_engine(
        sec.get_sf_pw(sec.check_env(env), wh, schema),
        connect_args={"log_max_query_length": 10_000},
    )
    with engine.begin() as conn:
        for s in sql.split(";"):
            conn.execute(text(s))
    return True


@task(log_stdout=True)
@log_queries
def rename_cols_in_preexsisting_table(
    thought_spot_table: str,
    env: T_ENV,
    wh: str,
    schema: str,
    rename_map: dict,
    display_map: dict,
) -> bool:
    def fix_column(s: str) -> str:
        return (
            s.lower()
            .strip()
            .replace(" ", "_")
            .replace("/", "_")
            .replace("\\", "_")
            .replace("-", "_")
        )

    thought_spot_table_param = bindparam("tst", quoted_name(thought_spot_table, False))
    engine = create_engine(
        sec.get_sf_pw(sec.check_env(env), wh, schema),
        connect_args={"log_max_query_length": 10_000},
    )
    with engine.begin() as conn:
        desc = conn.execute(
            text("desc table identifier(:tst)").bindparams(thought_spot_table_param)
        ).fetchall()
    df = pd.DataFrame(desc)
    df = df.rename(columns=fix_column)

    begin_cols = []
    for c in df.name.to_list():
        begin_cols.append(c.lower())

    post_treatment_1 = []
    for c in begin_cols:
        new = rename_map.get(c, c)
        post_treatment_1.append(new)

    post_treatment_2 = []
    for c in post_treatment_1:
        new = display_map.get(c, c)
        new = (
            new.strip()
            .replace(" ", "_")
            .replace("/", "_")
            .replace("\\", "_")
            .replace("-", "_")
            .replace("(", "")
            .replace(")", "")
            .upper()
        )
        post_treatment_2.append(new)

    this_df = pd.DataFrame({"orig": df.name.to_list(), "new_value": post_treatment_2})

    with engine.begin() as conn:
        for i, row in this_df[this_df.orig != this_df.new_value].iterrows():
            sql = text(
                f"alter table identifier(:tst) rename column {row.orig} to {row.new_value};"
            ).bindparams(thought_spot_table_param)
            conn.execute(sql)
    return True


@task(log_stdout=True)
@log_queries
def proc_sql_rules(
    args: GenArgsOutput,
    thought_spot_table: str,
    schema: str,
    wh: str,
    env: T_ENV,
) -> bool:
    def exec_sql_local_cn_(sql, engine):
        with engine.begin() as conn:
            conn.execute(sql)

    engine_ = create_engine(
        sec.get_sf_pw(sec.check_env(env), wh, schema),
        connect_args={"log_max_query_length": 10_000},
    )
    exec_sql_local_cn = partial(exec_sql_local_cn_, engine=engine_)

    """
    Parameters
    """
    ts_table_param = bindparam("tst", quoted_name(thought_spot_table, False))
    end_of_sfc_date_param = bindparam(
        "end_of_sfc_date", args["end_of_sfc_date"].date(), type_=Date
    )
    atr_goaling_date_param = bindparam(
        "atr_goaling_date", args["atr_goaling_date"].date(), type_=Date
    )
    end_customer_gu_id_param = bindparam(
        "end_customer_gu_id", args["end_customer_gu_id"], expanding=True
    )
    engagement_id_param = bindparam(
        "engagement_id", args["engagement_id"], expanding=True
    )
    contract_bill_to_guid_param = bindparam(
        "contract_bill_to_guid", args["contract_bill_to_guid"], expanding=True
    )
    pricing_model_param = bindparam("pricing_model", args["pricing_model"].upper())
    mx_ = (relativedelta(years=5) + datetime.utcnow()).date()
    mx = bindparam("mx", mx_, type_=Date)

    exec_sql_local_cn(
        text(
            "alter table identifier(:tst) add column cam_contract varchar default 'no_match';"
        ).bindparams(ts_table_param)
    )

    for ty in args["c_type_collection"]:
        inl = [str(x) for x in ty[1]]
        inl_param = bindparam("inl_param", inl, expanding=True)
        ty = bindparam("ty", ty[0])
        exec_sql_local_cn(
            text(
                "update identifier(:tst) r  set cam_contract = :ty where r.CONTRACT_NUMBER in :inl_param"
            ).bindparams(ts_table_param, ty, inl_param)
        )

    # cotermed until End Date ``
    exec_sql_local_cn(
        text(
            "update identifier(:tst) r set r.last_date_of_service_attached = nvl(r.last_date_of_service_attached, :mx)"
        ).bindparams(ts_table_param, mx)
    )
    exec_sql_local_cn(
        text(
            "update identifier(:tst) r set r.LAST_DATE_OF_RENEWAL = nvl(r.LAST_DATE_OF_RENEWAL, :mx)"
        ).bindparams(ts_table_param, mx)
    )
    # exec_sql_local_cn(text(f"update identifier(:tst) r set r.PARENT_LDOS = nvl(r.PARENT_LDOS, $mx);"))
    exec_sql_local_cn(
        text(
            "update identifier(:tst) r set r.PARENT_LDOS = nvl(r.PARENT_LDOS, :mx)"
        ).bindparams(ts_table_param, mx)
    )
    exec_sql_local_cn(
        text("update identifier(:tst) r set r.ldos= nvl(r.ldos, :mx);").bindparams(
            ts_table_param, mx
        )
    )
    exec_sql_local_cn(
        text(
            "alter table identifier(:tst) add column MSS_ASSESSMENT_STATUS varchar DEFAULT 'NOT ASSESSED';"
        ).bindparams(ts_table_param)
    )
    exec_sql_local_cn(
        text(
            """
        update identifier(:tst) r set r.MSS_ASSESSMENT_STATUS = mss.ASSESSMENT_STATUS
        from SERVICES_DB.SERVICES_MSS_BR.MSS_OPPORTUNITIES mss where  r.INSTANCE_ID = mss.INSTANCE_ID
        """
        ).bindparams(ts_table_param)
    )

    exec_sql_local_cn(
        text(
            """ 
        update identifier(:tst) r set r.mss_available_to_date = mss.ASSESSMENT_SUPPORT_END_DATE 
        from SERVICES_DB.SERVICES_MSS_BR.MSS_OPPORTUNITIES mss where  r.INSTANCE_ID = mss.INSTANCE_ID 
        """
        ).bindparams(ts_table_param)
    )
    exec_sql_local_cn(
        # TSS HTEC
        text(
            "alter table identifier(:tst) add column is_tss_htec varchar default 'N';"
        ).bindparams(ts_table_param)
    )

    exec_sql_local_cn(
        text(
            # TSS HTEC
            """update identifier(:tst) r set r.is_tss_htec = 'Y'
        where r.PRODUCT_FAMILY = 'TFFTS'
        """
        ).bindparams(ts_table_param)
    )

    exec_sql_local_cn(
        # BCS SUBSCRIPTION
        text(
            "alter table identifier(:tst) add column is_bcs_sub varchar default 'N';"
        ).bindparams(ts_table_param)
    )

    exec_sql_local_cn(
        # BCS SUBSCRIPTION
        text(
            """update identifier(:tst) r set r.is_bcs_sub = 'Y' where r.PRODUCT_FAMILY = 'BCS';"""
        ).bindparams(ts_table_param)
    )
    # exec_sql_local_cn(f"alter table identifier(:tst) add column is_serial_num_found varchar default 'N';") #SERIAL NUMBER NOT FOUND
    exec_sql_local_cn(
        # DISQUALIFIED ATR BY INSTALL BASE STATUS
        text(
            "alter table identifier(:tst) add column is_disqual_install_base_status varchar default 'N';"
        ).bindparams(ts_table_param)
    )

    exec_sql_local_cn(
        # DISQUALIFIED ATR BY INSTALL BASE STATUS
        text(
            """update identifier(:tst) r set r.is_disqual_install_base_status = 'Y' 
        where r.installed_base_status not in ('Latest-INSTALLED',
        'Compliance Flagged-INSTALLED',
        'Terminated',
        'Installed',
        'EXPIRED',
        'CREATED',
        'Latest',
        'COVERAGE RESTRICTED') or
        r.installed_base_status
        is NULL;"""
        ).bindparams(ts_table_param)
    )

    exec_sql_local_cn(
        # DNR (DO NOT RENEW FLAG)
        text(
            "alter table identifier(:tst) add column is_dnr_flag varchar default 'N';"
        ).bindparams(ts_table_param)
    )
    exec_sql_local_cn(
        # DNR (DO NOT RENEW FLAG)
        text(
            "update identifier(:tst) r set r.is_dnr_flag  ='Y' where DNR_FLAG is not null"
        ).bindparams(ts_table_param)
    )

    exec_sql_local_cn(
        # PAST LDOS (MSS DECLINED)
        text(
            "alter table identifier(:tst) add column is_past_ldos_mss_decided varchar default 'N';"
        ).bindparams(ts_table_param)
    )

    exec_sql_local_cn(
        # PAST LDOS (MSS DECLINED) changed 7-7-23
        text(
            """update identifier(:tst) r set r.is_past_ldos_mss_decided = 'Y' 
            where r.LDOS < current_date() and
             (r.mss_assessment_status in ('DENIED', 'DECLINED') or
             r.mss_available_to_date < current_date() or
             r.LDOS <= DATEADD(year, -2, current_date()) or
             r.last_coverage_end_date < r.LDOS);"""
        ).bindparams(ts_table_param)
    )

    exec_sql_local_cn(
        # PAST LDOS (MSS POTENTIAL)
        text(
            "alter table identifier(:tst) add column is_past_ldos_mss_potential varchar default 'N';"
        ).bindparams(ts_table_param)
    )
    exec_sql_local_cn(
        # PAST LDOS (MSS POTENTIAL)
        text(
            """update identifier(:tst) r set r.is_past_ldos_mss_potential = 'Y' 
        where r.LDOS < current_date() and
         r.MSS_ASSESSMENT_STATUS not in ('DENIED', 'DECLINED'); 
        """
        ).bindparams(ts_table_param)
    )

    exec_sql_local_cn(
        # COTERMED TO LDOS (MSS DECLINED)
        text(
            "alter table identifier(:tst) add column is_coterm_to_ldos_mss_declined varchar default 'N';"
        ).bindparams(ts_table_param)
    )
    exec_sql_local_cn(
        # COTERMED TO LDOS (MSS DECLINED) Updated 6-15
        text(
            """update identifier(:tst) r set r.is_coterm_to_ldos_mss_declined = 'Y' 
            where r.LDOS > current_date() and
            r.last_coverage_end_date >= r.LDOS and
            r.coverage_status in ('ACTIVE', 'SIGNED') and
             (r.mss_assessment_status in ('DENIED', 'DECLINED') or
             r.mss_available_to_date < current_date());"""
        ).bindparams(ts_table_param)
    )

    exec_sql_local_cn(
        # COTERMED TO LDOS (MSS POTENTIAL)
        text(
            "alter table identifier(:tst) add column is_coterm_to_ldos_mss_potential varchar default 'N';"
        ).bindparams(ts_table_param)
    )
    exec_sql_local_cn(
        # COTERMED TO LDOS (MSS POTENTIAL) Updated 6-15
        text(
            """update identifier(:tst) r set r.is_coterm_to_ldos_mss_potential = 'Y' 
            where r.ldos > current_date() and
            r.last_coverage_end_date >= r.ldos and
            r.coverage_status in ('ACTIVE', 'SIGNED') and
             r.mss_assessment_status not in ('DENIED', 'DECLINED'); 
        """
        ).bindparams(ts_table_param)
    )

    exec_sql_local_cn(
        # PARENT PAST LDOS (MSS POTENTIAL)
        text(
            "alter table identifier(:tst) add column is_parent_passed_ldos_mss_potential varchar default 'N';"
        ).bindparams(ts_table_param)
    )
    exec_sql_local_cn(
        # PARENT PAST LDOS (MSS POTENTIAL)
        text(
            """update identifier(:tst) r set r.is_parent_passed_ldos_mss_potential = 'Y' 
         where r.parent_ldos < current_date() and 
         r.mss_assessment_status not in('DENIED', 'DECLINED');"""
        ).bindparams(ts_table_param)
    )

    exec_sql_local_cn(
        # PARENT PAST LDOS (MSS DECLINED)
        text(
            "alter table identifier(:tst) add column is_parent_passed_ldos_mss_declined varchar default 'N';"
        ).bindparams(ts_table_param)
    )

    exec_sql_local_cn(
        # PARENT PAST LDOS (MSS DECLINED) changed 7-7-23
        text(
            """update identifier(:tst) r set r.is_parent_passed_ldos_mss_declined = 'Y'  
        where r.parent_ldos < current_date() and 
        (r.mss_assessment_status in ('DENIED', 'DECLINED') or
                 r.mss_available_to_date < current_date()or
             r.parent_ldos <= DATEADD(year, -2, current_date()) or
             r.last_coverage_end_date < r.parent_ldos);"""
        ).bindparams(ts_table_param)
    )

    exec_sql_local_cn(
        # PARENT COTERMED TO LDOS (MSS POTENTIAL)
        text(
            "alter table identifier(:tst) add column is_parent_coterm_ldos_mss_potential varchar default 'N';"
        ).bindparams(ts_table_param)
    )
    exec_sql_local_cn(
        # PARENT COTERMED TO LDOS (MSS POTENTIAL) Updated 6-15
        text(
            """update identifier(:tst) r set r.is_parent_coterm_ldos_mss_potential = 'Y'  
        where r.parent_ldos < current_date() and 
        r.last_coverage_end_date >= r.parent_ldos and 
        r.coverage_status in('ACTIVE', 'SIGNED') and
        r.mss_assessment_status not in('DENIED', 'DECLINED');"""
        ).bindparams(ts_table_param)
    )

    exec_sql_local_cn(
        # PARENT COTERMED TO LDOS (MSS DECLINED)
        text(
            "alter table identifier(:tst) add column is_parent_coterm_ldos_mss_declined varchar default 'N';"
        ).bindparams(ts_table_param)
    )

    exec_sql_local_cn(
        # PARENT COTERMED TO LDOS (MSS DECLINED) Updated 6-15
        text(
            """update identifier(:tst) r set r.is_parent_coterm_ldos_mss_declined = 'Y'  
        where r.parent_ldos < current_date() and 
        r.last_coverage_end_date >= r.parent_ldos and 
        r.coverage_status in ('ACTIVE', 'SIGNED') and
        (r.mss_assessment_status in ('DENIED', 'DECLINED') or
                 r.mss_available_to_date < current_date());"""
        ).bindparams(ts_table_param)
    )

    exec_sql_local_cn(
        # NOT LATEST INSTALLED (=COMPLIANCE FLAG/COVERAGE RESTRICTED)
        text(
            "alter table identifier(:tst) add column is_not_latest_installed_compliance varchar default 'N';"
        ).bindparams(ts_table_param)
    )
    exec_sql_local_cn(
        # NOT LATEST INSTALLED (=COMPLIANCE FLAG/COVERAGE RESTRICTED)
        text(
            """update identifier(:tst) r set r.is_not_latest_installed_compliance = 'Y' 
    where r.installed_base_status in ('Compliance Flagged-INSTALLED', 'COVERAGE RESTRICTED');"""
        ).bindparams(ts_table_param)
    )

    exec_sql_local_cn(
        # WRONG CUSTOMER
        text(
            "alter table identifier(:tst) add column is_correct_customer varchar default 'N';"
        ).bindparams(ts_table_param)
    )
    exec_sql_local_cn(
        # WRONG CUSTOMER
        text(
            "update identifier(:tst) r set r.is_correct_customer = 'Y' where r.INSTALLED_AT_GUID IN :end_customer_gu_id"
        ).bindparams(ts_table_param, end_customer_gu_id_param)
    )

    exec_sql_local_cn(
        # SFC EXCLUSION
        text(
            "alter table identifier(:tst) add column is_sfc_exclusion varchar default 'N';"
        ).bindparams(ts_table_param)
    )
    exec_sql_local_cn(
        # SFC EXCLUSION
        text(
            """update identifier(:tst) r set r.is_sfc_exclusion = 'Y'
     where r.INSTANCE_ID in (
        select d.INSTANCE_ID from SERVICES_DB.SERVICES_ENT_FBV.BV_MCE_AM_ENGAGEMENT_DATA d join SERVICES_DB.SERVICES_ENT_FBV.BV_MCE_AM_ENGAGEMENT_HDR h
        on (h.ENGAGEMENT_ID=d.ENGAGEMENT_ID)
        where d.EXCLUSION_FLAG = 'Y' and  h.ENGAGEMENT_NUMBER in :engagement_id );
        """
        ).bindparams(ts_table_param, engagement_id_param)
    )

    exec_sql_local_cn(
        # CO TERM
        text(
            "alter table identifier(:tst) add column is_coterm varchar default 'N';"
        ).bindparams(ts_table_param)
    )
    exec_sql_local_cn(
        # CO TERM Updated 6-15
        text(
            """
            update identifier(:tst) r  set r.is_coterm = 'Y' where 
            r.last_coverage_end_date >= :end_of_sfc_date and r.coverage_status in('ACTIVE', 'SIGNED')
            """
        ).bindparams(ts_table_param, end_of_sfc_date_param)
    )

    exec_sql_local_cn(
        # CO TERM TO LDOS
        text(
            "alter table identifier(:tst) add column is_coterm_to_ldos varchar default 'N'"
        ).bindparams(ts_table_param)
    )
    exec_sql_local_cn(
        # CO TERM TO LDOS Updated 6-15
        text(
            "update identifier(:tst) r set r.is_coterm_to_ldos = 'Y' where r.last_coverage_end_date >=r.ldos and r.coverage_status in ('ACTIVE', 'SIGNED');"
        ).bindparams(ts_table_param)
    )

    exec_sql_local_cn(
        # SFC SCOPE
        text(
            "alter table identifier(:tst) add column is_sfc_scope varchar default 'N';"
        ).bindparams(ts_table_param)
    )
    exec_sql_local_cn(
        # SFC SCOPE
        text(
            """update identifier(:tst) r set r.is_sfc_scope = 'Y'
         where r.is_coterm='N' and
         r.is_coterm_to_ldos='N' and
         r.SFC_FLAG = 'Y' and
         r.CONTRACT_BILL_TO_GUID in :contract_bill_to_guid
         """
        ).bindparams(ts_table_param, contract_bill_to_guid_param)
    )

    exec_sql_local_cn(
        # SFC IN SCOPE - OTHER PARTNER
        text(
            "alter table identifier(:tst) add column is_sfc_scope_other_partner varchar default 'N';"
        ).bindparams(ts_table_param)
    )
    exec_sql_local_cn(
        # SFC IN SCOPE - OTHER PARTNER
        text(
            """update identifier(:tst) r set r.is_sfc_scope_other_partner = 'Y'
        where r.is_coterm = 'N' and
        r.is_coterm_to_ldos = 'N' and
        r.SFC_FLAG = 'Y' and
        r.CONTRACT_BILL_TO_GUID not in :contract_bill_to_guid"""
        ).bindparams(ts_table_param, contract_bill_to_guid_param)
    )

    exec_sql_local_cn(
        text(
            "alter table identifier(:tst) add column is_not_sfc_is_cotermed_next_renewal_through_atr_goaling_period varchar default 'N';"
        ).bindparams(ts_table_param)
    )
    exec_sql_local_cn(
        text(
            """update identifier(:tst) r set r.is_not_sfc_is_cotermed_next_renewal_through_atr_goaling_period = 'Y' 
        where (r.is_coterm = 'Y' or r.is_coterm_to_ldos = 'Y') and 
        nvl(r.SFC_FLAG, 'N')  = 'N' and 
         r.last_coverage_end_date >= :atr_goaling_date;"""
        ).bindparams(ts_table_param, atr_goaling_date_param)
    )

    exec_sql_local_cn(
        text(
            "alter table identifier(:tst) add column is_not_sfc_is_cotermed_next_renewal_before_atr_goaling_period varchar default 'N';"
        ).bindparams(ts_table_param)
    )
    exec_sql_local_cn(
        text(
            """
        update identifier(:tst) r set r.is_not_sfc_is_cotermed_next_renewal_before_atr_goaling_period = 'Y' 
        where (r.is_coterm = 'Y' or r.is_coterm_to_ldos = 'Y') and 
        nvl(r.SFC_FLAG, 'N')  = 'N' and
         r.last_coverage_end_date < :atr_goaling_date;"""
        ).bindparams(ts_table_param, atr_goaling_date_param)
    )
    exec_sql_local_cn(
        # NON-SFC NOT COTERMED TO NEXT RENEWAL
        text(
            "alter table identifier(:tst) add column is_not_sfc_not_cotermed_next_renewal varchar default 'N';"
        ).bindparams(ts_table_param)
    )
    exec_sql_local_cn(
        # NON-SFC NOT COTERMED TO NEXT RENEWAL
        text(
            """update identifier(:tst) r set r.is_not_sfc_not_cotermed_next_renewal = 'Y'
            where r.is_coterm = 'N' and
            r.is_coterm_to_ldos = 'N' and
            nvl(r.SFC_FLAG, 'N')  = 'N';
            """
        ).bindparams(ts_table_param)
    )

    exec_sql_local_cn(
        # NON-SFC COTERMED TO NEXT RENEWAL
        text(
            "alter table identifier(:tst) add column is_not_sfc_is_cotermed_next_renewal varchar default 'N';"
        ).bindparams(ts_table_param)
    )
    exec_sql_local_cn(
        # NON-SFC COTERMED TO NEXT RENEWAL
        text(
            """update identifier(:tst) r set r.is_not_sfc_is_cotermed_next_renewal = 'Y'
                where (r.is_coterm = 'Y' or r.is_coterm_to_ldos = 'Y') and
                nvl(r.SFC_FLAG, 'N')  = 'N' and
                r.CONTRACT_BILL_TO_GUID in :contract_bill_to_guid;"""
        ).bindparams(ts_table_param, contract_bill_to_guid_param)
    )

    exec_sql_local_cn(
        # SFC COTERMED WITH MSO
        text(
            "alter table identifier(:tst) add column is_sfc_cotermed_with_mso varchar default 'N';"
        ).bindparams(ts_table_param)
    )
    exec_sql_local_cn(
        # SFC COTERMED WITH MSO
        text(
            """update identifier(:tst) r set r.is_sfc_cotermed_with_mso = 'Y' where
            (r.is_coterm = 'Y' or r.is_coterm_to_ldos = 'Y') and
            r.SFC_FLAG = 'Y' and r.missing_mso='N';"""
        ).bindparams(ts_table_param)
    )

    exec_sql_local_cn(
        # SFC COTERMED NO MSO
        text(
            "alter table identifier(:tst) add column is_sfc_cotermed_no_mso varchar default 'N';"
        ).bindparams(ts_table_param)
    )

    exec_sql_local_cn(
        # SFC COTERMED NO MSO
        text(
            """update identifier(:tst) r set r.is_sfc_cotermed_no_mso = 'Y' where
            (r.is_coterm = 'Y' or r.is_coterm_to_ldos = 'Y') and
            r.SFC_FLAG = 'Y' and r.missing_mso = 'Y';"""
        ).bindparams(ts_table_param)
    )

    exec_sql_local_cn(
        # atr action bucket
        text(
            "alter table identifier(:tst) add column atr_action_bucket varchar default 'UNK';"
        ).bindparams(ts_table_param)
    )

    exec_sql_local_cn(
        # mss_contract
        text(
            "alter table identifier(:tst) add column is_mss_contract varchar default 'N';"
        ).bindparams(ts_table_param)
    )

    exec_sql_local_cn(
        # mss_contract
        text(
            "update identifier(:tst) r set r.is_mss_contract = 'Y' where r.last_coverage_end_date > r.ldos;"
        ).bindparams(ts_table_param)
    )

    exec_sql_local_cn(
        # atr action bucket
        text(
            """update identifier(:tst) r set r.atr_action_bucket =
            case
            when r.is_tss_htec = 'Y' then 'TSS HTEC'
            when r.is_bcs_sub = 'Y' then 'BCS SUBSCRIPTION'
            when r.is_disqual_install_base_status = 'Y' then 'DISQUALIFIED ATR BY INSTALL BASE STATUS'
            when r.is_dnr_flag = 'Y' then 'DNR (DO NOT RENEW FLAG)'
            when r.is_mss_contract = 'Y' then 'MSS CONTRACT'
            when r.is_past_ldos_mss_decided = 'Y' then 'PAST LDOS (MSS DECLINED)' 
            when r.is_past_ldos_mss_potential = 'Y' then 'PAST LDOS (MSS POTENTIAL)'
            when r.is_coterm_to_ldos_mss_declined = 'Y' then 'COTERMED TO LDOS (MSS DECLINED)'
            when r.is_coterm_to_ldos_mss_potential = 'Y' then 'COTERMED TO LDOS (MSS POTENTIAL)'
            when r.is_parent_passed_ldos_mss_potential = 'Y' then 'PARENT PAST LDOS (MSS POTENTIAL)'
            when r.is_parent_passed_ldos_mss_declined = 'Y' then 'PARENT PAST LDOS (MSS DECLINED)' 
            when r.is_parent_coterm_ldos_mss_potential = 'Y' then 'PARENT COTERMED TO LDOS (MSS POTENTIAL)' 
            when r.is_parent_coterm_ldos_mss_declined = 'Y' then 'PARENT COTERMED TO LDOS (MSS DECLINED)' 
            when r.is_not_latest_installed_compliance = 'Y' then 'NOT LATEST INSTALLED (=COMPLIANCE FLAG/COVERAGE RESTRICTED)'
            when r.is_correct_customer = 'N' then 'WRONG CUSTOMER'
            when r.is_sfc_exclusion = 'Y' then 'SFC EXCLUSION'
            when r.is_sfc_scope = 'Y' then 'SFC SCOPE'
            when r.is_sfc_scope_other_partner = 'Y' then 'SFC IN SCOPE OTHER PARTNER'
            when r.is_not_sfc_is_cotermed_next_renewal_through_atr_goaling_period = 'Y' then 'NON-SFC COTERMED TO NEXT RENEWAL THROUGH END OF ATR GOALING PERIOD' 
            when r.is_not_sfc_is_cotermed_next_renewal_before_atr_goaling_period =  'Y' then 'NON-SFC COTERMED TO NEXT RENEWAL BEFORE END OF ATR GOALING PERIOD'  
            when r.is_not_sfc_not_cotermed_next_renewal = 'Y' then 'NON-SFC NOT COTERMED TO NEXT RENEWAL'
            when r.is_sfc_cotermed_with_mso = 'Y' then 'SFC COTERMED WITH MSO'
            when r.is_sfc_cotermed_no_mso = 'Y' then CONCAT_WS(' ', 'SFC COTERMED', :pricing_model, 'WITHOUT MSO') 
            else 'CAM REVIEW' end ; """
        ).bindparams(ts_table_param, pricing_model_param)
    )
    return True


@task(log_stdout=True, checkpoint=False)  # This is too large to checkpoint
@log_queries
def generate_output(
    args: GenArgsOutput,
    thought_spot_table: str,
    schema: str,
    env: T_ENV,
    wh: str,
) -> pd.DataFrame:
    atr_table_param = bindparam("atr_table", quoted_name(args["atr_table_name"], False))
    thought_spot_table_param = bindparam("tst", quoted_name(thought_spot_table, False))
    final_qry = text(
        """
        SELECT a.INSTANCE_ID
        ,DEAL_ID
        ,PARENT_INSTANCE_ID
        ,COVERAGE_STATUS
        ,INSTALLED_BASE_STATUS
        ,SERIAL_NUMBER
        ,CONFIGURATION_TYPE
        ,PID
        ,ITEM_TYPE
        ,LAST_COVERAGE_END_DATE
        ,COVERAGE_DETAILS_MONTHS
        ,PRODUCT_COVERAGE_TERMINATION_DATE
        ,LDOS
        ,MAPPED_TO_SERVICE_FLAG_ACAT
        ,PRODUCT_DESCRIPTION
        ,PRODUCT_FAMILY
        ,PRODUCT_TYPE
        ,QUANTITY
        ,ARCHITECTURE
        ,SUB_ARCHITECTURE
        ,INSTALLED_AT_CUSTOMER_NAME
        ,INSTALLED_AT_ADDRESS_LINES
        ,INSTALLED_AT_PROVINCE
        ,INSTALLED_AT_CITY
        ,INSTALLED_AT_POSTAL_CODE
        ,INSTALLED_AT_COUNTRY
        ,INSTALLED_AT_GUID
        ,INSTALLED_AT_GU_NAME
        ,INSTALLED_AT_PARTY_ID_PARENT
        ,INSTALLED_AT_PARTY_NAME_PARENT
        ,INSTALLED_AT_PARTY_ID
        ,INSTALLED_AT_PARTY_ID_NAME
        ,INSTALLED_AT_SITE_ID
        ,CONTRACT_BILL_TO_ID
        ,CONTRACT_NUMBER
        ,SERVICE_LEVEL
        ,CONTRACT_STATUS
        ,CONTRACT_BILL_TO_NAME
        ,CONTRACT_BILL_TO_GUID
        ,CONTRACT_BILL_TO_GU_NAME
        ,SERVICE_LEVEL_DESCRIPTION
        ,MSS_CONTRACT_FLAG
        ,SFC_FLAG
        ,MSO
        ,MISSING_MSO
        ,PAST_LDOS
        ,PRODUCT_EOL_FLAG
        ,CXEA_FLAG
        ,DNR_FLAG
        ,RENEWAL_CATEGORY
        ,PARENT_INSTANCE
        ,PARENT_SERIAL_NUMBER
        ,INSTALL_SITE_SYNCH_IN_CONFIG_FLAG
        ,COVERED_STATUS
        ,MEU_ALLOWED_CONTRACT_FLAG
        ,PRODUCT_PO
        ,PARENT_LDOS
        ,END_OF_NEW_SVC_ATTACHMENT_DT
        ,END_OF_TAC_SUPPORT_DATE
        ,LAST_DATE_OF_SERVICE_ATTACHED
        ,LAST_COVERAGE_DATE
        ,IS_MSS_AVAILABLE
        ,MSS_AVAILABLE_TO_DATE
        ,EXISTING_MSS_COVERAGE
        ,LAST_COVERAGE_FISCAL_QUARTER
        ,VIRTUAL_ACCOUNTS
        ,SMART_ACCOUNTS
        ,MSS_ASSESSMENT_STATUS
        ,IS_TSS_HTEC
        ,IS_BCS_SUB
        ,IS_DISQUAL_INSTALL_BASE_STATUS
        ,IS_PAST_LDOS_MSS_DECIDED
        ,IS_PAST_LDOS_MSS_POTENTIAL
        ,IS_COTERM_TO_LDOS_MSS_DECLINED
        ,IS_COTERM_TO_LDOS_MSS_POTENTIAL
        ,IS_PARENT_PASSED_LDOS_MSS_POTENTIAL
        ,IS_NOT_LATEST_INSTALLED_COMPLIANCE
        ,IS_DNR_FLAG
        ,IS_CORRECT_CUSTOMER
        ,IS_SFC_EXCLUSION
        ,IS_COTERM
        ,IS_COTERM_TO_LDOS
        ,IS_SFC_SCOPE
        ,IS_SFC_SCOPE_OTHER_PARTNER
        ,IS_NOT_SFC_IS_COTERMED_NEXT_RENEWAL_THROUGH_ATR_GOALING_PERIOD
        ,IS_NOT_SFC_IS_COTERMED_NEXT_RENEWAL_BEFORE_ATR_GOALING_PERIOD
        ,IS_NOT_SFC_NOT_COTERMED_NEXT_RENEWAL
        ,IS_NOT_SFC_IS_COTERMED_NEXT_RENEWAL
        ,IS_SFC_COTERMED_WITH_MSO
        ,IS_SFC_COTERMED_NO_MSO
        ,CAM_CONTRACT
        ,ATR_ACTION_BUCKET
        ,a.ATR_DOLLARS from identifier(:tst) r
      join identifier(:atr_table) a on (a.INSTANCE_ID=r.INSTANCE_ID)
      """
    ).bindparams(thought_spot_table_param, atr_table_param)
    engine = create_engine(
        sec.get_sf_pw(sec.check_env(env), wh, schema),
        connect_args={"log_max_query_length": 10_000},
    )
    with engine.connect() as conn:
        df = pd.read_sql(final_qry, conn)

    # ATR Action Table
    bucket_actions = Config.get_action_buckets()

    def _apply_bucket_actions(row, key: str) -> dict[str, str]:
        """Lookup columns based on key"""
        key_value = getattr(row, key)
        if key_value in bucket_actions:
            return bucket_actions[key_value]
        else:
            return bucket_actions["__default__"]

    apply_bucket_actions = partial(_apply_bucket_actions, key=Config.ACTION_BUCKET_KEY)

    df[Config.ACTION_BUCKET_COLS] = df.apply(
        apply_bucket_actions, axis=1, result_type="expand"
    )

    # Add 'cam_comments' column to the end of the dataframe
    # Add empty columns to the end of the dataframe
    empty_cols = ["atr_action_bucket_cam_validated", "cam_comments"]
    for col in empty_cols:
        df[col] = ""
    cols = [c for c in df.columns.tolist() if c not in empty_cols]
    cols.extend(empty_cols)
    df = df[cols]
    return df


@task(log_stdout=True)
def store_output(args: GenArgsOutput, df: pd.DataFrame, flow_env: T_FLOW) -> bool:
    """
    Store our final DataFrame to Either S3 (prod) or Local (dev)

    Parameters
    ----------
    args : GenArgsOutput
    df: pd.DataFrame
    flow_env: T_FLOW

    Notes
    -----
    args['output_uri'] will be treated as a file path if flow_env is dev
    """

    # Convert any pandas timestamp columns to native python datetime
    # Select by dtype
    date_cols = df.select_dtypes(include=["datetime64"]).columns.tolist()
    for col in date_cols:
        df[col] = df[col].dt.date

    upload_task = DataFrameUploadTask()
    print(f"Writing to {args['output_uri']}")
    if flow_env == "dev":
        upload_task.run(df=df, file_path=args["output_uri"], output_uri=None)
    else:
        upload_task.run(df=df, file_path=None, output_uri=args["output_uri"])

    return True


@task(log_stdout=True)
@log_queries
def remove_working_tables(run_params: GenArgsOutput, env: T_ENV, schema: str, wh: str):
    engine = create_engine(
        sec.get_sf_pw(sec.check_env(env), wh, schema),
        connect_args={"log_max_query_length": 10_000},
    )
    tables = []
    table_keys = (
        "atr_table_name",
        "coverage_table_name",
        "multis_table_name",
        "notes_table_name",
        "flat_table_name",
        "core_table_name",
    )
    for table_key in table_keys:
        table_name = run_params.get(table_key)  # type: ignore
        table_name_fqn = f"{schema}.{table_name}".lower()
        tables.append(table_name_fqn)

    con = engine.connect()
    for table in tables:
        stmt = text(f"DROP TABLE IF EXISTS identifier(:tbl) ;").bindparams(
            bindparam("tbl", quoted_name(table, False))
        )
        try:
            con.execute(stmt)
        except Exception as e:
            print(e)
    con.close()
    return tables


@task(log_stdout=True, tags=["snowflake_xsmall"])
@log_queries
def update_generic_upload_log_table(
    run_params: GenArgsOutput, env: T_ENV, flow_env: T_FLOW, wh: str, schema: str
):
    if flow_env == "dev":
        print("Not updating generic upload log table (dev)")
        return True
    output_uri_param = bindparam("output_uri", run_params["output_uri"], type_=VARCHAR)
    request_id_param = bindparam(
        "request_id", run_params["run_identifier"], type_=VARCHAR
    )
    stmt = text(
        """
        UPDATE CPS_DB.CPS_DSCI_API.DATA_CANVAS_GENERIC_UPLOAD
        set STATUS = 'Success', OUTPUT_FILE_PATH = :output_uri               
        where REQUEST_ID = :request_id;
        """
    ).bindparams(output_uri_param, request_id_param)
    engine = create_engine(
        sec.get_sf_pw(sec.check_env(env), wh, schema),
        connect_args={"log_max_query_length": 10_000},
    )
    with engine.begin() as conn:
        try:
            conn.execute(stmt)
        except Exception as e:
            print(e)
            print(
                f"Failed to update generic request_id: {run_params['run_identifier']}"
            )
    return True


@task(log_stdout=True, trigger=any_failed, tags=["snowflake_xsmall"])
@log_queries
def update_generic_upload_log_table_failed(
    request_id: str, env: T_ENV, flow_env: T_FLOW, wh: str, schema: str
):
    if flow_env == "dev":
        print("Not updating generic upload log table (dev)")
        return True
    request_id_param = bindparam("request_id", request_id, type_=VARCHAR)
    stmt = text(
        """
        UPDATE CPS_DB.CPS_DSCI_API.DATA_CANVAS_GENERIC_UPLOAD
        set STATUS = 'Failed'
        where REQUEST_ID = :request_id;
        """
    ).bindparams(request_id_param)
    engine = create_engine(
        sec.get_sf_pw(sec.check_env(env), wh, schema),
        connect_args={"log_max_query_length": 10_000},
    )
    with engine.begin() as conn:
        try:
            conn.execute(stmt)
        except Exception as e:
            print(e)
            print(f"Failed to update generic request_id: {request_id}")
    return True


storage_obj = Docker(
    base_image="837578041534.dkr.ecr.us-east-1.amazonaws.com/bases/prefect:0.15.13-python3.9",
    python_dependencies=[
        "'pandas~=1.5'",
        "'awswrangler~=2.20'",
        "'numpy~=1.22'",
        "'boto3~=1.18'",
        "'aiohttp'",
        "'snowflake-sqlalchemy>=1.4,<2.0'",
        "'s3fs==0.4'",
        "'hvac>0.11.0'",
        "'SQLAlchemy>=1.4,<2.0'",
        "'fastparquet>0.7.1'",
        "'XlsxWriter~=3.0'",
        "'oyaml'",
        "'pyxlsb'",
        "'openpyxl'",
    ],
    registry_url="837578041534.dkr.ecr.us-east-1.amazonaws.com/flows",
    files={str(Path.cwd() / "common"): "/root/.prefect/flows/common"},
    env_vars={"PYTHONPATH": "${PYTHONPATH}:/root/.prefect/flows/"},
)

with Flow(
    "atr-cam-flow",
    storage=add_wheels(storage_obj),
    run_config=KubernetesRun(memory_request="6G"),
    executor=LocalExecutor()
    if os.getenv("FLOW_ENV") == "dev"
    else LocalDaskExecutor(
        scheduler="processes",
        num_workers=1 if os.getenv("FLOW_ENV") == "dev" else os.cpu_count(),
    ),
    result=(
        LocalResult(dir="~//.prefect/results/atr-cam-flow")
        if os.getenv("FLOW_ENV") == "dev"
        else S3Result(bucket="cam-prefect-results")
    ),
) as flow:
    request_id_p = Parameter("request_id", required=True)
    bucket_name_p = Parameter("bucket_name", required=False, default=None)
    file_location_p = Parameter("file_location", required=True)
    env_param = Parameter("env", default="prod", required=False)
    flow_env_param = Parameter("flow_env", default="prod", required=False)
    eid_param = Parameter("eid", default=Config.EID, required=False)
    schema_param = Parameter("schema", default=Config.WORKING_SCHEMA, required=False)
    wh_lg_param = Parameter("wh_lg", default=Config.WAREHOUSE_LARGE, required=False)
    wh_sm_param = Parameter("wh_sm", default=Config.WAREHOUSE_SMALL, required=False)
    wh_xs_param = Parameter("wh_xs", default=Config.WAREHOUSE_XSMALL, required=False)

    data_mapping_result = get_json_from_s3(
        bucket="canvas-data-types", key="pandas_data_type_map.json"
    )
    rename_map_result = get_json_from_s3(
        bucket="canvas-data-types", key="canvas_col_rename.json"
    )
    display_map_result = get_json_from_s3(
        bucket="canvas-data-types", key="ts_display_name_map.json"
    )

    gen_args_result = gen_args(
        file_location=file_location_p,
        bucket_name=bucket_name_p,
        env=env_param,
        flow_env=flow_env_param,
        request_id=request_id_p,
        schema=schema_param,
        wh=wh_sm_param,
        data_mapping=data_mapping_result,
    )

    atr_table_name_result = gen_args_result["atr_table_name"]
    coverage_table_name_result = gen_args_result["coverage_table_name"]
    multis_table_name_result = gen_args_result["multis_table_name"]
    notes_table_name_result = gen_args_result["notes_table_name"]
    flat_table_name_result = gen_args_result["flat_table_name"]
    core_table_name_result = gen_args_result["core_table_name"]

    coverage_table_result = gen_coverage_data(
        scope_instance_tbl_name=atr_table_name_result,
        coverage_table_name=coverage_table_name_result,
        env=env_param,
        wh=wh_lg_param,
        schema=schema_param,
    )
    enrich_coverage_table = coverage_enrichments(
        coverage_table_name=coverage_table_result,
        eid=eid_param,
        env=env_param,
        wh=wh_sm_param,
        schema=schema_param,
    )
    multi_row_tbl_result = gen_current_data(
        multis_table_name=multis_table_name_result,
        scope_instance_table_name=atr_table_name_result,
        coverage_table_name=enrich_coverage_table,
        env=env_param,
        wh=wh_lg_param,
        schema=schema_param,
    )
    notes_tbl = gen_notes(
        multi_row_table_name=multi_row_tbl_result,
        notes_table_name=notes_table_name_result,
        env=env_param,
        wh=wh_sm_param,
        schema=schema_param,
    )

    flt_table = flatten_data(
        flat_table_name=flat_table_name_result,
        multi_row_tbl=multi_row_tbl_result,
        env=env_param,
        wh=wh_sm_param,
        schema=schema_param,
    )
    thought_spot_table_result = prep_final(
        core_table_name=core_table_name_result,
        multis_table_name=multi_row_tbl_result,
        notes_tbl_name=notes_tbl,
        flat_tbl=flt_table,
        env=env_param,
        wh=wh_sm_param,
        schema=schema_param,
    )
    parents = fix_missing_parents(
        thought_spot_table=thought_spot_table_result,
        flat_table=flt_table,
        env=env_param,
        wh=wh_sm_param,
        schema=schema_param,
    )

    enrich_canvas_table = canvas_enrichments(
        ts_table=thought_spot_table_result,
        env=env_param,
        wh=wh_sm_param,
        schema=schema_param,
    )
    enrich_canvas_table.set_upstream(parents)

    renamed = rename_cols_in_preexsisting_table(
        thought_spot_table=thought_spot_table_result,
        env=env_param,
        wh=wh_lg_param,
        schema=schema_param,
        rename_map=rename_map_result,
        display_map=display_map_result,
    )
    renamed.set_upstream(enrich_canvas_table)
    rules_processed = proc_sql_rules(
        args=gen_args_result,
        thought_spot_table=thought_spot_table_result,
        schema=schema_param,
        env=env_param,
        wh=wh_sm_param,
    )
    rules_processed.set_upstream(renamed)
    output_result = generate_output(
        args=gen_args_result,
        thought_spot_table=thought_spot_table_result,
        schema=schema_param,
        env=env_param,
        wh=wh_sm_param,
    )
    output_result.set_upstream(rules_processed)

    output_file_result = store_output(
        args=gen_args_result,
        df=output_result,
        flow_env=flow_env_param,
    )

    cleaned = remove_working_tables(
        run_params=gen_args_result,
        env=env_param,
        schema=schema_param,
        wh=wh_xs_param,
    )
    cleaned.set_upstream(output_file_result)

    log_updated = update_generic_upload_log_table(
        run_params=gen_args_result,
        env=env_param,
        flow_env=flow_env_param,
        wh=wh_xs_param,
        schema=schema_param,
        upstream_tasks=[cleaned],
    )

    failed_flow = update_generic_upload_log_table_failed(
        request_id=request_id_p,
        env=env_param,
        flow_env=flow_env_param,
        wh=wh_xs_param,
        schema=schema_param,
        upstream_tasks=[cleaned],
    )

    # If log_updated runs, the flow succeeded. If failed_flow runs, the flow failed.
    flow.set_reference_tasks([log_updated])

if __name__ == "__main__":
    flow.run(
        parameters=dict(
            request_id="424242",
            file_location=sys.argv[1],
            flow_env="dev",
        )
    )
