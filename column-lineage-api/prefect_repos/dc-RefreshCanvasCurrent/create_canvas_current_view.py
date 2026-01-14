import json
import psutil
import os
import random
from datetime import datetime
import string
import shutil
import awswrangler as wr
import boto3
import numpy as np
import pandas as pd
from prefect import Flow, Parameter, task, case
from prefect.tasks.aws.s3 import S3Upload
from sqlalchemy import create_engine
from common import sec
from prefect.engine.signals import SKIP
import pathlib
from typing import Dict

from prefect.tasks.prefect import RenameFlowRun
from common.trigger_prefect_flow import trigger_cloud_flow_run, trigger_ts_refresh_tags
from common.log_to_dc_job_messages import log_to_dc_job_messages, final_flow_state_message

from common import config

temp_base_location = "/mnt/newmt/ERP/home/alanzen/bulk_tmp"
from prefect.engine.results.s3_result import S3Result
from prefect.executors.dask import LocalDaskExecutor
from prefect.run_configs.kubernetes import KubernetesRun
from prefect.storage import Docker
from pathlib import Path
import oyaml

from sqlalchemy import create_engine, text, bindparam, Integer, column, String
import prefect.engine.signals as signals
from prefect.triggers import (
    all_successful,
    some_successful,
    not_all_skipped,
    all_failed,
    all_finished,
    always_run,
    some_failed,
)
import time


def log_msg(task, old_state, new_state):
    if new_state.is_skipped():
        msg = f"{task.name} FAILED VALIDATION.."
        failure_notes.append(msg)
        print(f"{msg}")
    elif new_state.is_failed():
        msg = f"{task.name} FAILED EXECUTION"
        failure_notes.append(msg)
        print(msg)
    return new_state


def check_env(env):
    print(env)
    if env == "dev":
        cn = "dev_cps_dsci_etl_svc"
    elif env == "stage":
        cn = "stg_cps_dsci_etl_svc"
    elif env == "prod":
        cn = "prd_cps_dsci_etl_svc"
    return cn


def fix_cols(df):
    cols = []
    for c in df.columns:
        cl = c.lower()
        cols.append(cl.strip().replace(" ", "_").replace("/", "_").replace("\\", "_").replace("-", "_"))
    return cols


def rename_standard_cols(df):
    #         rename_map = dict(zip(standard_df.col, standard_df.real_name))  #cant use show bc wa only know wheat we DO NOT weant to sheo
    rename_map = get_json_from_s3("canvas-data-types", "canvas_col_rename.json")
    df.rename(columns=rename_map, inplace=True)
    return df


def rename_canvas_create_cols(df):
    #         rename_map = dict(zip(standard_df.col, standard_df.real_name))  #cant use show bc wa only know wheat we DO NOT weant to sheo
    rename_map = get_json_from_s3("canvas-data-types", "canvas_prep_final_name_map.json")
    df.rename(columns=rename_map, inplace=True)
    return df


def remove_hidden_cols(df):
    hidden_cols = get_json_from_s3("canvas-data-types", "canvas_cols_to_be_hidden.json")
    hidden_list = list(set(df.columns).intersection(set(hidden_cols)))
    # print(df.shape)
    df.drop(hidden_list, axis=1, inplace=True)
    # print(df.shape)
    return df


def get_json_from_s3(bucket, key):
    s3 = boto3.resource("s3")
    obj = s3.Object(bucket, key)
    data = obj.get()["Body"].read().decode("utf-8")
    json_data = oyaml.safe_load(data)
    return json_data


def fix_numbers(s):
    s = pd.to_numeric(s.convert_dtypes(), errors="coerce")
    s = pd.to_numeric(s, errors="coerce").convert_dtypes()
    return s


def prep_data(df):
    # run after standard rename
    pandas_data_type_map = get_json_from_s3("canvas-data-types", "pandas_data_type_map.json")
    for k in df.columns:
        # print(k,pandas_data_type_map.get(k, 'GO DEFINE IT') )
        if pandas_data_type_map.get(k, "xxxxx") in ["Int64", "float64", "int"]:  # "str" had this
            df[k] = fix_numbers(df[k])
        elif pandas_data_type_map.get(k, "xxxxx") in ["datetime64[ns]"]:
            df[k] = pd.to_datetime(df[k], errors="coerce")
        elif pandas_data_type_map.get(k, "xxxxx") in ["str"]:
            df[k] = df[k].astype("str")
        else:
            df[k] = df[k].astype("str")
    df = df.replace(["nan", "None", "<NA>"], np.nan)
    return df


@task(log_stdout=True, tags=["snowflake_large"])
def rename_cols_in_preexsisting_table(thought_spot_table, env, schema, cid, notification_id, requested_by, ):
    engine = create_engine(sec.get_sf_pw(check_env("prod"), "CPS_DSCI_ETL_EXT3_WH", schema))
    con = engine.connect()
    df = pd.DataFrame(con.execute(f"desc table {thought_spot_table}").fetchall())
    df.columns = fix_cols(df)
    # col -> real
    rename_map = get_json_from_s3("canvas-data-types", f"{env}/canvas_col_rename.json")
    # devide level and canvas create specific "in process" -> real name
    # none were changed...
    # proc_rename_map = get_json_from_s3('canvas-data-types', 'canvas_prep_final_name_map.json')
    # real -> display
    display_map = get_json_from_s3("canvas-data-types", f"{env}/ts_display_name_map.json")

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

    for i, row in this_df[this_df.orig != this_df.new_value].iterrows():
        sql = f"alter table {thought_spot_table} rename column {row.orig} to {row.new_value};"
        print(sql)
        con.execute(sql)

    # add drop hidden fields here
    log_to_dc_job_messages(env, cid, f"SUCCESS: Step 10/12 renamed columns in {thought_spot_table}.", requested_by,
                           notification_id)
    con.close()
    return True


@task(log_stdout=True)
def upload_parquets(loc, canvas_id, run_date_mod, aws_loc):
    loc_path = os.path.join(loc, canvas_id)
    if os.path.isdir(loc_path):
        files_to_move = os.listdir(loc_path)
        print(files_to_move)
        OBJ_TO_delete = wr.s3.list_objects(aws_loc)
        print(OBJ_TO_delete)
        for otd in OBJ_TO_delete:
            print(f"del: {otd}")
            wr.s3.delete_objects(otd)
        for f in files_to_move:
            print(os.path.join(loc_path, f), os.path.join(aws_loc, f))
            wr.s3.upload(local_file=os.path.join(loc_path, f), path=os.path.join(aws_loc, f))


@task(log_stdout=True, tags=["snowflake_large"])
def fix_missing_parents(thought_spot_table, flat_table, env, wh, schema, cid, notification_id, requested_by, ):
    dest_tbl = f"{thought_spot_table}_missing_parents".upper()

    sql = f"""
        create or replace Transient table {dest_tbl} as
        with sub as (
            select distinct c.parent_instance_id
            from {thought_spot_table} c
        ) , missing as
            (
            select sub.PARENT_INSTANCE_ID from sub
            left join {thought_spot_table} c on (sub.PARENT_INSTANCE_ID=c.instance_id)
            where c.instance_id is null
            ),kids as
                (
                select min(c.instance_id) as mn_ins, c.PARENT_INSTANCE_ID
                from {thought_spot_table} c
                    join missing on (c.PARENT_INSTANCE_ID = missing.PARENT_INSTANCE_ID)
                group by c.PARENT_INSTANCE_ID
                )
            select * from kids;




    -- fix aggs for missing parents
    update  {thought_spot_table} c
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
           c.device_level_installed_at_site_id_total = nvl(fix.device_level_installed_at_site_id_total,0),
           c.device_level_gplp_x_quantity = nvl(fix.device_level_gplp_x_quantity,0),
            c.device_level_gplp_list_price = nvl(fix.device_level_gplp_list_price,0)
    from (select p.MN_INS , f.*  from {dest_tbl} p
           join {flat_table} f on (f.PARENT_INSTANCE_ID=p.PARENT_INSTANCE_ID  )
           ) fix
            where fix.MN_INS = c.instance_id ;



    drop table {dest_tbl};
            """

    engine = create_engine(sec.get_sf_pw(check_env("prod"), "CPS_DSCI_ETL_EXT3_WH", schema))
    con = engine.connect()

    for s in sql.split(";"):
        print(s)
        res = con.execute(s)
    log_to_dc_job_messages(env, cid, f"SUCCESS: Step 8/12 Fixed missing parents.", requested_by, notification_id)
    # con.close()
    return True


@task(log_stdout=True, tags=["snowflake_large"])
def coverage_enrichments(coverage_table, eid, env, wh, schema, cid, notification_id, requested_by, ):
    sql = f"""


    alter table {coverage_table} add column   is_mss_available varchar default 'NA';
    alter table {coverage_table} add column   existing_mss_coverage varchar default 'NA';
    alter table {coverage_table}  add column  mss_available_to_date date;
    alter table {coverage_table}  add column  mss_service_available varchar default 'NA';



    --this gets the actual MSS SL available (CURRENT MSS is NA which is an issue)aron TODO 
    update {coverage_table} c
         set  c.existing_mss_coverage = case when o.MSS_SERVICE_LEVEL_GROUP is null then '-' else 'MSS_COVERAGE' end,
              c.mss_service_available = o.MSS_SERVICE_LEVEL_GROUP
         from (     select arrayagg(distinct mss_service_level_group) within group (order by mss_service_level_group) as sslg,
                           mss_service_level_group
                    from CPS_DSCI_ARCHIVE.MSS_PCODE_MAPPING mss
                    group by mss_service_level_group
              ) o
         where ARRAYS_OVERLAP(array_construct(c.service_level ), o.sslg);

    update {coverage_table}   c
             set  c.is_mss_available = o.assessment_status ,
                  c.mss_available_to_date = o.assessment_support_end_date
             from SERVICES_DB.SERVICES_MSS_BR.MSS_OPPORTUNITIES o
             where c.instance_id = o.instance_id;



      alter table {coverage_table} add column  am_service_contract_type varchar default 'NA';
      alter table {coverage_table} add column  am_offer_type varchar default 'NA';
      alter table {coverage_table} add column  am_contract_allowed_srv_lvl varchar default 'NA';
      alter table {coverage_table} add column  responsible_users varchar default 'NA';
      alter table {coverage_table} add column  monitor_reason varchar default 'NA';
      alter table {coverage_table}  add column  contract_name varchar default 'NA';

      update {coverage_table} c
         set  c.am_service_contract_type = o.BUYING_PROGRAM_NAME,
              c.am_offer_type = o.SOLD_AS_SERVICE_NAME,
              c.am_contract_allowed_srv_lvl = o.ALLOWED_SERVICE_LEVELS,
              c.responsible_users = o.responsible_users,
              c.monitor_reason = o.monitor_reason,
              c.contract_name=o.contract_name
         from (



        with msc as (
                    select mc.CONTRACT_NUMBER,
                               listagg(distinct mc.ALLOWED_SERVICE_LEVELS ,',') WITHIN GROUP (ORDER BY mc.ALLOWED_SERVICE_LEVELS) as ALLOWED_SERVICE_LEVELS,
                               listagg(distinct mc.CONTRACT_NAME,',') WITHIN GROUP (ORDER BY mc.CONTRACT_NAME) as CONTRACT_NAME,
                               listagg(distinct ct.SOLD_AS_SERVICE_NAME,',') WITHIN GROUP (ORDER BY ct.SOLD_AS_SERVICE_NAME) as SOLD_AS_SERVICE_NAME,
                               listagg(distinct ctt.BUYING_PROGRAM_NAME,',') WITHIN GROUP (ORDER BY ctt.BUYING_PROGRAM_NAME) as BUYING_PROGRAM_NAME,
                               listagg(distinct u.CISCO_CCO_ID,',' ) WITHIN GROUP (ORDER BY u.CISCO_CCO_ID ) as responsible_users,
                               'MANAGED' as monitor_reason
                        from {schema}.dc_BOOKINGS_CONTRACTS c
                        join {schema}.dc_BOOKINGS_CONTRACTS_RESPONSIBLE_USERS r on ( r.BOOKING_CONTRACT=c.BOOKING_CONTRACT)
                        join {schema}.dc_ENGAGEMENT_TO_BOOKINGS_RESPONSIBLE_USER eu on ( eu.BOOKING_CONTRACT=r.BOOKING_CONTRACT and eu.DC_USER_ID=r.DC_USER_ID )
                        join {schema}.dc_managed_service_contracts mc on ( mc.DC_USER_ID=eu.DC_USER_ID and mc.BOOKING_CONTRACT=eu.BOOKING_CONTRACT and mc.DC_ENGAGEMENT_ID = eu.DC_ENGAGEMENT_ID)
                        join {schema}.DC_USERS u on (u.USER_ID=mc.DC_USER_ID)
                        left join {schema}.dc_sold_as_service_types ct on ( ct.service_type_id =c.SOLD_AS_SERVICE_TYPE_ID )
                        left join {schema}.dc_buying_programs ctt on (ctt.buying_program_type_id=c.BUYING_PROGRAM_TYPE_ID)
                        where eu.DC_ENGAGEMENT_ID = {eid}  and c.IS_DELETED = 'F' and  r.IS_DELETED = 'F'  and  eu.IS_DELETED = 'F'  and  mc.IS_DELETED = 'F'
                        group by CONTRACT_NUMBER
                        ), monsc as (select monc.CONTRACT_NUMBER,
                                            'NA' as ALLOWED_SERVICE_LEVELS,
                                            'NA' as CONTRACT_NAME,
                                            'NA' as SOLD_AS_SERVICE_NAME,
                                            'NA' as BUYING_PROGRAM_NAME,
                                            'NA' as Responsible_users,
                                            mt.MONITOR_REASON
                                     from {schema}.dc_BOOKINGS_CONTRACTS c
                                              join {schema}.dc_BOOKINGS_CONTRACTS_RESPONSIBLE_USERS r
                                                   on (r.BOOKING_CONTRACT = c.BOOKING_CONTRACT)
                                              join {schema}.dc_ENGAGEMENT_TO_BOOKINGS_RESPONSIBLE_USER eu
                                                   on (eu.BOOKING_CONTRACT = r.BOOKING_CONTRACT and
                                                       eu.DC_USER_ID = r.DC_USER_ID)
                                              join {schema}.DC_MONITOR_SERVICE_CONTRACTS monc
                                                   on (monc.DC_ENGAGEMENT_ID = eu.DC_ENGAGEMENT_ID)
                                              join {schema}.dc_CONTRACT_MONITOR_TYPES mt
                                                   on (mt.monitor_type_id = monc.MONITOR_TYPE_ID)
                                     where eu.DC_ENGAGEMENT_ID =  {eid} 
                                       and c.IS_DELETED = 'F'
                                       and r.IS_DELETED = 'F'
                                       and eu.IS_DELETED = 'F'
                                       and monc.IS_DELETED = 'F'
                                       AND NOT EXISTS (SELECT 0
                                                       FROM msc
                                                       where msc.CONTRACT_NUMBER = monc.CONTRACT_NUMBER))
                        select * from monsc union select * from msc
    ) o
     where c.CONTRACT_NUMBER = o.contract_number;


    -- WE ARE REALLY OVER WRITING THIS VALUE BC IT NEEDED TO LOOK ACROSS FUTURE AND CURRENT VS THE VIEW DEF OF MAX(END | TERM)

    UPDATE {coverage_table} T SET  T.last_coverage_date=CORRECT.MX
    FROM (
        SELECT max(LAST_COVERAGE_DATE) MX, INSTANCE_ID
        FROM {coverage_table}
        WHERE FLAG_WE_USE IN ('L', 'Z')
        GROUP BY INSTANCE_ID
    ) CORRECT
    WHERE T.INSTANCE_ID = CORRECT.INSTANCE_ID;




    alter table {coverage_table}  add column last_coverage_fiscal_quarter varchar(40);
    update  {coverage_table}  i set last_coverage_fiscal_quarter = d.FISCAL_QTR_SORTED_NAME
        from CPS_DSCI_ARCHIVE.DIM_DATE_NEW d where d.DATE = i.last_coverage_date;



    alter table {coverage_table}  add column flat_coverage_status varchar;


    update {coverage_table} c set c.flat_coverage_status = 'ACTIVE'
    from
    (
        select distinct INSTANCE_ID
        from {coverage_table} 
        where current_date between  START_DATE and last_coverage_date


    )i
    where i.INSTANCE_ID = c.INSTANCE_ID and c.flat_coverage_status is null;


    update {coverage_table}  c set c.flat_coverage_status = 'ACTIVE/SIGNED'
        from
        (
              select distinct INSTANCE_ID
                from {coverage_table} 
                where STS_CODE in ( 'SIGNED')
                 and current_date <= START_DATE

        )i
        where i.INSTANCE_ID = c.INSTANCE_ID and c.flat_coverage_status= 'ACTIVE';

    update {coverage_table}  c set c.flat_coverage_status = 'SIGNED'
        from
        (
              select distinct INSTANCE_ID
                from {coverage_table} 
                where STS_CODE in ( 'SIGNED')
                 and current_date <=  START_DATE

        )i
        where i.INSTANCE_ID = c.INSTANCE_ID and c.flat_coverage_status is null;




    update {coverage_table}  c set c.flat_coverage_status = 'NOT ACTIVE'
    from
    (
        select INSTANCE_ID, count(distinct STS_CODE ) as cnt
        from {coverage_table} 
        where current_date > last_coverage_date
        group by INSTANCE_ID
    )i
    where i.INSTANCE_ID = c.INSTANCE_ID and c.flat_coverage_status is null;





    alter table {coverage_table}  add column signed_mso varchar;
    alter table {coverage_table}  add column signed_bill_to varchar;

    update {coverage_table}  c
    set c.signed_mso = i.mso, c.signed_bill_to = i.contract_bill_to
    from(
        select INSTANCE_ID,
               listagg( distinct MAINTENANCE_SO_NUMBER , ' | ') as mso,
               listagg( distinct  concat(contract_bill_to_name,':(',contract_bill_to_id,')') , ' | ') as contract_bill_to
        from {coverage_table} 
        where STS_CODE in ('SIGNED')
        group by INSTANCE_ID
    ) i
    where i.INSTANCE_ID = c.INSTANCE_ID;



         """

    engine = create_engine(sec.get_sf_pw(check_env("prod"), "CPS_DSCI_ETL_EXT3_WH", schema))
    con = engine.connect()
    for s in sql.split(";"):
        print(s)
        res = con.execute(s)
    log_to_dc_job_messages(env, cid, f"SUCCESS: Step 3/12 Generated coverage enrichments.", requested_by,
                           notification_id)
    return True


@task(log_stdout=True, tags=["snowflake_large"])
def canvas_enrichments(ts_table, eid, env, wh, schema, cid, notification_id, requested_by, ):
    print("----------------------{ts_table}---------------")

    sql = f"""


         alter table {ts_table}  add column covered_to_ldos varchar(40) default 'NO';


         update {ts_table}  i set i.covered_to_ldos = o.identifier
         from (
                 select INSTANCE_ID,
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
                 select parent_INSTANCE_ID,product_family
                 from {ts_table}
                 where parent_INSTANCE_ID =INSTANCE_ID

              ) o where o.parent_INSTANCE_ID= i.parent_INSTANCE_ID;




             alter table {ts_table} add column virtual_accounts varchar(5000);
             alter table {ts_table} add column smart_accounts varchar(5000);

             update {ts_table}  d set d.virtual_accounts=i.virtual_accounts, d.smart_accounts=i.smart_accounts
             from (
                     select
                     interesting.INSTANCE_ID,
                     listagg(   concat( nvl(sa.SMART_ACCOUNT_NAME,''), '_(', nvl(interesting.SMART_ACCOUNT,''),')'),'|') as SMART_ACCOUNTS,
                     listagg(   concat( nvl(va.VIRTUAL_ACCOUNT_NAME,''), '_(', nvl(interesting.VIRTUAL_ACCOUNT,''),')'),'|') as VIRTUAL_ACCOUNTS
                     from
                     {ts_table} ts
                     join CPS_DSCI_ARCHIVE.smart_account_to_instance_id interesting on (ts.INSTANCE_ID=interesting.INSTANCE_ID)
                     left join CPS_DSCI_EBV.BV_SMART_ACCOUNTS_D sa on (sa.SMART_ACCOUNT_KEY =try_to_number(interesting.smart_account))
                     left join CPS_DSCI_EBV.BV_VIRTUAL_ACCOUNTS_D va on (va.VIRTUAL_ACCOUNT_KEY = try_to_number(interesting.virtual_account))
                     group by interesting.INSTANCE_ID

                  ) i
             where i.INSTANCE_ID = d.INSTANCE_ID
             ;


             alter table {ts_table} add column  is_economically_viable varchar default 'F';
             alter table {ts_table} add column  is_fsu varchar default 'F';

             update {ts_table} c
                      set is_fsu = 'T'

                      from ( select pid  from  CPS_DSCI_API.FRU_PIDS
                           ) o
                      where c.PID=o.pid;


             update {ts_table} c
                      set is_economically_viable = 'T'
             where ( is_fsu = 'T' or  nvl(SERVICE_LIST_PRICE,0) > 0 ) and nvl(c.IS_MSS_AVAILABLE,'-') not in ('DECLINED');  



             -- Zone stuff below 
             alter table {ts_table}  add column   FIRST_DISCOVERY_DATE date ;
            alter table {ts_table}  add column   LAST_DISCOVERY_DATE date ;
            alter table {ts_table}  add column   DAYS_SINCE_LAST_DISCOVERY_DATE int ;
            alter table {ts_table}  add column   DISCOVERY_COUNT int ;
            alter table {ts_table}  add column   ZONE varchar default 'Cisco Only';
            alter table {ts_table}  add column   ZONE_DURATION varchar ;
            alter table {ts_table}  add column   HOST_NAME varchar ;




            update {ts_table}  t
            set t.FIRST_DISCOVERY_DATE=d.first_discovery_date,
            t.LAST_DISCOVERY_DATE=d.last_discovery_date,
            t.DAYS_SINCE_LAST_DISCOVERY_DATE=d.DAYS_SINCE_LAST_DISCOVERY_DATE,
            t.DISCOVERY_COUNT=d.discovery_count,
            t.ZONE=
                case
                    when d.sources like '%collector%' and  d.sources like '%customer%' then 'Cisco + Collector + Customer'
                    when d.sources like '%collector%' then 'Cisco + Collector'
                    when  d.sources like '%customer%' then 'Cisco + Customer'
                    else  'Cisco Only' end,
            t.ZONE_DURATION=d.zone_duration,
            t.HOST_NAME=d.host_name
            from
            (
            with e as (select z.*
            from {schema}.DC_EVIDENCE_ZONES z
            where z.DC_ENGAGEMENT_ID = {eid}
            ),
            last_with_HN as (
            WITH OBS AS
            (
            select e.DC_ENGAGEMENT_ID, d.INSTANCE_ID, max(h.EFFECTIVE_DATE) last_hn_date
            from e join {schema}.DC_EVIDENCE_COLLECTOR_HDR h
            on (e.DC_ENGAGEMENT_ID = h.DC_ENGAGEMENT_ID)
            join {schema}.DC_EVIDENCE_COLLECTOR_DETAILS d
            on (d.request_id = h.request_id and d.INSTANCE_ID = e.INSTANCE_ID)
            where d.HOST_NAME is not null and len(d.HOST_NAME) > 0
            and e.DC_ENGAGEMENT_ID = {eid}
            group by e.DC_ENGAGEMENT_ID, d.INSTANCE_ID
            )
            select LN.DC_ENGAGEMENT_ID, d.INSTANCE_ID ,max(d.HOST_NAME) as HOST_NAME 
            from {schema}.DC_EVIDENCE_COLLECTOR_HDR h
            join {schema}.DC_EVIDENCE_COLLECTOR_DETAILS d  on (d.request_id = h.request_id and H.DC_ENGAGEMENT_ID = {eid} )
            join OBS LN on (d.INSTANCE_ID = LN.INSTANCE_ID and H.DC_ENGAGEMENT_ID =LN.DC_ENGAGEMENT_ID and LN.last_hn_date=H.EFFECTIVE_DATE )
            group by LN.DC_ENGAGEMENT_ID, d.INSTANCE_ID
            )
            select e.* , HN.HOST_NAME  from e left join last_with_HN HN on ( HN.INSTANCE_ID=E.INSTANCE_ID AND HN.DC_ENGAGEMENT_ID=E.DC_ENGAGEMENT_ID)) d
            where d.instance_id =t.instance_id and d.DC_ENGAGEMENT_ID= {eid};


              """

    engine = create_engine(sec.get_sf_pw(check_env("prod"), "CPS_DSCI_ETL_EXT3_WH", schema))
    con = engine.connect()
    for s in sql.split(";"):
        print(s)
        res = con.execute(s)
    log_to_dc_job_messages(env, cid, f"SUCCESS: Step 9/12 Completed Canvas Enrichments.", requested_by, notification_id)
    # con.close()
    return True


def delete_json_from_s3(bucket, key):
    key = f"{key}.json"
    session = boto3.Session(aws_access_key_id=sec.ACCESS_KEY, aws_secret_access_key=sec.SECRET_KEY)

    s3 = session.resource("s3")
    obj = s3.Object(bucket, key)
    obj.delete()

    return True


@task(trigger=all_finished, log_stdout=True, tags=["snowflake_large"], skip_on_upstream_skip=False)
def remove_working_tables(coverage_table, scope_tbl, start_tbl, flt_table, notes_tbl, relevant_tbl, env, schema,
                          canvas_id, notification_id, requested_by, ):
    sql = f"""
        drop table IF EXISTS {scope_tbl} ;
        drop table IF EXISTS {start_tbl} ;
        drop table IF EXISTS {flt_table} ;
        drop table IF EXISTS {notes_tbl} ;
        drop table IF EXISTS {relevant_tbl} ;
        drop table IF EXISTS {coverage_table} ;
        """
    engine = create_engine(sec.get_sf_pw(check_env("prod"), "CPS_DSCI_ETL_EXT3_WH", schema))
    con = engine.connect()

    for s in sql.split(";"):
        print(s)
        try:
            res = con.execute(s)
        except Exception as e:
            print(type(e))

    con.close()

    try:
        delete_json_from_s3("canvas-lock", canvas_id)
    except:
        print("deleting canvas lock file failed")
    log_to_dc_job_messages(env, canvas_id, f"SUCCESS: Step 12/12 Removed all working tables.", requested_by,
                           notification_id)
    log_to_dc_job_messages(env, canvas_id, f"SUCCESS: Run completed for CANVAS-{canvas_id}", requested_by,
                           notification_id)
    return True


@task(log_stdout=True, tags=["snowflake_large"])
def prep_final(cid, multi_row_tbl, notes_tbl, flat_tbl, env, wh, schema, notification_id, requested_by, ):
    core_table = f"{schema}.CANVAS_{cid.split('-')[-1]}_THOUGHT_SPOT".lower()
    cid.split("-")[-1]

    # core_table = f"{schema}.{cid.replace('-', '_')}_THOUGHT_SPOT".lower()

    print(f"---------------{core_table}--------------------------")

    sql = f"""
        create or replace transient table {core_table}   as
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
                nvl(device_level_gplp_list_price,0) as device_level_gplp_list_price,
               nvl(device_level_gplp_x_quantity,0) as device_level_gplp_x_quantity,
               case when n.INSTANCE_ID is null then 'single'
                                    else 'multi_line_fix' end as modified_record,
                    TO_VARCHAR(n.notes) as note
                    from {multi_row_tbl} d
                    left join {notes_tbl}  n on (n.INSTANCE_ID=d.INSTANCE_ID)
                    left join {flat_tbl} f on (d.instance_id = f.parent_instance_id)
                    where nvl(d.ORDERV_CURRENT,1) =1"""

    engine = create_engine(sec.get_sf_pw(check_env("prod"), "CPS_DSCI_ETL_EXT3_WH", schema))
    con = engine.connect()
    print(sql)
    con.execute(sql)
    log_to_dc_job_messages(env, cid, f"SUCCESS: Step 7/12 Prepped final table.", requested_by, notification_id)
    # con.close()
    return core_table


@task(log_stdout=True, tags=["snowflake_large"])
def flatten_data(multi_row_tbl, env, wh, schema, cid, notification_id, requested_by):
    flat_tbl = f"{multi_row_tbl}_flat".upper()
    flat_sql = f"""
      create or replace Transient table {flat_tbl} as
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
           usd_extended_list_price,  -- 505
           global_product_list_price,
         global_product_list_price_x_quantity
         from {multi_row_tbl}
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
            count(distinct g.INSTALLED_AT_SITE_ID)                                                  as device_level_installed_at_site_id_total,
            sum(g.global_product_list_price) as device_level_gplp_list_price,
            sum(g.global_product_list_price_x_quantity) as device_level_gplp_x_quantity
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
    print(flat_sql)
    engine = create_engine(sec.get_sf_pw(check_env("prod"), "CPS_DSCI_ETL_EXT3_WH", schema))
    con = engine.connect()
    con.execute(flat_sql)
    log_to_dc_job_messages(env, cid, f"SUCCESS: Step 6/12 Flattened data.", requested_by, notification_id)
    # con.close()
    return flat_tbl


@task(trigger=all_successful, log_stdout=True, tags=["snowflake_large"])
def gen_current_data(scope_instance_tbl_name, env, wh, schema, coverage_table, cid, notification_id, requested_by, ):
    multi_row_tbl = f"{scope_instance_tbl_name}_multis".upper()

    core_sql = f"""
        create or replace Transient table {multi_row_tbl} as
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
             ),
             eov as (
                 select msi.segment1,-- C_EXT_ATTR4 EO_LAST_SHIP_DATE,-- C_EXT_ATTR2 END_OF_CHANGE_DATE,-- C_EXT_ATTR3 END_OF_LIFE_DATE,-- C_EXT_ATTR1 ACTIVE_FLAG,
                        -- D_EXT_ATTR1 EO_INT_ANNOUNCE_DATE,-- D_EXT_ATTR2 EO_EXT_ANNOUNCE_DATE,-- D_EXT_ATTR3 EO_SALES_DATE,-- D_EXT_ATTR4 EO_SW_AVAL_DATE,
                        -- D_EXT_ATTR5 EO_SVC_ATTACH_DATE,-- D_EXT_ATTR6 EO_SW_MAINTENANCE_DATE,
                        -- D_EXT_ATTR7 EO_TAC_SUPPORT_DATE,-- D_EXT_ATTR9 EO_CONTRACT_RENEW_DATE,
                        TO_CHAR(CPS_DSCI_ARCHIVE.FIX_DATES_W_DEF(D_EXT_ATTR8::DATE, '2150-12-31'::DATE),'yyyy-mm-dd')     as EO_SECURITY_VUL_SUPPORT_DATE,
                        rank() over ( partition by msi.segment1 order by msi.REQUEST_ID desc,msi.EDWSF_CREATE_DTM desc ) as orderv_eov
                 from EDW_SC_ETL_DB.SS.CG1_EGO_MTL_SY_ITEMS_EXT_B ego
                          join EDW_SC_ETL_DB.SS.CG1_MTL_SYSTEM_ITEMS_B msi
                               on (ego.inventory_item_ID = msi.inventory_item_ID)
                 where ego.attr_group_Id = 224 --         (SELECT attr_group_id FROM EDW_SC_ETL_DB.ss.CG1_EGO_ATTR_GROUPS_V  WHERE attr_group_name = 'END_OF_LIFE' )
                   and msi.organization_Id = 1
                   and nvl(ego.EDWSF_SOURCE_DELETED_FLAG, 'N') = 'N'
                   and nvl(msi.EDWSF_SOURCE_DELETED_FLAG, 'N') = 'N'
             ), acat_enrichments as (
             
                         with dc_needs as
                              (select ACAT_CUSTOMER_ID, l.DC_ENGAGEMENT_ID
                               from DC_ACAT_LINKS l join DC_ENGAGEMENT_HDR h on ( h.DC_ENGAGEMENT_ID=l.DC_ENGAGEMENT_ID)
                               join DC_CANVAS_HDR c on ( c.DC_ENGAGEMENT_ID=h.DC_ENGAGEMENT_ID)
                               where l.is_deleted = 'F' and h.is_deleted = 'F'
                               and c.CANVAS_ID = {cid}
                              ),
                              latest as
                                (
                                select d.REQUEST_ID, d.CUSTOMER_ID, d.LAST_UPDATE_DATE, count(0) as cnt
                                FROM   SERVICES_DB.SERVICES_IB_FBV.BV_IBSA_ACAT_DISCOVERY_SUM D
                                join SERVICES_DB.SERVICES_IB_FBV.BV_IBSA_ACAT_CUSTOMER_MASTER m on (m.CUSTOMER_ID=D.CUSTOMER_ID)
                                join dc_needs on (dc_needs.ACAT_CUSTOMER_ID=D.CUSTOMER_ID)
                                left join SERVICES_DB.SERVICES_IB_FBV.BV_IBSA_ACAT_DISCOVERY_DATA a on (d.REQUEST_ID=a.ACAT_REQUEST_ID and m.CUSTOMER_ID = SWEEPS_CUSTOMER_NUMBER)
                                where  d.TOTAL_LINES > 0
                                         and d.REQUEST_TYPE in ('ON-DEMAND', 'Discovery(System)')
                                         and d.data_purged like 'RETAIN%'
                                group by d.REQUEST_ID, d.CUSTOMER_ID, d.LAST_UPDATE_DATE
                                ),
                            ranked as
                            (
                                select REQUEST_ID,CUSTOMER_ID,LAST_UPDATE_DATE,cnt,
                                rank() over ( partition by CUSTOMER_ID order by LAST_UPDATE_DATE desc ) as orderv
                                from latest
                                where cnt > 5
                            ),picked as
                            (
                                select distinct ranked.*
                                from DC_ACAT_LINKS l
                                left join ranked on (l.ACAT_CUSTOMER_ID = ranked.CUSTOMER_ID)
                                where orderv = 1
                            )
                select max(UNCOVERED_CATEGORY) as ACAT_UNCOVERED_CATEGORY,
                       max(REASON_CODE) as ACAT_REASON_CODE,
                       max(EXCLUDE_FLAG) as ACAT_EXCLUDE_FLAG,
                       min(EARLIEST_DISCOVERY_DATE) as ACAT_EARLIEST_DISCOVERY_DATE,
                       max(picked.LAST_UPDATE_DATE) as ACAT_LAST_UPDATE_DATE,
                       instance_id
                from SERVICES_DB.SERVICES_IB_FBV.BV_IBSA_ACAT_DISCOVERY_DATA d join picked on ( picked.CUSTOMER_ID=d.SWEEPS_CUSTOMER_NUMBER and picked.REQUEST_ID=d.ACAT_REQUEST_ID)
                group by instance_id

             )
            , scope as (
                   select   instance_id,
                  listagg(distinct source, ',') within group (order by source ) as sources,
                  listagg(distinct evidence, ',') within group (order by evidence ) as evidence
                    from {scope_instance_tbl_name}
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
               eol.END_OF_CHANGE_DT::date                                                         as END_OF_CHANGE_DT,
               eol.END_OF_MANUFACTURING_DT::date                                                  as END_OF_MANUFACTURING_DT,
               eol.END_OF_NEW_SVC_ATTACHMENT_DT::date                                             as END_OF_NEW_SVC_ATTACHMENT_DT,
               eol.END_OF_SOFTWARE_MAINTENANCE_DT::date                                           as end_of_software_maintenance_date,   -- 237,582
               eol.END_OF_ROUTINE_FAIL_ANLYSYS_DT::date                                           as END_OF_ROUTINE_FAIL_ANLYSYS_DT,
               eol.END_OF_SALE_DT::date                                                           as end_of_sale_date,                   --235, 580,
               eol.EOL_SOFTWARE_AVAILABLE_DT::date                                                as EOL_SOFTWARE_AVAILABLE_DT,
               eol.EOL_SIGNATURE_RELEASE_DT::date                                                 as EOL_SIGNATURE_RELEASE_DT,
               eol.END_OF_SVC_CONTRACT_RNWL_DT::date                                              as END_OF_SVC_CONTRACT_RNWL_DT,
               eol.END_OF_TAC_ENGG_SUPPORT_DT::date                                               as end_of_tac_support_date,            --239,584,
               eol.END_OF_SFTWR_LICENSE_AVAIL_DT::date                                            as end_of_sw_license_date,             --581, 236

               CPS_DSCI_ARCHIVE.FIX_DATES(item.last_date_of_service_attach)                       as last_date_of_service_attached,      --285, 593
               CPS_DSCI_ARCHIVE.FIX_DATES(item.last_date_of_renewal)                              as last_date_of_renewal,               -- 592, 284

               item.product_list_price_gpl_us                                                     as global_product_list_price,          --255, 587
               item.product_list_price_gpl_us * ib.QUANTITY                                       as global_product_list_price_x_quantity,  
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
                   WHEN datediff(day, CURRENT_TIMESTAMP, coverage.LAST_COVERAGE_DATE) BETWEEN -365 AND -1
                       THEN 'f.Coverage Ended <1 year ago'
                   WHEN datediff(day, CURRENT_TIMESTAMP, coverage.LAST_COVERAGE_DATE) BETWEEN -730 AND -366
                       THEN 'e.Coverage Ended <2 years ago'
                   WHEN datediff(day, CURRENT_TIMESTAMP, coverage.LAST_COVERAGE_DATE) BETWEEN -1095 AND -731
                       THEN 'd.Coverage Ended <3 years ago'
                   WHEN datediff(day, CURRENT_TIMESTAMP, coverage.LAST_COVERAGE_DATE) BETWEEN -1460 AND -1096
                       then 'c.Coverage Ended <4 years ago'
                   WHEN datediff(day, CURRENT_TIMESTAMP, coverage.LAST_COVERAGE_DATE) BETWEEN -1825 AND -1461
                       THEN 'b.Coverage Ended <5 years ago'
                   WHEN datediff(day, CURRENT_TIMESTAMP, coverage.LAST_COVERAGE_DATE) <= -1826
                        THEN 'a.Coverage Ended >5 years ago'
                   else 'g.Never Covered' end  AS COVERAGE_END_ANNUAL_DURATION,

               coverage.DUPLICATE_COVERAGE_FLAG                                                   as cvd_DUPLICATE_COVERAGE_FLAG,
               DUPLICATE_CVG_REF_LINE_ID,
               scope.sources,
               scope.evidence,
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
               coverage.last_coverage_fiscal_quarter,
               eov.EO_SECURITY_VUL_SUPPORT_DATE::date as EO_SECURITY_VUL_SUPPORT_DATE,
              sav.SAV_REFERENCE_VERSION,
              sav.level_1,
              sav.level_2,
              sav.level_3,
              sav.level_4,
              sav.level_5,
              sav.level_6,
              sav.sav_name,
              sav.sav_id,
            isite.cav_id,
            isite.cav_name,
            isite.cav_bu_id,
            isite.cav_bu_name,
            nvl(cp.is_significant,'YES')                   as contract_simplification_is_significant,
            coverage.signed_mso,
            coverage.signed_bill_to,
            coverage.flat_coverage_status,
            coverage.responsible_users ,
            coverage.monitor_reason ,
            coverage.contract_name ,
            acat_enrichments.ACAT_UNCOVERED_CATEGORY,
            acat_enrichments.ACAT_REASON_CODE,
            acat_enrichments.ACAT_EXCLUDE_FLAG,
            acat_enrichments.ACAT_EARLIEST_DISCOVERY_DATE,
            acat_enrichments.ACAT_LAST_UPDATE_DATE
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
            left join CPS_DSCI_BR.party_to_sav_level sav on (sav.party_id = isite.CR_PARTY_ID)
            left join CPS_DSCI_ARCHIVE.CORRECTED_PIDS cp on (ib.ITEM_NAME = cp.ITEM_NAME)
            left join acat_enrichments on ( acat_enrichments.INSTANCE_ID= scope.INSTANCE_ID )
            left join  {coverage_table} coverage on (scope.INSTANCE_ID = coverage.INSTANCE_ID)

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
                 left join eov on (eol.BK_PRODUCT_ID = eov.segment1 and eov.orderv_eov = 1)
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
    engine = create_engine(sec.get_sf_pw(check_env("prod"), "CPS_DSCI_ETL_EXT3_WH", schema))
    con = engine.connect()
    print("alter session set  STATEMENT_TIMEOUT_IN_SECONDS = 10800")
    print(core_sql)
    con.execute("alter session set  STATEMENT_TIMEOUT_IN_SECONDS = 10800")
    con.execute(core_sql)
    log_to_dc_job_messages(env, cid, f"SUCCESS: Step 4/12 Retrieved current data.", requested_by, notification_id)
    # con.close()
    return multi_row_tbl


@task(trigger=all_successful, log_stdout=True, tags=["snowflake_large"])
def gen_coverage_data(scope_instance_tbl_name, cid, env, wh, schema, notification_id, requested_by, ):
    coverage_table = f"{schema}.CANVAS_COVERAGE_{cid.split('-')[-1]}_THOUGHT_SPOT".lower()

    core_sql = f"""
    create or replace transient table {coverage_table} as

    with scope as (select   distinct instance_id from {scope_instance_tbl_name}
    ),coverage as(
                select PARENT_INSTANCE_ID, COVERED_LINE_ID, cvd_line.INSTANCE_ID, cvd_line.CONTRACT_ID, cvd_line.SERVICE_LINE_ID, STS_CODE, START_DATE, END_DATE, CLE_ID_RENEWED, CLE_ID_RENEWED_TO,
                DNR_FLAG, PRICE_NEGOTIATED, PRICE_UNIT, MAINTENANCE_PO_NUMBER, MAINTENANCE_SO_NUMBER, DATE_TERMINATED,
                LINE_NUMBER, LINE_CREATION_DATE, LINE_LAST_UPDATE_DATE, LINE_CREATED_BY, LINE_LAST_UPDATED_BY, cvd_line.CURRENCY_CODE, DATE_RENEWED,
                cvd_line.CVD_ATTRIBUTE14, cvd_line.CVD_ATTRIBUTE15, cvd_line.DUPLICATE_COVERAGE_FLAG, DUPLICATE_CVG_REF_LINE_ID, USD_PRICE_NEGOTIATED, USD_PRICE_UNIT,
                cvd_line.USD_CONVERSION_RATE, COVERED_LINE_MOVED_FROM, COVERED_LINE_MOVED_TO, cvd_line.SAVA, MAPPED_SKU, OFFER_TYPE, ACTUAL_PRICE_NEGOTIATED,
                 QUOTE_NUMBER, OFFER_ATO_SUITE_NAME,   TERMINATION_CREDIT, last_coverage_date ,
                case
                when current_date between cvd_line.START_DATE and DATEADD(DAY, 30,least(END_DATE, nvl(DATE_TERMINATED,END_DATE)))::date then 'L'
                when current_date < DATEADD(DAY, 30,least(END_DATE, nvl(DATE_TERMINATED,END_DATE)))::date then 'Z'
                when current_date > DATEADD(DAY, 30,least(END_DATE, nvl(DATE_TERMINATED,END_DATE)))::date then 'P'
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
                from   {scope_instance_tbl_name} scope join  CPS_DSCI_BR.CAM_DS_INSTANCE_DETAIL  ib  on (ib.INSTANCE_ID=scope.INSTANCE_ID)
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
                from   {scope_instance_tbl_name} scope join CPS_DSCI_BR.CAM_DS_INSTANCE_DETAIL ib  on (ib.INSTANCE_ID=scope.INSTANCE_ID)
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
    engine = create_engine(sec.get_sf_pw(check_env("prod"), "CPS_DSCI_ETL_EXT3_WH", schema))
    con = engine.connect()
    print("alter session set  STATEMENT_TIMEOUT_IN_SECONDS = 10800")
    print(core_sql)
    con.execute("alter session set  STATEMENT_TIMEOUT_IN_SECONDS = 10800")
    con.execute(core_sql)
    # con.close()
    log_to_dc_job_messages(env, cid, f"SUCCESS: Step 2/12 Generated coverage data.", requested_by, notification_id)
    return coverage_table


@task(log_stdout=True, tags=["snowflake_large"])
def gen_notes(multi_row_tbl, env, wh, schema, cid, notification_id, requested_by, ):
    notes_tbl = f"{multi_row_tbl}_notes".upper()
    notes_sql = f"""
    create or replace Transient table {notes_tbl} as
    with flat as (
          select INSTANCE_ID,
               PARENT_INSTANCE_ID,
                array_agg(DISTINCT service_level) OVER ( PARTITION BY PARENT_INSTANCE_ID) as list_of_service_levels,
                array_agg(DISTINCT coverage_line_id::bigint ) OVER ( PARTITION BY PARENT_INSTANCE_ID) as list_of_covered_lines,
                array_agg(DISTINCT contract_number  ) OVER ( PARTITION BY PARENT_INSTANCE_ID ) as list_of_contracts,
                row_number() over ( partition by  f.INSTANCE_ID order by f.coverage_line_id  desc) as row_num_cli
                from {multi_row_tbl} f
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
    print(notes_sql)
    engine = create_engine(sec.get_sf_pw(check_env("prod"), "CPS_DSCI_ETL_EXT3_WH", schema))
    con = engine.connect()
    con.execute(notes_sql)
    log_to_dc_job_messages(env, cid, f"SUCCESS: Step 5/12 Generated notes.", requested_by, notification_id)
    # con.close()
    return notes_tbl


def build_params_dict(canvas_id, files, engagement_id, table_schema, des_tbl, env):
    input_parameters = {
        "canvas_id": canvas_id,
        "destination_table": des_tbl,
        "engagement_id": engagement_id,
        "env": env,
        "files": files,
        "schema": table_schema,
    }
    print(json.dumps(input_parameters))
    return json.dumps(input_parameters)


@task(log_stdout=True)
def clean_working_space(working_space: str):
    # sort fo safe must start with /tmp
    # horrid  code
    if working_space.startswith(temp_base_location):
        shutil.rmtree(working_space, ignore_errors=False, onerror=None)


@task(trigger=all_finished, log_stdout=True, nout=2, tags=["snowflake_large"])
def log_run_to_table(canvas_id, files, engagement_id, table_schema, des_tbl, env, notification_id, requested_by, ):
    engine = create_engine(sec.get_sf_pw(check_env("prod"), "CPS_DSCI_ETL_EXT3_WH", table_schema))
    run_date = datetime.now()

    input_parameters = {
        "canvas_id": canvas_id,
        "engagement_id": engagement_id,
        "files": files,
        "env": env,
        "type": "current view",
    }

    log_df = pd.DataFrame(index=[0], columns=["CANVAS_ID", "INPUT_PARAMETERS", "PROCESSING", "RUN_DATE"])
    log_df["CANVAS_ID"] = canvas_id
    log_df["INPUT_PARAMETERS"] = f"{json.dumps(input_parameters)}"
    log_df["PROCESSING"] = "P"
    log_df["RUN_DATE"] = run_date

    log_df.to_sql("dc_canvas_create_run_log".lower(), engine, index=False, if_exists="append", chunksize=10)
    return input_parameters, run_date


from os import listdir
from os.path import isfile, join


def prep_data_location(this_path, clear_contents=True):
    this_path = pathlib.Path(str(this_path))
    if this_path.exists() and this_path.is_dir():
        if clear_contents:
            shutil.rmtree(this_path)
            pathlib.Path(this_path).mkdir(parents=True, exist_ok=True)
    else:
        pathlib.Path(this_path).mkdir(parents=True, exist_ok=True)
    return this_path


@task(trigger=all_finished, log_stdout=True, tags=["snowflake_large"], skip_on_upstream_skip=False)
def write_metadata_table(canvas_id, engagement_id, env, schema, run_date, s3_parq_loc, notification_id, requested_by, ):
    engine = create_engine(sec.get_sf_pw(check_env("prod"), "CPS_DSCI_ETL_EXT3_WH", schema))
    con = engine.connect()

    json_loc = "s3://messaging.stage.cisco.com/start-canvas-creation/"
    print("####################")
    print(s3_parq_loc)
    date_created = datetime.now().date().isoformat()

    # global var bad
    print(
        f"-------------------------------------------ERR MESSAGES: {failure_notes}-------------------------------------------"
    )
    if len(failure_notes) > 0:
        status = f"failed : {failure_notes[0]}"
    else:
        status = f"success"

    print(f"status = {status}")

    update_metadata_query = f"""
    UPDATE CPS_DB.{schema}.DC_CANVAS_HDR set CANVAS_STATUS = '{status}',
                                                FILE_PATH ='{s3_parq_loc}',
                                                CREATE_DTM = current_date()
    where DC_ENGAGEMENT_ID = {int(engagement_id)} and CANVAS_ID ='{canvas_id}';
    """

    print(update_metadata_query)
    try:
        con.execute(update_metadata_query)
    except Exception as e:
        print(e)
        print("Data for this canvas_id is not in CPS_DB.CPS_BIA_BR.DATA_CANVAS_HDR")
        pass

    update_canvas_processing_log_query = f"""
     UPDATE CPS_DB.{schema}.dc_canvas_create_run_log set PROCESSING = 'complete'
     where CANVAS_ID = '{canvas_id}' and RUN_DATE = '{run_date}';
     """

    print(update_canvas_processing_log_query)
    try:
        con.execute(update_canvas_processing_log_query)
    except Exception as e:
        print(e)
        print(f"Data for this canvas_id is not in CPS_DB.{schema}ARCHIVE.CANVAS_CREATE_RUN_LOG")
        pass
    return status


@task
def constuct_message_file_key(canvas_id: str):
    return f"canvas-processing-status/canvas-{canvas_id}-{datetime.now().isoformat()}.json"


@task(log_stdout=True, nout=9, tags=["snowflake_large"], state_handlers=[log_msg])
def prep_vars(scopes, eid, date, env, cid, source_data_date_filter, tag_ids=[], collector_files_ids=[],
              customer_files_ids=[]):
    print(scopes)
    print(env)
    print(tag_ids)

    correct_schema = get_correct_schema.run(env)
    sf_env = check_env("prod")
    engine = create_engine(sec.get_sf_pw(sf_env, warehouseMed, correct_schema))

    requested_by_query = f"select created_by from {correct_schema}.DC_CANVAS_HDR where canvas_id = {cid}"
    print(requested_by_query)

    requested_by_df = pd.read_sql(requested_by_query, engine)

    requested_by = str(requested_by_df.created_by[0])

    warehouse = "CPS_DSCI_ETL_EXT3_WH"
    engine = create_engine(sec.get_sf_pw(check_env("prod"), warehouse, correct_schema))
    directives = []
    for d in scopes:
        directives.append(d["name"].upper())

    print(f"scope set with these directives: {directives}")

    date = date[0: min(10, len(date))]
    run_date_mod = date.replace("-", "_")

    # fix and format the date filter including default val
    formatted_source_data_date_filter = pd.to_datetime(
        source_data_date_filter, errors="coerce", infer_datetime_format=True
    )
    if pd.isnull(formatted_source_data_date_filter):
        sql_filter_date = "2020-06-01"
    else:
        sql_filter_date = formatted_source_data_date_filter.strftime("%Y-%m-%d")

    aws_canvas_out_pth = f"s3://canvas-data-store-{env}/CANVAS_FILES/{run_date_mod}/{cid}"

    scope_sql = []

    def gen_temp_name(cid, correct_schema, requested_by):
        letters = string.ascii_letters
        random_string = "{}".format("".join(random.choice(letters) for i in range(10)))
        try:
            RenameFlowRun().run(flow_run_name=f"""{cid}-{requested_by}-{random_string}""")
        except Exception:
            pass

        return (
            f"{correct_schema}.canvas_{random_string}_{cid.replace('-', '_')}".lower(),
            f"{correct_schema}.canvas_{random_string}_relevant_{cid.replace('-', '_')}".lower(),
        )

    scope_instance_tbl_name, relevant_tbl_name = gen_temp_name(cid, correct_schema, requested_by)

    relevant_sql = f"""
            create or replace transient table {relevant_tbl_name} as 
            with sub as (
                select DC_ENGAGEMENT_ID from {correct_schema}.dc_ENGAGEMENT_HDR h where DC_ENGAGEMENT_ID = {eid}
            )
                      select e.ACAT_CUSTOMER_ID      as this_value,  'ACATACCOUNTID'   as src  from  {correct_schema}.dc_ACAT_LINKS e  join sub on (e.DC_ENGAGEMENT_ID =sub.DC_ENGAGEMENT_ID ) where e.IS_DELETED ='F'
                     union
                      select e.MCE_ENGAGEMENT_NUMBER as this_value,  'MCEENGAGEMENTID' as src from  {correct_schema}.dc_MCE_LINKS e   join sub on (e.DC_ENGAGEMENT_ID =sub.DC_ENGAGEMENT_ID ) where e.IS_DELETED ='F'
                     union
                      select e.SMART_ACCOUNT         as this_value,  'SMARTACCOUNTID'  as src from  {correct_schema}.dc_SMART_ACCOUNT_LINKS e  join sub on (e.DC_ENGAGEMENT_ID =sub.DC_ENGAGEMENT_ID ) where e.IS_DELETED ='F'
                     union
                      select e.CR_PARTY_ID           as this_value,  'GUID'            as src from  {correct_schema}.dc_PARTY_LINKS e  join sub on (e.DC_ENGAGEMENT_ID =sub.DC_ENGAGEMENT_ID ) where e.IS_DELETED ='F' 

                """

    scope_sql.append([relevant_sql, "relevant_sql"])

    ddl = f"""create or replace Transient table {scope_instance_tbl_name}
                (
                INSTANCE_ID BIGINT,
                SOURCE      VARCHAR(30),
                EVIDENCE      VARCHAR(50000)
                )"""
    scope_sql.append([ddl, "ddl"])

    for sc in directives:  # set scope sql, dest table, and to get tags per option
        print(sc)

        if sc == "BASELINE_TAGS" or sc == "Baseline Tags":

            if len(tag_ids) == 0:
                tagin = '1746'
            else:
                tagin = ','.join(map(str, tag_ids))

            scope_sql.append(
                [
                    f"""
                    insert into {scope_instance_tbl_name}(INSTANCE_ID, SOURCE, EVIDENCE)
                    select INSTANCE_ID, 'BASELINE', NULL from {correct_schema}.DC_ENGAGEMENT_TAGS_{eid}
                    where TAG_ID in ({tagin}) and is_deleted = 'F' """,
                    "BASELINE_TAGS",
                ]
            )

        if sc == "SMART_ACCOUNT" or sc == "Smart Account":
            scope_sql.append(
                [
                    f"""
                    insert into  {scope_instance_tbl_name} (INSTANCE_ID,SOURCE,EVIDENCE)
                    select distinct INSTANCE_ID , 'SMART_ACCOUNT' as source, 'CURRENT DEFINITION' as EVIDENCE 
                    from CPS_DSCI_ARCHIVE.smart_account_to_instance_id 
                        where smart_account in (
                        select SMART_ACCOUNT::varchar as SMART_ACCOUNT from {correct_schema}.DC_SMART_ACCOUNT_LINKS 
                        where DC_ENGAGEMENT_ID = {eid}
                        )
                    """,
                    "SMART_ACCOUNT",
                ]
            )

        if sc == "MCE":
            scope_sql.append(
                [
                    f"""
                    insert into  {scope_instance_tbl_name} (INSTANCE_ID,SOURCE,EVIDENCE)

                    WITH MCE_EV AS (


                 select distinct d.INSTANCE_ID , h.ENGAGEMENT_NUMBER, CURRENT_DATE() as DATE_SOURCED
                        from SERVICES_DB.SERVICES_ENT_FBV.BV_MCE_AM_ENGAGEMENT_DATA d
                        join SERVICES_DB.SERVICES_ENT_FBV.BV_MCE_AM_ENGAGEMENT_HDR h  on (h.ENGAGEMENT_ID=d.ENGAGEMENT_ID)
                        join {relevant_tbl_name} relevant on (relevant.this_value=  h.ENGAGEMENT_NUMBER )
                        where relevant.src ='MCEENGAGEMENTID' and h.LAST_UPDATED_DATE > '{sql_filter_date}'::date
                    union
                 select distinct E.INSTANCE_ID , E.ENGAGEMENT_NUMBER, E.DATE_SOURCED
                    FROM CPS_DSCI_ARCHIVE.MCE_EVIDENCE E
                    join {relevant_tbl_name} relevant on (relevant.this_value= E.ENGAGEMENT_NUMBER )
                    where relevant.src= 'MCEENGAGEMENTID' and E.DATE_SOURCED > '{sql_filter_date}'::date 
                 )
                    select distinct INSTANCE_ID , 'MCE' as source,
                    LISTAGG( distinct CONCAT('MCE: ', ENGAGEMENT_NUMBER, '->', DATE_SOURCED::DATE), '|')   over (partition  BY INSTANCE_ID) as EVIDENCE
                    FROM MCE_EV

                    """,
                    "MCE",
                ]
            )

        if sc == "CONTRACTS" or sc == "Managed Contracts":
            scope_sql.append(
                [
                    f"""
                insert into  {scope_instance_tbl_name}(INSTANCE_ID,SOURCE,EVIDENCE)
                    with flattened_contracts
                     as (
                        select distinct {eid}::int as ENGAGEMENT_ID  , mc.CONTRACT_NUMBER
                        from {correct_schema}.dc_BOOKINGS_CONTRACTS c
                        join {correct_schema}.dc_BOOKINGS_CONTRACTS_RESPONSIBLE_USERS r on ( r.BOOKING_CONTRACT=c.BOOKING_CONTRACT)
                        join {correct_schema}.dc_ENGAGEMENT_TO_BOOKINGS_RESPONSIBLE_USER eu on ( eu.BOOKING_CONTRACT=r.BOOKING_CONTRACT and eu.DC_USER_ID=r.DC_USER_ID )
                        join {correct_schema}.dc_managed_service_contracts mc on ( mc.DC_USER_ID=eu.DC_USER_ID and mc.BOOKING_CONTRACT=eu.BOOKING_CONTRACT and mc.DC_ENGAGEMENT_ID = eu.DC_ENGAGEMENT_ID)
                        where eu.DC_ENGAGEMENT_ID = {eid}::int and c.IS_DELETED = 'F' and  r.IS_DELETED = 'F'   and  eu.IS_DELETED = 'F' and   mc.IS_DELETED = 'F'
                        union
                        select  distinct {eid}::int as ENGAGEMENT_ID  , monc.CONTRACT_NUMBER
                        from {correct_schema}.dc_BOOKINGS_CONTRACTS c
                        join {correct_schema}.dc_BOOKINGS_CONTRACTS_RESPONSIBLE_USERS r on ( r.BOOKING_CONTRACT=c.BOOKING_CONTRACT)
                        join {correct_schema}.dc_ENGAGEMENT_TO_BOOKINGS_RESPONSIBLE_USER eu on ( eu.BOOKING_CONTRACT=r.BOOKING_CONTRACT and eu.DC_USER_ID=r.DC_USER_ID )
                        join {correct_schema}.DC_MONITOR_SERVICE_CONTRACTS monc on ( monc.DC_ENGAGEMENT_ID= eu.DC_ENGAGEMENT_ID)
                        where eu.DC_ENGAGEMENT_ID = {eid}::int and c.IS_DELETED = 'F' and  r.IS_DELETED = 'F'   and  eu.IS_DELETED = 'F' and   monc.IS_DELETED = 'F'
                        ), dets as (-- per instance with flat tags
                                select cvd.INSTANCE_ID, 'CONTRACTS' as src, cvd.covered_line_id
                                from flattened_contracts c
                                join EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_SAHDR_CORE hdr on ( c.CONTRACT_NUMBER::varchar = hdr.CONTRACT_NUMBER)
                                join CPS_DSCI_BR.CAM_DS_CVDPRDLINE_DETAIL cvd on (hdr.CONTRACT_ID = cvd.CONTRACT_ID)
                                where  NVL(cvd.EDWSF_SOURCE_DELETED_FLAG, 'N') = 'N'
                                   UNION
                                select  cvdh.INSTANCE_ID, 'CONTRACTS' as src,  cvdh.covered_line_id
                                from flattened_contracts c join EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_SAHDR_CORE hdrh  on( c.CONTRACT_NUMBER::varchar = hdrh.CONTRACT_NUMBER)
                                join CPS_DSCI_BR.CAM_DS_CVDPRDLINE_DETAIL_H   cvdh on(hdrh.CONTRACT_ID = cvdh.CONTRACT_ID)
                                where NVL(cvdh.EDWSF_SOURCE_DELETED_FLAG, 'N') = 'N'
                                )
                                select INSTANCE_ID, src,
                                LISTAGG( distinct CONCAT('clid: ', covered_line_id), '|')   over (partition  BY INSTANCE_ID) as EVIDENCE
                                from dets                                
                           """,
                    "CONTRACTS",
                ]
            )

        if sc == "ACAT":
            scope_sql.append(
                [
                    f"""
                insert into {scope_instance_tbl_name}(INSTANCE_ID,SOURCE,EVIDENCE)
                 with dc_needs as
                              (select ACAT_CUSTOMER_ID, l.DC_ENGAGEMENT_ID
                               from DC_ACAT_LINKS l join DC_ENGAGEMENT_HDR h on ( h.DC_ENGAGEMENT_ID=l.DC_ENGAGEMENT_ID)
                               where l.is_deleted = 'F' and h.is_deleted = 'F'
                               and h.DC_ENGAGEMENT_ID = {eid}
                              ),
                              latest as
                                (
                                select d.REQUEST_ID, d.CUSTOMER_ID, d.LAST_UPDATE_DATE, count(0) as cnt
                                FROM   SERVICES_DB.SERVICES_IB_FBV.BV_IBSA_ACAT_DISCOVERY_SUM D
                                join SERVICES_DB.SERVICES_IB_FBV.BV_IBSA_ACAT_CUSTOMER_MASTER m on (m.CUSTOMER_ID=D.CUSTOMER_ID)
                                join dc_needs on (dc_needs.ACAT_CUSTOMER_ID=D.CUSTOMER_ID)
                                left join SERVICES_DB.SERVICES_IB_FBV.BV_IBSA_ACAT_DISCOVERY_DATA a on (d.REQUEST_ID=a.ACAT_REQUEST_ID and m.CUSTOMER_ID = SWEEPS_CUSTOMER_NUMBER)
                                where  d.TOTAL_LINES > 0
                                         and d.REQUEST_TYPE in ('ON-DEMAND', 'Discovery(System)')
                                         and d.data_purged like 'RETAIN%'
                                group by d.REQUEST_ID, d.CUSTOMER_ID, d.LAST_UPDATE_DATE
                                ),
                            ranked as
                            (
                                select REQUEST_ID,CUSTOMER_ID,LAST_UPDATE_DATE,cnt,
                                rank() over ( partition by CUSTOMER_ID order by LAST_UPDATE_DATE desc ) as orderv
                                from latest
                                where cnt > 5
                            ),picked as
                            (
                                select distinct ranked.*
                                from DC_ACAT_LINKS l
                                left join ranked on (l.ACAT_CUSTOMER_ID = ranked.CUSTOMER_ID)
                                where orderv = 1
                            )
                 select distinct instance_id , 'ACAT' as source,'last_available' as EVIDENCE
                from SERVICES_DB.SERVICES_IB_FBV.BV_IBSA_ACAT_DISCOVERY_DATA d join picked on ( picked.CUSTOMER_ID=d.SWEEPS_CUSTOMER_NUMBER and picked.REQUEST_ID=d.ACAT_REQUEST_ID)


                """,
                    "ACAT",
                ]
            )

        if sc == "PARTY":
            parties_sql = f"""
                with sub as (
                    select DC_ENGAGEMENT_ID from {correct_schema}.dc_ENGAGEMENT_HDR h where DC_ENGAGEMENT_ID = {eid}
                ) select e.CR_PARTY_ID           as GUID from  {correct_schema}.dc_PARTY_LINKS e  join sub on (e.DC_ENGAGEMENT_ID =sub.DC_ENGAGEMENT_ID ) where e.IS_DELETED ='F'

            """
            parties = pd.read_sql(parties_sql, engine)
            hier_p = []
            for i, row in parties.iterrows():
                hier_p.append(f"rev_hier like '%.{row.guid}.%'")

            if parties.shape[0] > 0:
                where_clause = " or ".join(hier_p)

                scope_sql.append(
                    [
                        f"""
                                    insert into {scope_instance_tbl_name} (INSTANCE_ID,SOURCE )
                                    with splits as (
                                    select d.HIERARCHY, d.party_id as pid, index, value as party_id
                                    from EDW_MASTER_ETL_DB.SS.CRT_XXNGCR_DNM_RELATIONSHIPS d,
                                         LATERAL split_to_table(HIERARCHY, '-') a
                                    where global_ultimate_id in
                                        (
                                        select distinct global_ultimate_id from
                                        EDW_MASTER_ETL_DB.SS.CRT_XXNGCR_DNM_RELATIONSHIPS d
                                        where party_id in (
                                          select this_value as guid 
                                          from {relevant_tbl_name} relevant  
                                          where relevant.src= 'GUID')
                                        )
                                ), almost_string as (  --properly formatted stting
                                    select HIERARCHY,pid, listagg(party_id, '.')   within group (order by index desc) as hier_string
                                    from splits
                                    group by pid, HIERARCHY
                                ), hier as (
                                 select pid, HIERARCHY, concat('.',hier_string,'.') as rev_hier from almost_string
                                 ), parties_in as (
                                 select * from hier 
                                 where  {where_clause}
                                ), install_sites as
                                (
                                    select SITE_USE_ID
                                    from EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_SITE_GU_DENORM g join parties_in on (g.CR_PARTY_ID  = parties_in.pid)
                                )
                                select  distinct ib.instance_id,  'PARTY_ID' as source from 
                                CPS_DSCI_BR.CAM_DS_INSTANCE_DETAIL ib
                                join install_sites i on (i.SITE_USE_ID=ib.install_at_site_use_id )
                                    """,
                        "PARTY",
                    ]
                )
    ############################end of directives loop ##################################################################

    if len(collector_files_ids) > 0:
        scope_sql.append(
            [
                f"""
                            insert into  {scope_instance_tbl_name} (INSTANCE_ID,SOURCE,EVIDENCE)
                            select distinct INSTANCE_ID , 'COLLECTOR'  as source,   LISTAGG(distinct REQUEST_ID,',') as EVIDENCE
                             from  {correct_schema}.DC_EVIDENCE_COLLECTOR_DETAILS where REQUEST_ID in ({','.join(map(str, collector_files_ids))})
                             group by INSTANCE_ID,'COLLECTOR'
                            """,
                "COLLECTOR",
            ]
        )
    if len(customer_files_ids) > 0:
        scope_sql.append(
            [
                f"""
                            insert into  {scope_instance_tbl_name} (INSTANCE_ID,SOURCE,EVIDENCE)
                            select distinct INSTANCE_ID , 'CUSTOMER'  as source,  LISTAGG(distinct REQUEST_ID,',')as EVIDENCE from
                            {correct_schema}.DC_EVIDENCE_CUSTOMER_DETAILS where REQUEST_ID in({','.join(map(str, customer_files_ids))})
                            group by INSTANCE_ID,'CUSTOMER'
                            """,
                "CUSTOMER",
            ]
        )

    return (
        scope_instance_tbl_name,
        aws_canvas_out_pth,
        date,
        run_date_mod,
        scope_sql,
        directives,
        relevant_tbl_name,
        requested_by,
        correct_schema,
    )


@task(log_stdout=True, tags=["snowflake_large"], state_handlers=[log_msg])
def gen_scope_sql(scope_sql, env, tmp_schema, scope_tbl, request_id, notification_id, requested_by, ):
    warehouse = "CPS_DSCI_ETL_EXT3_WH"

    engine = create_engine(sec.get_sf_pw(check_env("prod"), warehouse, tmp_schema))
    con = engine.connect()
    print("Scope SQL", scope_sql)
    for s in scope_sql:
        print(s[0])
        df = pd.DataFrame(con.execute(s[0]).fetchall())
        print(df.shape)
        if df.iloc[0, 0] == 0:  # need to use iloc due to the column name changing
            print(f"USER_ERROR: Step 1/12 0 lines exsist for {s[1]}")
            log_to_dc_job_messages(env, request_id, f"USER_ERROR: Step 1/12 0 lines exsist for {s[1]}", requested_by,
                                   notification_id)

    df = pd.DataFrame(con.execute(f"select count(0) as total_scope from {scope_tbl}").fetchall())
    # con.close()
    print(f"select count(0) as total_scope from {scope_tbl}")
    total_scope_rows =df.iat[0, 0]

    if total_scope_rows < 1:
        log_to_dc_job_messages(env, request_id, f"USER_ERROR: No Scope Instances exsist for this canvas request...",
                               requested_by, notification_id)
        raise signals.SKIP("No Scope Instances..")
    log_to_dc_job_messages(env, request_id, f"SUCCESS: Step 1/12 Loaded a total scope of {total_scope_rows}",
                           requested_by, notification_id)

    return total_scope_rows


def get_sec_dir(pth):
    return os.path.join(os.getcwd(), pth)


@task(log_stdout=True, nout=2)
def convert_to_string(eid, cid):
    eid = str(eid)
    # cid = f"CANVAS-{str(cid)}"
    cid = str(cid)
    return eid, cid


@task(log_stdout=True)
def get_correct_schema(env):
    if env == "prod":
        return "CPS_DSCI_API"
    else:
        return "CPS_DSCI_BR"


@task(log_stdout=True)
def add_to_gu_log_table(sf_env, request_id, status, qry_type, dc_engagement_id, requestedBy, notification_id, ):
    cn = check_env("prod")
    correct_schema = get_correct_schema.run(sf_env)

    engine = create_engine(sec.get_sf_pw(cn, "CPS_DSCI_ETL_EXT3_WH", correct_schema))
    if isinstance(requestedBy, list):
        requestedBy = requestedBy[0]
    con = engine.connect()
    date_created = datetime.now().date().isoformat()
    if qry_type == "insert":
        qry = f"""
        insert into {correct_schema}.dc_generic_upload(dc_engagement_id,
                                                        request_id,
                                                        file_location,
                                                        status,
                                                        output_file_path,
                                                        generic_template_name, 
                                                        CREATED_BY,
                                                        CREATE_DTM,
                                                        is_deleted
                                                        ) values ({dc_engagement_id},
                                                                    {request_id},
                                                                    's3://Logs for your Canvas Creation {request_id}',
                                                                    '{status}',
                                                                    's3://Click the magnifying glass to retreive your logs ->>>> ',
                                                                    'Click the magnifying glass...',
                                                                    '{requestedBy}',
                                                                    '{date_created}',
                                                                    'F'
                                                                    )
        """
    elif qry_type == "update":
        qry = f"""
        UPDATE CPS_DB.{correct_schema}.dc_generic_upload set STATUS = '{status}'         
        where REQUEST_ID  = '{int(request_id)}' ;
        """

    con.execute(qry)


@task(log_stdout=True)
def update_config(notification_id, requestd_by):
    if notification_id != 0:
        # prefect.context(notification_id=notification_id)
        # prefect.context(requestd_by=requestd_by)
        config.notification_id = notification_id
        config.requested_by = requestd_by

    return True



@task(log_stdout=True)
def check_if_needs_ts_refresh(correct_schema, original_canvas_run_dtm, eng_id):
    original_canvas_run_dtm = str(original_canvas_run_dtm)

    engine = create_engine(
        sec.get_sf_pw('prd_cps_dsci_etl_svc', 'CPS_DSCI_ETL_EXT1_WH', 'prd_cps_dsci_etl_svc')
    )
    srch_qry = f"""    select c.CREATE_DTM
                    from CPS_DB.{correct_schema}.DC_TAGS v
                    join CPS_DB.{correct_schema}.DC_TAGSET c
                    ON v.TAGSET_ID = c.TAGSET_ID
                    where c.IS_DELETED = 'F'
                    and c.CREATE_DTM > TO_TIMESTAMP('{original_canvas_run_dtm}')
                    and c.dc_engagement_id = {eng_id}"""

    print(srch_qry)

    df = pd.read_sql(srch_qry, engine)

    print("check_if_needs_ts_refresh" , df)

    if df.empty:
        newer_tags_exsist = False

    else:
        newer_tags_exsist = True

    return newer_tags_exsist



storage_obj = Docker(
    base_image="837578041534.dkr.ecr.us-east-1.amazonaws.com/bases/prefect:0.15.13-python3.9",
    python_dependencies=[
        "pandas==1.4.2",
        "awswrangler==2.12.1",
        "numpy==1.25.1",
        "boto3",
        "botocore",
        "aiohttp==3.8.4",
        "hvac==0.11.2",
        "snowflake-sqlalchemy==1.2.4",
        "s3fs==0.4",
        "SQLAlchemy===1.4.41",
        "awswrangler==2.12.1",
        "fastparquet==0.7.2",
        "XlsxWriter==3.1.2",
        "oyaml==1.0",
        "networkx==2.8",
        "binpacking==1.5.2",
        "cloudpickle==2.0.0"
    ],
    registry_url="837578041534.dkr.ecr.us-east-1.amazonaws.com/dc/p1",
    path="/root/.prefect/flows/create_canvas_current_view.py",
    files={
        get_sec_dir("create_canvas_current_view.py"): "/root/.prefect/flows/create_canvas_current_view.py",
        get_sec_dir("common/sec.py"): "/root/.prefect/flows/common/sec.py",
        get_sec_dir("common/log_to_dc_job_messages.py"): "/root/.prefect/flows/common/log_to_dc_job_messages.py",
        get_sec_dir("common/trigger_prefect_flow.py"): "/root/.prefect/flows/common/trigger_prefect_flow.py",
        get_sec_dir("common/config.py"): "/root/.prefect/flows/common/config.py",
        get_sec_dir("common/aws_sec.py"): "/root/.prefect/flows/common/aws_sec.py",
    },
    env_vars={"PYTHONPATH": "${PYTHONPATH}:/root/.prefect/flows/"},
    ignore_healthchecks=True,
    stored_as_script=True
)

with Flow(
        "refresh-canvas-create-current",
        storage=storage_obj,
        run_config=KubernetesRun(memory_request=60000000000),
        # executor=LocalDaskExecutor(scheduler="threads", num_workers=(psutil.cpu_count(logical=True)-1)),
        executor=LocalDaskExecutor(scheduler="processes", num_workers=4),
        result=S3Result(bucket="cam-prefect-results"),
) as flow:
    cid = Parameter("canvas_id", required=True)
    scopes = Parameter("files", required=True)
    eid = Parameter("engagement_id", required=True)
    date = Parameter("date", required=True)  # run date
    env = Parameter("env", required=True)
    source_data_date_filter = Parameter("source_data_date_filter")
    tag_ids = Parameter("tag_ids", required=False)
    collector_files_ids = Parameter("collector_files", required=False, default=[])
    customer_files_ids = Parameter("customer_files", required=False, default=[])
    notification_id = Parameter("notification_id", required=False, default=0)
    # action = Parameter("action", required=False, default='create')
    original_canvas_run_dtm = Parameter("original_canvas_run_dtm", required=False)


    warehouse = "CPS_DSCI_ETL_EXT2_WH"
    warehouseMed = "cps_dsci_etl_wh"  # Medium
    warehouseXsmall = "CPS_DSCI_ETL_EXT1_WH"  # X-Small
    warehouseSmall = "CPS_DSCI_ETL_EXT2_WH"  # Small
    snowflake_db = "CPS_DB"
    wh_sm = "CPS_DSCI_ETL_EXT2_WH"
    wh_lg = "CPS_DSCI_ETL_EXT3_WH"
    working_schema = "CPS_DSCI_API"
    message_bucket = "data.canvas.messaging.cisco.com"
    bucket_name = "canvas-data-store-prod"
    loc_parquets = "/mnt/newmt/ERP/home/alanzen/bulk_tmp"
    failure_notes = []

    #
    # with case(action, "create", ):
    #     eid, cid = convert_to_string(eid, cid)
    #
    #     (
    #         scope_instance_tbl_name,
    #         aws_canvas_out_pth,
    #         date,
    #         run_date_mod,
    #         scope_sql,
    #         directives,
    #         relevant_tbl_name,
    #         requested_by,
    #         correct_schema,
    #     ) = prep_vars(scopes, eid, date, env, cid, source_data_date_filter, tag_ids, collector_files_ids, customer_files_ids,
    #                   upstream_tasks=[eid, cid])
    #
    #     logged_complete_to_gu = add_to_gu_log_table(
    #         env, cid, "InProgress", "insert", eid, requested_by, notification_id, upstream_tasks=[eid]
    #     )
    #     # pnt(scope_instance_tbl_name)
    #     # have all directives, flat eng data and scope ddl
    #     scope_rows = gen_scope_sql(
    #         scope_sql,
    #         env,
    #         correct_schema,
    #         scope_instance_tbl_name,
    #         cid,
    #         notification_id, requested_by,
    #         upstream_tasks=[
    #             scope_instance_tbl_name,
    #             aws_canvas_out_pth,
    #             date,
    #             run_date_mod,
    #             scope_sql,
    #             directives,
    #             relevant_tbl_name,
    #             requested_by,
    #             correct_schema,
    #             logged_complete_to_gu,
    #         ],
    #     )
    #     coverage_table = gen_coverage_data(
    #         scope_instance_tbl_name, cid, env, wh_lg, correct_schema, notification_id, requested_by,
    #         upstream_tasks=[scope_rows]
    #     )
    #     enrich_coverage_table = coverage_enrichments(
    #         coverage_table, eid, env, wh_sm, correct_schema, cid, notification_id, requested_by,
    #         upstream_tasks=[coverage_table]
    #     )
    #     multi_row_tbl = gen_current_data(
    #         scope_instance_tbl_name,
    #         env,
    #         wh_lg,
    #         correct_schema,
    #         coverage_table,
    #         cid,
    #         notification_id, requested_by,
    #         upstream_tasks=[coverage_table, enrich_coverage_table],
    #     )
    #     notes_tbl = gen_notes(multi_row_tbl, env, wh_sm, correct_schema, cid, notification_id, requested_by,
    #                           upstream_tasks=[multi_row_tbl])
    #     flt_table = flatten_data(multi_row_tbl, env, wh_sm, correct_schema, cid, notification_id, requested_by,
    #                              upstream_tasks=[notes_tbl])
    #     thought_spot_table = prep_final(
    #         cid,
    #         multi_row_tbl,
    #         notes_tbl,
    #         flt_table,
    #         env,
    #         wh_sm,
    #         correct_schema, notification_id, requested_by,
    #         upstream_tasks=[multi_row_tbl, notes_tbl, flt_table, enrich_coverage_table],
    #     )
    #
    #     parents = fix_missing_parents(
    #         thought_spot_table, flt_table, env, wh_sm, correct_schema, cid, notification_id, requested_by,
    #         upstream_tasks=[thought_spot_table]
    #     )
    #
    #     enrich_canvas_table = canvas_enrichments(
    #         thought_spot_table, eid, env, wh_sm, correct_schema, cid, notification_id, requested_by,
    #         upstream_tasks=[multi_row_tbl, parents]
    #     )
    #     renamed = rename_cols_in_preexsisting_table(
    #         thought_spot_table, env, correct_schema, cid, notification_id, requested_by,
    #         upstream_tasks=[enrich_canvas_table]
    #     )
    #
    #     logged, run_date = log_run_to_table(
    #         cid, directives, eid, correct_schema, "DC_CANVAS_HDR", env, notification_id, requested_by,
    #         upstream_tasks=[renamed]
    #     )
    #
    #     triggered = trigger_cloud_flow_run(cid, requested_by, env, correct_schema, eid, notification_id,
    #                                        upstream_tasks=[renamed])
    #
    #     meta = write_metadata_table(
    #         cid,
    #         eid,
    #         env,
    #         correct_schema,
    #         run_date,
    #         thought_spot_table, notification_id, requested_by,
    #         upstream_tasks=[renamed, scope_instance_tbl_name, logged, run_date],
    #     )
    #
    #     # remove tables  LAST!!http://172.18.138.27:8090/notebooks/current_view-Copy3.ipynb#
    #     removed = remove_working_tables(
    #         coverage_table,
    #         scope_instance_tbl_name,
    #         multi_row_tbl,
    #         notes_tbl,
    #         flt_table,
    #         relevant_tbl_name,
    #         env,
    #         correct_schema,
    #         cid, notification_id, requested_by,
    #         upstream_tasks=[meta, logged, run_date],
    #     )
    #
    #     logged_complete_to_gu = add_to_gu_log_table(
    #         env, cid, "Complete", "update", eid, requested_by, notification_id, upstream_tasks=[removed]
    #     )
    #
    #     all_done = final_flow_state_message(env, notification_id, requested_by, upstream_tasks=[logged_complete_to_gu])


    eid, cid = convert_to_string(eid, cid)

    (
        scope_instance_tbl_name,
        aws_canvas_out_pth,
        date,
        run_date_mod,
        scope_sql,
        directives,
        relevant_tbl_name,
        requested_by,
        correct_schema,
    ) = prep_vars(scopes, eid, date, env, cid, source_data_date_filter, tag_ids, collector_files_ids,
                  customer_files_ids,
                  upstream_tasks=[eid, cid])

    logged_complete_to_gu = add_to_gu_log_table(
        env, cid, "InProgress", "insert", eid, requested_by, notification_id, upstream_tasks=[eid]
    )
    # pnt(scope_instance_tbl_name)
    # have all directives, flat eng data and scope ddl
    scope_rows = gen_scope_sql(
        scope_sql,
        env,
        correct_schema,
        scope_instance_tbl_name,
        cid,
        notification_id, requested_by,
        upstream_tasks=[
            scope_instance_tbl_name,
            aws_canvas_out_pth,
            date,
            run_date_mod,
            scope_sql,
            directives,
            relevant_tbl_name,
            requested_by,
            correct_schema,
            logged_complete_to_gu,
        ],
    )
    coverage_table = gen_coverage_data(
        scope_instance_tbl_name, cid, env, wh_lg, correct_schema, notification_id, requested_by,
        upstream_tasks=[scope_rows]
    )
    enrich_coverage_table = coverage_enrichments(
        coverage_table, eid, env, wh_sm, correct_schema, cid, notification_id, requested_by,
        upstream_tasks=[coverage_table]
    )
    multi_row_tbl = gen_current_data(
        scope_instance_tbl_name,
        env,
        wh_lg,
        correct_schema,
        coverage_table,
        cid,
        notification_id, requested_by,
        upstream_tasks=[coverage_table, enrich_coverage_table],
    )
    notes_tbl = gen_notes(multi_row_tbl, env, wh_sm, correct_schema, cid, notification_id, requested_by,
                          upstream_tasks=[multi_row_tbl])
    flt_table = flatten_data(multi_row_tbl, env, wh_sm, correct_schema, cid, notification_id, requested_by,
                             upstream_tasks=[notes_tbl])
    thought_spot_table = prep_final(
        cid,
        multi_row_tbl,
        notes_tbl,
        flt_table,
        env,
        wh_sm,
        correct_schema, notification_id, requested_by,
        upstream_tasks=[multi_row_tbl, notes_tbl, flt_table, enrich_coverage_table],
    )

    parents = fix_missing_parents(
        thought_spot_table, flt_table, env, wh_sm, correct_schema, cid, notification_id, requested_by,
        upstream_tasks=[thought_spot_table]
    )

    enrich_canvas_table = canvas_enrichments(
        thought_spot_table, eid, env, wh_sm, correct_schema, cid, notification_id, requested_by,
        upstream_tasks=[multi_row_tbl, parents]
    )
    renamed = rename_cols_in_preexsisting_table(
        thought_spot_table, env, correct_schema, cid, notification_id, requested_by,
        upstream_tasks=[enrich_canvas_table]
    )

    logged, run_date = log_run_to_table(
        cid, directives, eid, correct_schema, "DC_CANVAS_HDR", env, notification_id, requested_by,
        upstream_tasks=[renamed]
    )

    needs_ts_refresh = check_if_needs_ts_refresh(correct_schema, original_canvas_run_dtm, eid,upstream_tasks=[logged, run_date])

    triggered = trigger_ts_refresh_tags(cid, requested_by, env, correct_schema, eid, notification_id, needs_ts_refresh,
                                       upstream_tasks=[needs_ts_refresh])

    meta = write_metadata_table(
        cid,
        eid,
        env,
        correct_schema,
        run_date,
        thought_spot_table, notification_id, requested_by,
        upstream_tasks=[renamed, scope_instance_tbl_name, logged, run_date],
    )

    # remove tables  LAST!!http://172.18.138.27:8090/notebooks/current_view-Copy3.ipynb#
    removed = remove_working_tables(
        coverage_table,
        scope_instance_tbl_name,
        multi_row_tbl,
        notes_tbl,
        flt_table,
        relevant_tbl_name,
        env,
        correct_schema,
        cid, notification_id, requested_by,
        upstream_tasks=[meta, logged, run_date],
    )

    logged_complete_to_gu = add_to_gu_log_table(
        env, cid, "Complete", "update", eid, requested_by, notification_id, upstream_tasks=[removed]
    )

    all_done = final_flow_state_message(env, notification_id, requested_by, upstream_tasks=[logged_complete_to_gu])

if __name__ == "__main__":
    flow.run(
        {
            "canvas_id": 28554,
            "date": "2024-06-17T19:02:25.202045",
            "engagement_id": 94,
            "env": "prod",
            "files": [{
                "name": "CONTRACTS"
            }, {
                "name": "BASELINE_TAGS"
            }],
            "source_data_date_filter": "2024-03-19 04:00:00+00:00",
            "tag_ids": [1379],
            "original_canvas_run_dtm" : "2024-03-19 04:00:00+00:00"


        }
    )
