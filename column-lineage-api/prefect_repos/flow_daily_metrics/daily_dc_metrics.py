import boto3
import pandas as pd
from prefect import Flow, Parameter, task
import datetime
from sqlalchemy import create_engine
import oyaml
from common import sec
import psutil
pd.set_option("display.max_columns", 400)
pd.set_option("display.max_rows", 400)
pd.set_option('display.max_colwidth', 4000)
temp_base_location = "/tmp"
from prefect.engine.results.s3_result import S3Result
from prefect.executors.dask import LocalDaskExecutor
from prefect.run_configs.kubernetes import KubernetesRun
from prefect.storage import Docker
import os



def get_json_from_s3(bucket, key):
    s3 = boto3.resource('s3')
    obj = s3.Object(bucket, key)
    data = obj.get()['Body'].read().decode('utf-8')
    json_data = oyaml.safe_load(data)
    return json_data

def explode_info(df, key_col, split_col, name_list):
    mce_l = []
    mce = df[[key_col, split_col]]
    for i, row in mce.iterrows():
        if row[split_col] is not None and len(row[split_col]) > 0:
            vals = row[split_col].replace("  ", " ").replace("  ", " ").replace(" ", ",").replace(",,", ",").split(',')
            for v in vals:
                if len(v) > 0:
                    # print(int(v))
                    mce_l.append([row[key_col], v])
    return pd.DataFrame(mce_l, columns=name_list)



def check_env(env):
    print(env)
    if env == "dev":
        cn = "dev_cps_dsci_etl_svc"
    elif env == "stage":
        cn = "stg_cps_dsci_etl_svc"
    elif env == "prod":
        cn = "prd_cps_dsci_etl_svc"
    return cn




@task(log_stdout=True, tags=["snowflake_xlarge"])
def build_unified_tag_view(env):
    today = pd.Timestamp(datetime.date.today())
    tag_view = f"CPS_DSCI_API.ALL_TAGS_TBL_{today.strftime('%Y_%m_%d')}"
    cn = check_env(env)
    engine = create_engine(sec.get_sf_pw(cn, 'CPS_DSCI_ETL_EXT4_WH', 'cps_dsci_archive'))
    #tblsql = "select TABLE_SCHEMA || '.' || TABLE_NAME as tag_tbls from information_schema.tables where TABLE_SCHEMA = 'CPS_DSCI_API' and TABLE_NAME like 'DATA_CANVAS_ENGAGEMENT_TAGS_%'"
    
    tblsql ="""with active_eng as (
            select  DC_ENGAGEMENT_ID from CPS_DSCI_API.DC_ENGAGEMENT_HDR where IS_DELETED = 'F'
            )
        ,exist as (
                select TABLE_SCHEMA || '.' || TABLE_NAME as tag_tbls ,
                    try_to_number(replace(TABLE_NAME, 'DC_ENGAGEMENT_TAGS_', '') ) as cid
                    from information_schema.tables
                    where TABLE_SCHEMA = 'CPS_DSCI_API' and TABLE_NAME like 'DC_ENGAGEMENT_TAGS_%'
            )
            select tag_tbls 
            from exist join active_eng on (exist.cid=active_eng.DC_ENGAGEMENT_ID)
         where exist.cid is not null and active_eng.DC_ENGAGEMENT_ID is not null"""
    
    tbls = pd.read_sql(tblsql, engine)
    union_sql = tbls.tag_tbls.to_list()
    union_sql = " union select * from ".join(union_sql)
    union_sql = f"""create or replace TABLE {tag_view} as 
                select * from {union_sql} ;"""
    print(union_sql)
    con = engine.connect()
    con.execute(union_sql)
    con.close()
    return tag_view
    
    
    
    
    
    

@task(log_stdout=True, tags=["snowflake_xlarge"])
def daily_dc_metrics(view):
    sf_warehouse = 'CPS_DSCI_ETL_EXT4_WH'
    schema = 'CPS_DSCI_ARCHIVE'

    engine = create_engine(sec.get_sf_pw('prd_cps_dsci_etl_svc', sf_warehouse, schema))

    # new metircs:

    splitable_sql = f"""
    
    ALTER SESSION SET QUERY_TAG = 'daily_dc_metrics';
    alter session set AUTOCOMMIT=TRUE;


    SET view = '{view}';
    set tags_tbl = '{view}';
    set cdte = (select  replace($view,'CPS_DSCI_API.ALL_TAGS_TBL_',''));
    
    set ttbl = concat('CPS_DSCI_API.ibstats_pl_',$cdte);
    set metrics_tbl = concat('CPS_DSCI_API.lifecycle_metrics_',$cdte);
    select $ttbl,$tags_tbl,$metrics_tbl;


create or replace transient table  IDENTIFIER($ttbl)  as
with flattened_contracts as (
        select distinct DC_ENGAGEMENT_ID, CONTRACT_NUMBER, BOOKING_CONTRACT as AM_BOOKING_CONTRACT, DC_USER_ID
        from CPS_DSCI_API.dc_managed_service_contracts
        where   IS_DELETED = 'F'
    )
      select
             ib.INSTANCE_ID,
             ib.PARENT_INSTANCE_ID ,
             f.CONTRACT_NUMBER,
             ib.INSTALL_AT_SITE_USE_ID,
             item.product_list_price_gpl_us ,
             ib.QUANTITY  ,
             f.AM_BOOKING_CONTRACT as AM_BOOKING_CONTRACT,
              f.DC_ENGAGEMENT_ID,
              f.DC_USER_ID,
              item.service_list_price ,
            nvl(cp.is_significant,'YES')                   as contract_simplification_is_significant,
                case
                   when item.mapped_to_service_flag = 'YES WITH SPM' then 'T'
                   else 'F' end as mapped_to_service,
                case when
            current_date < CPS_DSCI_ARCHIVE.FIX_DATES_W_DEF(item.last_date_of_support::DATE, '2150-12-31'::DATE) then 'F'
         else 'T' end is_past_ldos,
        case
           when IB.covered_status = 'A' then 'ACTIVE'
           when IB.covered_status = 'I' then 'EXPIRED'
           when IB.covered_status = 'N' then 'NEVER COVERED'
           end                                                                            as coverage_status,
         arrayagg(distinct tgs.tag_id) within group (order by tgs.tag_id desc) as tags,
         case when f.CONTRACT_NUMBER is not null then 'MANAGED' else 'UNMANAGED' end as is_managed
         from CPS_DSCI_BR.CAM_DS_INSTANCE_DETAIL ib
             left join IDENTIFIER($tags_tbl) tgs  on
             (  tgs.is_deleted = 'F' and ib.INSTANCE_ID = tgs.INSTANCE_ID AND NVL(IB.EDWSF_SOURCE_DELETED_FLAG, 'N') = 'N')
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
            left join  flattened_contracts  f on
                ( f.CONTRACT_NUMBER=try_to_number(contract_header.CONTRACT_NUMBER) and tgs.DC_ENGAGEMENT_ID=f.DC_ENGAGEMENT_ID )
            left join EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_SAIB_ITEMS item on
            (
              item.INVENTORY_ITEM_ID = ib.inventory_item_id
            and
                nvl(item.EDWSF_SOURCE_DELETED_FLAG, 'N') = 'N'
            )
          left join CPS_DSCI_ARCHIVE.CORRECTED_PIDS cp on (ib.ITEM_NAME = cp.ITEM_NAME)
        --where ib.INSTANCE_ID = ib.PARENT_INSTANCE_ID  -- parent level only but if we need to have $$ we will need to handle entire confir and create aggs at parent level
        group by
            ib.INSTANCE_ID,
            ib.PARENT_INSTANCE_ID ,
            ib.INSTALL_AT_SITE_USE_ID,
            f.CONTRACT_NUMBER,
             f.AM_BOOKING_CONTRACT ,
              f.DC_ENGAGEMENT_ID,
              f.DC_USER_ID,
             item.product_list_price_gpl_us ,
             ib.QUANTITY  ,
             item.service_list_price ,
            nvl(cp.is_significant,'YES'),
                case
                   when item.mapped_to_service_flag = 'YES WITH SPM' then 'T'
                   else 'F' end ,
                case when current_date < CPS_DSCI_ARCHIVE.FIX_DATES_W_DEF(item.last_date_of_support::DATE, '2150-12-31'::DATE) then 'F'
                else 'T' end,
        case
           when IB.covered_status = 'A' then 'ACTIVE'
           when IB.covered_status = 'I' then 'EXPIRED'
           when IB.covered_status = 'N' then 'NEVER COVERED'
           end                                                                            ,
         case when  f.CONTRACT_NUMBER is not null then 'MANAGED' else 'UNMANAGED' end;
         


create or replace transient table   IDENTIFIER($metrics_tbl)  as
with
    flattened_contracts as (
            select eru.DC_ENGAGEMENT_ID,  c.BOOKING_CONTRACT , ru.DC_USER_ID, count(CONTRACT_NUMBER) as distinct_service_contracts
            from CPS_DSCI_API.DC_BOOKINGS_CONTRACTS c
                left join CPS_DSCI_API.DC_BOOKINGS_CONTRACTS_RESPONSIBLE_USERS ru on ( ru.BOOKING_CONTRACT=c.BOOKING_CONTRACT and ru.IS_DELETED='F')
                left join CPS_DSCI_API.dc_ENGAGEMENT_TO_BOOKINGS_RESPONSIBLE_USER eru on (eru.BOOKING_CONTRACT=ru.BOOKING_CONTRACT and eru.DC_USER_ID=ru.DC_USER_ID and eru.IS_DELETED='F')
                left join  CPS_DSCI_API.DC_MANAGED_SERVICE_CONTRACTS msc on (
                   msc.DC_ENGAGEMENT_ID=eru.DC_ENGAGEMENT_ID and
                   msc.BOOKING_CONTRACT=ru.BOOKING_CONTRACT and
                   msc.DC_USER_ID=ru.DC_USER_ID and
                   msc.IS_DELETED='F'
                   )
           -- where  c.BOOKING_CONTRACT =	204254950
            group by eru.DC_ENGAGEMENT_ID,  c.BOOKING_CONTRACT , ru.DC_USER_ID
    ), employee_data as (
     select distinct LEVEL6_CISCO_WORKER_NAME,
                                        LEVEL7_CISCO_WORKER_NAME,
                                        LEVEL8_CISCO_WORKER_NAME,
                                        LEVEL9_CISCO_WORKER_NAME,
                                        u.USER_ID,
                                        h.EMP_NAME,
                                        h.mgr_name,
                                        nvl(h.emp_cco_id_masked, 'NOT IN ORG') as emp_cco_id_masked,
                                        h.dc_theater as USER_THEATER,
                                        u.USER_TITLE as ROLE,
                                        u.USER_ID    as DC_USER_ID,
                                        h.EMP_COUNTRY as delivered_by_country
                                       ,h.DC_SEGMENT  as mgr_segment
                        from CPS_DSCI_API.DC_USERS u
                                 left join CPS_DSCI_API.organizational_hierarchy h
                                           on (concat(h.emp_cco_id, '@cisco.com') = u.CISCO_CCO_ID)
                        where u.IS_DELETED = 'F'
                        and emp_cco_id_masked is not null
    )
    ,active as (
            select distinct e.DC_ENGAGEMENT_ID, e.ENGAGEMENT_NAME, st.sold_as_service_name,bp.buying_program_name,
                        t.theater_name as booking_theater_name,  pm.PRICING_MODEL_NAME, c.ACCOUNT_NAME, c.BOOKING_CONTRACT,
                        c.BOOKED_DATE,
                        c.agreement_start_date,c.agreement_end_date,c.create_dtm,
                        EMP.LEVEL6_CISCO_WORKER_NAME,
                        EMP.LEVEL7_CISCO_WORKER_NAME,
                        EMP.LEVEL8_CISCO_WORKER_NAME,
                        EMP.LEVEL9_CISCO_WORKER_NAME,
                        EMP.EMP_NAME,
                        EMP.mgr_name,
                        EMP.emp_cco_id_masked,
                        emp.dc_user_id,
                        EMP.USER_THEATER,
                        EMP.ROLE,
                        EMP.mgr_segment
                        ,rl.bookings_role,
                        c.BOOKED_SAV_1,c.BOOKED_SAV_2, c.BOOKED_SAV_3,
                        c.BOOKING_COUNTRY,
                        emp.delivered_by_country,
                        MGR.emp_cco_id_masked as claimed_by_manager
            from CPS_DSCI_API.dc_BOOKINGS_CONTRACTS c
            left join CPS_DSCI_API.dc_BOOKINGS_CONTRACTS_RESPONSIBLE_USERS r on ( r.BOOKING_CONTRACT=c.BOOKING_CONTRACT and  r.IS_DELETED = 'F'  )
            left join CPS_DSCI_API.dc_ENGAGEMENT_TO_BOOKINGS_RESPONSIBLE_USER eu on ( eu.BOOKING_CONTRACT=r.BOOKING_CONTRACT and eu.DC_USER_ID=r.DC_USER_ID  and  eu.IS_DELETED = 'F'  )
            left join CPS_DSCI_API.DC_ENGAGEMENT_HDR e on (e.DC_ENGAGEMENT_ID=eu.DC_ENGAGEMENT_ID and e.is_deleted = 'F' )
            left join CPS_DSCI_API.DC_PRICING_MODEL pm on ( pm.PRICING_TYPE_ID=c.SOLD_AS_PRICING_TYPE_ID)
            join CPS_DSCI_API.DC_SOLD_AS_SERVICE_TYPES st on ( st.SERVICE_TYPE_ID=c.SOLD_AS_SERVICE_TYPE_ID)  -- all are mapped to at least a 1
            join CPS_DSCI_API.dc_buying_programs bp on ( bp.BUYING_PROGRAM_TYPE_ID= c.BUYING_PROGRAM_TYPE_ID)
            join CPS_DSCI_API.dc_theater t on ( t.THEATER_ID = c.BOOKED_THEATER_ID)
            left join CPS_DSCI_API.dc_bookings_user_role rl on ( rl.BOOKINGS_ROLE_ID=r.SERVICE_ROLE_ID)
            LEFT JOIN employee_data EMP ON (r.DC_USER_ID=EMP.USER_ID)
            LEFT JOIN employee_data MGR ON (c.claimed_and_managed_by=MGR.USER_ID)
            where c.IS_DELETED = 'F'
              --and current_date between c.AGREEMENT_START_DATE and dateadd(day, 30, c.AGREEMENT_END_DATE)
    ), primary_cam as (
        select distinct  LEVEL6_CISCO_WORKER_NAME as pc_LEVEL6_CISCO_WORKER_NAME,
                        LEVEL7_CISCO_WORKER_NAME as  pc_LEVEL7_CISCO_WORKER_NAME,
                        LEVEL8_CISCO_WORKER_NAME as  pc_LEVEL8_CISCO_WORKER_NAME,
                        LEVEL9_CISCO_WORKER_NAME as  pc_LEVEL9_CISCO_WORKER_NAME,
                        EMP_NAME as pc_EMP_NAME,
                        mgr_name as pc_mgr_name,
                        emp_cco_id_masked as pc_emp_cco_id_masked,
                        c.BOOKING_CONTRACT as pc_BOOKING_CONTRACT,
                        e.DC_ENGAGEMENT_ID as pc_DC_ENGAGEMENT_ID
                        , e.ENGAGEMENT_NAME as pc_ENGAGEMENT_NAME
             from CPS_DSCI_API.dc_BOOKINGS_CONTRACTS c
            join CPS_DSCI_API.dc_BOOKINGS_CONTRACTS_RESPONSIBLE_USERS r on ( r.BOOKING_CONTRACT=c.BOOKING_CONTRACT and  r.IS_DELETED = 'F'  )
            join CPS_DSCI_API.dc_ENGAGEMENT_TO_BOOKINGS_RESPONSIBLE_USER eu on ( eu.BOOKING_CONTRACT=r.BOOKING_CONTRACT and eu.DC_USER_ID=r.DC_USER_ID  and  eu.IS_DELETED = 'F'  )
            join CPS_DSCI_API.DC_ENGAGEMENT_HDR e on (e.DC_ENGAGEMENT_ID=eu.DC_ENGAGEMENT_ID and e.is_deleted = 'F' )
            join CPS_DSCI_API.DC_PRICING_MODEL pm on ( pm.PRICING_TYPE_ID=c.SOLD_AS_PRICING_TYPE_ID)
            join CPS_DSCI_API.DC_SOLD_AS_SERVICE_TYPES st on ( st.SERVICE_TYPE_ID=c.SOLD_AS_SERVICE_TYPE_ID)  -- all are mapped to at least a 1
            join CPS_DSCI_API.dc_buying_programs bp on ( bp.BUYING_PROGRAM_TYPE_ID= c.BUYING_PROGRAM_TYPE_ID)
            join CPS_DSCI_API.dc_theater t on ( t.THEATER_ID = c.BOOKED_THEATER_ID)
            join CPS_DSCI_API.dc_bookings_user_role rl on ( rl.BOOKINGS_ROLE_ID=r.SERVICE_ROLE_ID)
            join employee_data ed on ( ed.USER_ID=eu.DC_USER_ID)
        where rl.bookings_role =  'CAM-PRIMARY'

),  disengaged as ( -- join at bookibg
    with mdis as (select distinct BOOKING_CONTRACT, max(s.create_dtm) mx_def_date
                  from CPS_DSCI_API.DC_WF_DISENGAGE s
                  group by BOOKING_CONTRACT
                  )
    select mdis.BOOKING_CONTRACT, mx_def_date, r.DISENGAGEMENT_REASON
        from mdis join  CPS_DSCI_API.DC_WF_DISENGAGE s on ( s.BOOKING_CONTRACT=mdis.BOOKING_CONTRACT and mdis.mx_def_date = s.CREATE_DTM )
            join CPS_DSCI_API.DC_TYP_DISENGAGE r on ( r.DISENGAGEMENT_REASON_ID = s.DISENGAGEMENT_REASON_ID)
        where s.IS_DELETED = 'F'
),    gu_per_eng_bc as (  -- join at engaggement level
        with aa as (-- active bookings to engagements with party data ideally
                select distinct p.CR_PARTY_ID , e.dc_engagement_id
                from CPS_DSCI_API.DC_ENGAGEMENT_HDR e
                         left join CPS_DSCI_API.DC_PARTY_LINKS p on (p.DC_ENGAGEMENT_ID = e.DC_ENGAGEMENT_ID and p.IS_DELETED = 'F')
                where e.IS_DELETED = 'F'
                )
                        select  aa.dc_engagement_id, listagg(h.GLOBAL_ULTIMATE_ID,',') as gus
                        from aa
                        join EDW_MASTER_ETL_DB.SS.CRT_XXNGCR_DNM_RELATIONSHIPS h
                        on (h.PARTY_ID = aa.CR_PARTY_ID)
                        group by aa.dc_engagement_id
    ), so as ( -- this and qualified SO need to be crisp granularity of booking contract level across 2 events signoff and disconnect... so is it really 1?
            with mx_date as (-- resolve to tru last event
                select s.BOOKING_CONTRACT, max(s.CREATE_DTM) as last_signoff_date
                from CPS_DSCI_API.DC_WF_IB_SIGNOFF s
                group by BOOKING_CONTRACT
            ) -- get the unique last event details
            select distinct s.BOOKING_CONTRACT,
                   case
                       when s.SIGNOFF_METHOD_ID != 7 then 'Signed off'
                       when s.SIGNOFF_METHOD_ID = 7 then 'Defered Signed off'
                       else 'sign_off_overdue'
                       end           as signoff_type,
                     last_signoff_date,
                 m.SIGNOFF_METHOD as ibv_method ,
                i.SIGN_OFF_IDENTITY as ibv_identity,
              e.SIGNOFF_EVENT as ibv_event
                from CPS_DSCI_API.DC_WF_IB_SIGNOFF s
                    join CPS_DSCI_API.DC_TYP_SIGNOFF_METHOD m on ( m.SIGNOFF_METHOD_ID=s.SIGNOFF_METHOD_ID)
                join CPS_DSCI_API.DC_TYP_SIGN_OFF_IDENTITY i on ( i.SIGN_OFF_IDENTITY_ID = s.SIGN_OFF_IDENTITY_ID)
                join CPS_DSCI_API.DC_TYP_SIGNOFF_EVENT e on ( e.SIGNOFF_EVENT_ID = s.signoff_event_id)
                join mx_date on ( mx_date.BOOKING_CONTRACT=s.BOOKING_CONTRACT and mx_date.last_signoff_date=s.CREATE_DTM)
                join CPS_DSCI_API.dc_BOOKINGS_CONTRACTS c
                          on (c.BOOKING_CONTRACT = s.BOOKING_CONTRACT and c.is_deleted = 'F')
            where current_date between c.AGREEMENT_START_DATE and dateadd(day, 30, c.AGREEMENT_END_DATE)
            and s.is_deleted = 'F'
    ),
    qualified_signoff as ( -- qualify the last event with current date for correct status
        select  distinct BOOKING_CONTRACT,ibv_method, ibv_identity,ibv_event,
           case
                when DATEDIFF(day, last_signoff_date,current_date) > 90 then  'sign_off_overdue'  -- regardless of type after 90 your overdue
                else signoff_type
           end as qualified_ibv,
           DATEDIFF(day, last_signoff_date,current_date) as days_since_last_signoff_event
        from so
    )  --select * from qualified_signoff
    select     active.ACCOUNT_NAME,
               active.BOOKING_CONTRACT ,
               ib_per_gu.AM_BOOKING_CONTRACT,
               active.DC_ENGAGEMENT_ID,
               active.ENGAGEMENT_NAME,
               active.sold_as_service_name,
               active.buying_program_name,
               active.booking_theater_name,
               active.PRICING_MODEL_NAME ,
               active.agreement_start_date,
               active.agreement_end_date,
               active.create_dtm,
               case
                   when qualified_signoff.qualified_ibv is null then 'Never Signed Off'
                   else qualified_signoff.qualified_ibv end as qualified_ibv ,
               max(days_since_last_signoff_event) as days_since_last_signoff_event,  -- safe-ish but not good in table
               case
                    when current_date between active.agreement_start_date and dateadd(days, 30, active.agreement_end_date) then 'Current'
                    when current_date < active.agreement_start_date then 'Future'
                    when current_date > dateadd(days, 30, active.agreement_end_date) then 'Expired'
                    else 'Not Current' end as current_contract,
                ib_per_gu.contract_simplification_is_significant ,
                ib_per_gu.is_managed,
                coverage_status,
                is_past_ldos,
                mapped_to_service,
                active.LEVEL6_CISCO_WORKER_NAME,
                active.LEVEL7_CISCO_WORKER_NAME,
                active.LEVEL8_CISCO_WORKER_NAME,
                active.LEVEL9_CISCO_WORKER_NAME,
                active.EMP_NAME,
                active.mgr_name,
                active.emp_cco_id_masked,
                active.USER_THEATER,
                active.ROLE,
                null as dc_user_id ,
                active.bookings_role,
                active.BOOKED_SAV_1,active.BOOKED_SAV_2,active.BOOKED_SAV_3,
                active.BOOKING_COUNTRY,
                active.delivered_by_country,
                active.claimed_by_manager,

             sum( case when ib_per_gu.INSTANCE_ID = ib_per_gu.parent_INSTANCE_ID then 1 else 0 end) as total_IB,
             count(distinct ib_per_gu.INSTALL_AT_SITE_USE_ID) as total_sites,
             sum(ib_per_gu.product_list_price_gpl_us  * ib_per_gu.QUANTITY) as  PLP_X_QUANTITY,
             sum(ib_per_gu.service_list_price * ib_per_gu.QUANTITY) as SLP_X_QUANTITY,
            ff.distinct_service_contracts as distinct_service_contracts,
             sum(case when ARRAYS_OVERLAP(array_construct(1746), ib_per_gu.TAGS)  then 1 else 0 end) as TOTAL_BASELINE_TAGS,
             sum(case when ARRAYS_OVERLAP(array_construct(1381), ib_per_gu.TAGS)  then 1 else 0 end) as SIGNED_OFF_IN_SCOPE_ACCOUNT_TEAM,
             sum(case when ARRAYS_OVERLAP(array_construct(1379), ib_per_gu.TAGS)  then 1 else 0 end) as SIGNED_OFF_IN_SCOPE_CUSTOMER,
             sum(case when ARRAYS_OVERLAP(array_construct(1380), ib_per_gu.TAGS)  then 1 else 0 end) as SIGNED_OFF_IN_SCOPE_PARTNER,
             sum(case when ARRAYS_OVERLAP(array_construct(1382), ib_per_gu.TAGS)  then 1 else 0 end) as SIGNED_OFF_IN_SCOPE_RENEWALS_MANAGER,
             sum(case when ARRAYS_OVERLAP(array_construct(1363), ib_per_gu.TAGS)  then 1 else 0 end) as TBD_DECISION_NEEDED,
             sum(case when ARRAYS_OVERLAP(array_construct(11176), ib_per_gu.TAGS)  then 1 else 0 end) as TBD_MONITOR,
             sum(case when ARRAYS_OVERLAP(array_construct(1364), ib_per_gu.TAGS)  then 1 else 0 end) as TBD_PROPOSED_IN_SCOPE,
             sum(case when ARRAYS_OVERLAP(array_construct(1360), ib_per_gu.TAGS)  then 1 else 0 end) as TBD_PROPOSED_OUT_OF_SCOPE,
             sum(case when ARRAYS_OVERLAP(array_construct(1367), ib_per_gu.TAGS)  then 1 else 0 end) as IN_CONFLICT_CUSTOMER_DISAGREEMENT,
             sum(case when ARRAYS_OVERLAP(array_construct(1362), ib_per_gu.TAGS)  then 1 else 0 end) as IN_CONFLICT_PARTNER_DISAGREEMENT,
             sum(case when ARRAYS_OVERLAP(array_construct(1361), ib_per_gu.TAGS)  then 1 else 0 end) as IN_CONFLICT_RECENT_SHIPMENT,
             sum(case when ARRAYS_OVERLAP(array_construct(1359), ib_per_gu.TAGS)  then 1 else 0 end) as IN_CONFLICT_SCOPE_AMENDMENT_NEEDED,
             sum(case when ARRAYS_OVERLAP(array_construct(1358), ib_per_gu.TAGS)  then 1 else 0 end) as SIGNED_OFF_OUT_OF_SCOPE,
             sum(case when TAGS is null then 0
                      when ARRAYS_OVERLAP(array_construct(1381,1379,1380,1382,1363,11176,1364,1360,1367,1362,1361,1359,1358), TAGS)  then 0
                  else 1 end) as NO_VALIDATED_IB_TAG_APPLIED,

            sum(case when ARRAYS_OVERLAP(array_construct(1746), ib_per_gu.TAGS)  then ib_per_gu.product_list_price_gpl_us * ib_per_gu.QUANTITY else 0 end) as GPL_TOTAL_BASELINE,
             sum(case when ARRAYS_OVERLAP(array_construct(1381), ib_per_gu.TAGS)  then ib_per_gu.product_list_price_gpl_us * ib_per_gu.QUANTITY else 0 end) as GPL_SIGNED_OFF_IN_SCOPE_ACCOUNT_TEAM,
             sum(case when ARRAYS_OVERLAP(array_construct(1379), ib_per_gu.TAGS)  then ib_per_gu.product_list_price_gpl_us * ib_per_gu.QUANTITY else 0 end) as GPL_SIGNED_OFF_IN_SCOPE_CUSTOMER,
             sum(case when ARRAYS_OVERLAP(array_construct(1380), ib_per_gu.TAGS)  then ib_per_gu.product_list_price_gpl_us * ib_per_gu.QUANTITY else 0 end) as GPL_SIGNED_OFF_IN_SCOPE_PARTNER,
             sum(case when ARRAYS_OVERLAP(array_construct(1382), ib_per_gu.TAGS)  then ib_per_gu.product_list_price_gpl_us * ib_per_gu.QUANTITY else 0 end) as GPL_SIGNED_OFF_IN_SCOPE_RENEWALS_MANAGER,
             sum(case when ARRAYS_OVERLAP(array_construct(1363), ib_per_gu.TAGS)  then ib_per_gu.product_list_price_gpl_us * ib_per_gu.QUANTITY else 0 end) as GPL_TBD_DECISION_NEEDED,
             sum(case when ARRAYS_OVERLAP(array_construct(11176), ib_per_gu.TAGS)  then ib_per_gu.product_list_price_gpl_us * ib_per_gu.QUANTITY else 0 end) as GPL_TBD_MONITOR,
             sum(case when ARRAYS_OVERLAP(array_construct(1364), ib_per_gu.TAGS)  then ib_per_gu.product_list_price_gpl_us * ib_per_gu.QUANTITY else 0 end) as GPL_TBD_PROPOSED_IN_SCOPE,
             sum(case when ARRAYS_OVERLAP(array_construct(1360), ib_per_gu.TAGS)  then ib_per_gu.product_list_price_gpl_us * ib_per_gu.QUANTITY else 0 end) as GPL_TBD_PROPOSED_OUT_OF_SCOPE,
             sum(case when ARRAYS_OVERLAP(array_construct(1367), ib_per_gu.TAGS)  then ib_per_gu.product_list_price_gpl_us * ib_per_gu.QUANTITY else 0 end) as GPL_IN_CONFLICT_CUSTOMER_DISAGREEMENT,
             sum(case when ARRAYS_OVERLAP(array_construct(1362), ib_per_gu.TAGS)  then ib_per_gu.product_list_price_gpl_us * ib_per_gu.QUANTITY else 0 end) as GPL_IN_CONFLICT_PARTNER_DISAGREEMENT,
             sum(case when ARRAYS_OVERLAP(array_construct(1361), ib_per_gu.TAGS)  then ib_per_gu.product_list_price_gpl_us * ib_per_gu.QUANTITY else 0 end) as GPL_IN_CONFLICT_RECENT_SHIPMENT,
             sum(case when ARRAYS_OVERLAP(array_construct(1359), ib_per_gu.TAGS)  then ib_per_gu.product_list_price_gpl_us * ib_per_gu.QUANTITY else 0 end) as GPL_IN_CONFLICT_SCOPE_AMENDMENT_NEEDED,
             sum(case when ARRAYS_OVERLAP(array_construct(1358), ib_per_gu.TAGS)  then ib_per_gu.product_list_price_gpl_us * ib_per_gu.QUANTITY else 0 end) as GPL_SIGNED_OFF_OUT_OF_SCOPE,
             sum(case when ARRAYS_OVERLAP(array_construct(1381,1379,1380,1382,1363,11176,1364,1360,1367,1362,1361,1359,1358), TAGS)
                 then ib_per_gu.product_list_price_gpl_us * ib_per_gu.QUANTITY else 0 end) as GPL_NO_VALIDATED_IB_TAG_APPLIED,


             sum(case when ARRAYS_OVERLAP(array_construct(1746), ib_per_gu.TAGS)  then ib_per_gu.service_list_price * ib_per_gu.QUANTITY else 0 end) as  SLP_TOTAL_BASELINE,
             sum(case when ARRAYS_OVERLAP(array_construct(1381), ib_per_gu.TAGS)  then ib_per_gu.service_list_price * ib_per_gu.QUANTITY else 0 end) as  SLP_SIGNED_OFF_IN_SCOPE_ACCOUNT_TEAM,
             sum(case when ARRAYS_OVERLAP(array_construct(1379), ib_per_gu.TAGS)  then ib_per_gu.service_list_price * ib_per_gu.QUANTITY else 0 end) as  SLP_SIGNED_OFF_IN_SCOPE_CUSTOMER,
             sum(case when ARRAYS_OVERLAP(array_construct(1380), ib_per_gu.TAGS)  then ib_per_gu.service_list_price * ib_per_gu.QUANTITY else 0 end) as  SLP_SIGNED_OFF_IN_SCOPE_PARTNER,
             sum(case when ARRAYS_OVERLAP(array_construct(1382), ib_per_gu.TAGS)  then ib_per_gu.service_list_price * ib_per_gu.QUANTITY else 0 end) as  SLP_SIGNED_OFF_IN_SCOPE_RENEWALS_MANAGER,
             sum(case when ARRAYS_OVERLAP(array_construct(1363), ib_per_gu.TAGS)  then ib_per_gu.service_list_price * ib_per_gu.QUANTITY else 0 end) as  SLP_TBD_DECISION_NEEDED,
             sum(case when ARRAYS_OVERLAP(array_construct(11176), ib_per_gu.TAGS)  then ib_per_gu.service_list_price * ib_per_gu.QUANTITY else 0 end) as SLP_TBD_MONITOR,
             sum(case when ARRAYS_OVERLAP(array_construct(1364), ib_per_gu.TAGS)  then ib_per_gu.service_list_price * ib_per_gu.QUANTITY else 0 end) as  SLP_TBD_PROPOSED_IN_SCOPE,
             sum(case when ARRAYS_OVERLAP(array_construct(1360), ib_per_gu.TAGS)  then ib_per_gu.service_list_price * ib_per_gu.QUANTITY else 0 end) as  SLP_TBD_PROPOSED_OUT_OF_SCOPE,
             sum(case when ARRAYS_OVERLAP(array_construct(1367), ib_per_gu.TAGS)  then ib_per_gu.service_list_price * ib_per_gu.QUANTITY else 0 end) as  SLP_IN_CONFLICT_CUSTOMER_DISAGREEMENT,
             sum(case when ARRAYS_OVERLAP(array_construct(1362), ib_per_gu.TAGS)  then ib_per_gu.service_list_price * ib_per_gu.QUANTITY else 0 end) as  SLP_IN_CONFLICT_PARTNER_DISAGREEMENT,
             sum(case when ARRAYS_OVERLAP(array_construct(1361), ib_per_gu.TAGS)  then ib_per_gu.service_list_price * ib_per_gu.QUANTITY else 0 end) as SLP_IN_CONFLICT_RECENT_SHIPMENT,
             sum(case when ARRAYS_OVERLAP(array_construct(1359), ib_per_gu.TAGS)  then ib_per_gu.service_list_price * ib_per_gu.QUANTITY else 0 end) as  SLP_IN_CONFLICT_SCOPE_AMENDMENT_NEEDED,
             sum(case when ARRAYS_OVERLAP(array_construct(1358), ib_per_gu.TAGS)  then ib_per_gu.service_list_price * ib_per_gu.QUANTITY else 0 end) as  SLP_SIGNED_OFF_OUT_OF_SCOPE,
             sum(case when ARRAYS_OVERLAP(array_construct(1381,1379,1380,1382,1363,11176,1364,1360,1367,1362,1361,1359,1358), TAGS)
                    then ib_per_gu.service_list_price * ib_per_gu.QUANTITY else 0 end) as SLP_NO_VALIDATED_IB_TAG_APPLIED,
            active.BOOKED_DATE,
            primary_cam.pc_LEVEL6_CISCO_WORKER_NAME,
            primary_cam.pc_LEVEL7_CISCO_WORKER_NAME,
            primary_cam.pc_LEVEL8_CISCO_WORKER_NAME,
            primary_cam.pc_LEVEL9_CISCO_WORKER_NAME,
            primary_cam.pc_EMP_NAME,
            primary_cam.pc_mgr_name,
            primary_cam.pc_emp_cco_id_masked,
            primary_cam.pc_BOOKING_CONTRACT,
            primary_cam.pc_DC_ENGAGEMENT_ID,
            qualified_signoff.ibv_method,
            qualified_signoff.ibv_identity,
              qualified_signoff.ibv_event,
            gu_per_eng_bc.gus as engagement_global_ultimates,
            disengaged.mx_def_date as last_disengage_date,
            disengaged.DISENGAGEMENT_REASON as last_disengage_reason

    from active
        left join IDENTIFIER($ttbl)  as ib_per_gu on (
                      active.DC_ENGAGEMENT_ID= ib_per_gu.DC_ENGAGEMENT_ID
                      and  active.booking_contract =ib_per_gu.am_booking_contract
                      and active.DC_USER_ID = ib_per_gu.DC_USER_ID
                            )
        left join  flattened_contracts ff on (
                active.DC_ENGAGEMENT_ID= ff.DC_ENGAGEMENT_ID
                      and  active.booking_contract =ff.booking_contract
                      and active.DC_USER_ID = ff.DC_USER_ID

             )
            left join qualified_signoff on (qualified_signoff.BOOKING_CONTRACT=active.BOOKING_CONTRACT )
            left join  primary_cam on ( primary_cam.pc_BOOKING_CONTRACT=active.BOOKING_CONTRACT)
            left join gu_per_eng_bc on ( gu_per_eng_bc.DC_ENGAGEMENT_ID = active.DC_ENGAGEMENT_ID )
            left join   disengaged  on ( disengaged.BOOKING_CONTRACT =  active.booking_contract)
            where active.booking_contract  is not null -- hack
            group by  active.ACCOUNT_NAME,
                  active.BOOKING_CONTRACT,
                  ib_per_gu.AM_BOOKING_CONTRACT, active.DC_ENGAGEMENT_ID, active.ENGAGEMENT_NAME,
                  active.sold_as_service_name,active.buying_program_name,active.booking_theater_name,
                  active.PRICING_MODEL_NAME ,
               ib_per_gu.contract_simplification_is_significant ,
               coverage_status,
               is_past_ldos,
               mapped_to_service, ib_per_gu.is_managed,
               active.agreement_start_date,active.agreement_end_date,active.create_dtm,
                    active.LEVEL6_CISCO_WORKER_NAME,
                    active.LEVEL7_CISCO_WORKER_NAME,
                    active.LEVEL8_CISCO_WORKER_NAME,
                    active.LEVEL9_CISCO_WORKER_NAME,
                    active.EMP_NAME,
                    active.mgr_name,
                    active.emp_cco_id_masked,
                    active.USER_THEATER,
                    active.ROLE,
                    null ,
                    active.bookings_role,
                    active.BOOKED_DATE,
                    case when qualified_signoff.qualified_ibv is null then 'Never Signed Off' else qualified_ibv end,
            primary_cam.pc_LEVEL6_CISCO_WORKER_NAME,
            primary_cam.pc_LEVEL7_CISCO_WORKER_NAME,
            primary_cam.pc_LEVEL8_CISCO_WORKER_NAME,
            primary_cam.pc_LEVEL9_CISCO_WORKER_NAME,
            primary_cam.pc_EMP_NAME,
            primary_cam.pc_mgr_name,
            primary_cam.pc_emp_cco_id_masked,
            primary_cam.pc_BOOKING_CONTRACT,
            primary_cam.pc_DC_ENGAGEMENT_ID,
            gu_per_eng_bc.gus ,
            disengaged.mx_def_date ,
            disengaged.DISENGAGEMENT_REASON ,
            qualified_signoff.ibv_method,
               qualified_signoff.ibv_identity,
                qualified_signoff.ibv_event, active.BOOKED_SAV_1,active.BOOKED_SAV_2,active.BOOKED_SAV_3,
                active.BOOKING_COUNTRY,
                active.delivered_by_country,
                active.claimed_by_manager,
                ff.distinct_service_contracts
;



create or replace transient table  CPS_DSCI_API.lifecycle_metrics as select * from IDENTIFIER($metrics_tbl) ;         
         
         
         
        
"""
    con = engine.connect()
    for s in splitable_sql.split(';'):
        print(s)
        con.execute(s)
    con.close()
    return True


def get_sec_dir():
    return os.getcwd() + "/common/sec.py"




storage_obj = Docker(
       base_image="837578041534.dkr.ecr.us-east-1.amazonaws.com/bases/prefect:0.15.13-python3.9",
    python_dependencies=[
        "pandas==1.4.2",
        "awswrangler==2.12.1",
        "numpy",
        "boto3",
        "botocore",
        "aiohttp==3.8.4",
        "hvac==0.11.2",
        "snowflake-sqlalchemy==1.2.4",
        "s3fs==0.4",
        "SQLAlchemy===1.4.35",
        "awswrangler==2.12.1",
        "fastparquet==0.7.2",
        "XlsxWriter==3.1.2",
        "oyaml==1.0",
        "networkx==2.8",
        "binpacking==1.5.2",
        "cloudpickle==2.0.0"

    ],
    registry_url="837578041534.dkr.ecr.us-east-1.amazonaws.com/flows",
    files={
        get_sec_dir(): "/root/.prefect/flows/common/sec.py"
    },
    env_vars={"PYTHONPATH": "${PYTHONPATH}:/root/.prefect/flows/"},
)

with Flow(
        "daily-dc-metrics",
        storage= storage_obj,
        run_config=KubernetesRun(memory_request=60000000000, labels=["dev"]),
        executor=LocalDaskExecutor(scheduler="processes", num_workers=psutil.cpu_count(logical=True)),
        #executor=LocalDaskExecutor(scheduler="processes", num_workers=16),
        result=S3Result(bucket="cam-prefect-results")
) as flow:

    view = build_unified_tag_view('prod')
    daily_dc_metrics = daily_dc_metrics(view)

if __name__ == "__main__":
    flow.run()