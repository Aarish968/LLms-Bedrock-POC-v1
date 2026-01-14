import os
from pathlib import Path

import oracledb
import pandas as pd
from prefect import Flow, task
from prefect import resource_manager
from prefect.run_configs.kubernetes import KubernetesRun

from dc_p1_htec_etl.common.sec import create_sf_conn, aws_sec

temp_base_location = "/tmp"
from prefect.engine.results.s3_result import S3Result
from prefect.storage import Docker


@resource_manager
class Oracle_Connection:
    def __init__(self):
        self.host = "72.163.53.175"
        self.port = "1541"
        self.dbname = "cx_oracle"
        self.username = "APPSRO"
        self.password = "Rj4A_c3N"

    def setup(self):
        oracledb.defaults.fetch_lobs = False
        dsn = oracledb.makedsn(self.host, self.port, self.dbname)
        dsn = """(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=72.163.53.175)(PORT=1541))(ADDRESS=(PROTOCOL=TCP)(HOST=72.163.53.176)(PORT=1541)))(CONNECT_DATA=(SERVICE_NAME=SCAPRD.cisco.com)(SERVER=DEDICATED)))"""
        return oracledb.connect(user=self.username, password=self.password, dsn=dsn)

    @staticmethod
    def cleanup(conn):
        conn.close()


@task(log_stdout=True)
def pull_HTEC(conn):
    local_store = "/tmp"
    this_date = pd.to_datetime("today")
    # overide when necessary
    # this_date =  date(2024,1,31)
    this_date_mod_str = this_date.strftime("%Y_%m_%d")
    out_name = os.path.join(local_store, this_date_mod_str)

    # cr_engine = create_engine(sec.finance_ro_connection)
    sql = f"""SELECT
        qv.quote_id_c,
        qv.quote_version,
        qv.quote_discount,
        qv.quote_net_price,
        qv.htom_net_price,
        qv.htts_net_price,
        qv.hte_net_price,
        qv.am_net_price,
        qv.test_flag,
        qv.request_flag,
        qv.ep_insert_date AS quote_create_date,
        qm.request_type,
        qm.customer_name AS quote_customer_name,
        qm.ft_master_id,
        qm.ft_service_id,
        qm.line_of_business,
        qm.global_flag,
        qm.sales_theater,
        qm.region,
        qm.country,
        qm.scoping_method,
        qm.cscc_quote_number,
        qm.cscc_ann_list_price,
        qm.install_base,
        qm.tsa_supported_contracts,
        qm.calc_tlp,
        qm.month_duration,
        qm.dsa_deal_id AS quote_deal_id,
        qm.quote_cost,
        qm.quote_list_price,
        qm.core_sku,
        qm.htom_cost,
        qm.htom_list_price,
        qm.htts_cost,
        qm.htts_list_price,
        qm.hte_cost,
        qm.hte_list_price,
        qm.htom_util_pct,
        qm.hte_util_pct,
        qm.am_sku,
        qm.am_cost,
        qm.am_list_price,
        qm.mw,
        qm.growth_factor,
        qm.mw_cost,
        qm.mw_list_price,
        qm.sla_cost,
        qm.sla_list_price,
        qm.program_type,
        qm.am_util_pct,
        fs.id AS service_id,
        fs.contract_number,
        fs.start_date,
        fs.end_date,
        fs.funding_source,
        fs.quote_number,
        fs.dsa_deal_id,
        fs.contract_term,
        fs.status,
        fs.booking_date,
        fs.so_num as SALES_ORDER,
        fs.quote_number_provided,
        fm.master_id,
        fm.cpy_name AS ft_cpy_name,
        fm.portfolio,
        fm.archived,
        fm.parent_master_id,
        acdv.cpy_key,
        acdv.cpy_name,
        acdv.sales_level1,
        acdv.sales_level2,
        acdv.sales_level3,
        acdv.sales_level4,
        acdv.sales_level5,
        acdv.sales_level6,
        acdv.uscom_pooling,
        acdv.bcs_pooling,
        acdv.ps_pooling
        FROM
        eportal_adm.ec_pipeline_spa_quote_version qv
        JOIN eportal_adm.ec_pipeline_spa_quote_master qm ON qv.quote_id_c = qm.quote_id_c
        JOIN eportal_adm.financial_services fs ON qm.quote_id_c = fs.quote_number
        JOIN eportal_adm.financial_master fm ON fm.master_id = fs.master_id
        LEFT JOIN eportal_adm.asap_company_data_v acdv ON fm.portfolio = acdv.portfolio
        """
    if not os.path.exists(out_name):
        Path(out_name).mkdir(parents=True, exist_ok=True)

        available_data = pd.read_sql(sql, conn)
        print(available_data.head())
        available_data.to_parquet(
            os.path.join(out_name, "data.parquet"),
            engine="pyarrow",
            compression="snappy",
            index=False,
        )
    else:
        print(f"already done {out_name}")

    return out_name


@task(log_stdout=True)
def create_htec_table(file_loc, sf_env):
    con, engine = create_sf_conn(sf_env)

    df = pd.read_parquet(file_loc)

    resultsS = con.execute(
        "truncate table CPS_DSCI_API.HTEC_FINANCE_DATA_FEED_STG"
    ).fetchall()
    resultsS = con.execute("commit").fetchall()
    print(resultsS)
    df.to_sql(
        "HTEC_FINANCE_DATA_FEED_STG".lower(),
        engine,
        schema="CPS_DSCI_API",
        index=False,
        if_exists="append",
        chunksize=7000,
    )

    return True


@task(log_stdout=True)
def final_merge_query(sf_env):
    con, engine = create_sf_conn(sf_env)

    htec_query_0 = """MERGE INTO CPS_DB.CPS_DSCI_API.HTEC_FINANCE_DATA d
      USING CPS_DB.CPS_DSCI_API.HTEC_FINANCE_DATA_FEED_STG s
    ON      d.quote_id_c = s.quote_id_c
        AND d.service_id = s.service_id
        AND nvl(d.sales_order,'-999') = nvl(s.sales_order,'-999')
        AND nvl(d.cpy_key,-999)::int = nvl(s.cpy_key,-999)::int
  WHEN NOT MATCHED THEN INSERT(QUOTE_ID_C, QUOTE_VERSION, QUOTE_DISCOUNT, QUOTE_NET_PRICE, HTOM_NET_PRICE, HTTS_NET_PRICE, HTE_NET_PRICE, AM_NET_PRICE, TEST_FLAG, REQUEST_FLAG, QUOTE_CREATE_DATE, REQUEST_TYPE, QUOTE_CUSTOMER_NAME, FT_MASTER_ID, FT_SERVICE_ID, LINE_OF_BUSINESS, GLOBAL_FLAG, SALES_THEATER, REGION, COUNTRY, SCOPING_METHOD, CSCC_QUOTE_NUMBER, CSCC_ANN_LIST_PRICE, INSTALL_BASE, TSA_SUPPORTED_CONTRACTS, CALC_TLP, MONTH_DURATION, QUOTE_DEAL_ID, QUOTE_COST, QUOTE_LIST_PRICE, CORE_SKU, HTOM_COST, HTOM_LIST_PRICE, HTTS_COST, HTTS_LIST_PRICE, HTE_COST, HTE_LIST_PRICE, HTOM_UTIL_PCT, HTE_UTIL_PCT, AM_SKU, AM_COST, AM_LIST_PRICE, MW, GROWTH_FACTOR, MW_COST, MW_LIST_PRICE, SLA_COST, SLA_LIST_PRICE, PROGRAM_TYPE, AM_UTIL_PCT, SERVICE_ID, CONTRACT_NUMBER, START_DATE, END_DATE, FUNDING_SOURCE, QUOTE_NUMBER, DSA_DEAL_ID, CONTRACT_TERM, STATUS, BOOKING_DATE, SALES_ORDER, QUOTE_NUMBER_PROVIDED, MASTER_ID, FT_CPY_NAME, PORTFOLIO, ARCHIVED, PARENT_MASTER_ID, CPY_KEY, CPY_NAME, SALES_LEVEL1, SALES_LEVEL2, SALES_LEVEL3, SALES_LEVEL4, SALES_LEVEL5, SALES_LEVEL6, USCOM_POOLING, BCS_POOLING, PS_POOLING
                              ,create_dtm)
  VALUES (s.QUOTE_ID_C, s.QUOTE_VERSION, s.QUOTE_DISCOUNT, s.QUOTE_NET_PRICE, s.HTOM_NET_PRICE, s.HTTS_NET_PRICE, s.HTE_NET_PRICE, s.AM_NET_PRICE, s.TEST_FLAG, s.REQUEST_FLAG, s.QUOTE_CREATE_DATE, s.REQUEST_TYPE, s.QUOTE_CUSTOMER_NAME, s.FT_MASTER_ID, s.FT_SERVICE_ID, s.LINE_OF_BUSINESS, s.GLOBAL_FLAG, s.SALES_THEATER, s.REGION, s.COUNTRY, s.SCOPING_METHOD, s.CSCC_QUOTE_NUMBER, s.CSCC_ANN_LIST_PRICE, s.INSTALL_BASE, s.TSA_SUPPORTED_CONTRACTS, s.CALC_TLP, s.MONTH_DURATION, s.QUOTE_DEAL_ID, s.QUOTE_COST, s.QUOTE_LIST_PRICE, s.CORE_SKU, s.HTOM_COST, s.HTOM_LIST_PRICE, s.HTTS_COST, s.HTTS_LIST_PRICE, s.HTE_COST, s.HTE_LIST_PRICE, s.HTOM_UTIL_PCT, s.HTE_UTIL_PCT, s.AM_SKU, s.AM_COST, s.AM_LIST_PRICE, s.MW, s.GROWTH_FACTOR, s.MW_COST, s.MW_LIST_PRICE, s.SLA_COST, s.SLA_LIST_PRICE, s.PROGRAM_TYPE, s.AM_UTIL_PCT, s.SERVICE_ID, s.CONTRACT_NUMBER, s.START_DATE, s.END_DATE, s.FUNDING_SOURCE, s.QUOTE_NUMBER, s.DSA_DEAL_ID, s.CONTRACT_TERM, s.STATUS, s.BOOKING_DATE, s.SALES_ORDER, s.QUOTE_NUMBER_PROVIDED, s.MASTER_ID, s.FT_CPY_NAME, s.PORTFOLIO, s.ARCHIVED, s.PARENT_MASTER_ID, s.CPY_KEY, s.CPY_NAME, s.SALES_LEVEL1, s.SALES_LEVEL2, s.SALES_LEVEL3, s.SALES_LEVEL4, s.SALES_LEVEL5, s.SALES_LEVEL6, s.USCOM_POOLING, s.BCS_POOLING, s.PS_POOLING
                            ,current_date)"""

    qry_0_done = con.execute(htec_query_0)

    htec_query_1 = """create or replace transient  table CPS_DSCI_API.tmp_htec_updates as
with htec as (
select
    case
        when upper(SALES_LEVEL1) like  '%APJ%' then 4
        when upper(SALES_LEVEL1) like  '%EMEA%' then 3
        when upper(SALES_LEVEL1) like  '%AMER%' then 2
        else 1 end as h_booking_theater_id,
    case
        when upper(REQUEST_TYPE) like '%RENEW%' then 'RENEWAL'
        when upper(REQUEST_TYPE) like '%INCRE%' then 'UPSELL'
        when upper(REQUEST_TYPE) like '%NEW%' then 'NEW'
        else 'NOT KNOWN / NEW TYPE'
        end as h_new_renew,
                   COUNTRY         as h_booked_country,
               AM_NET_PRICE   as h_booked_revenue,
               AM_COST         as h_CAM_COST,
               CONTRACT_NUMBER as h_booking_contract,
               QUOTE_NUMBER    as h_audit_to_quote,
               AM_UTIL_PCT     as h_booked_allocation_TOTAL,
               START_DATE      as h_agreement_start_date,
               END_DATE        as h_agreement_end_date,
               BOOKING_DATE    as h_BOOKING_DATE,
               SALES_LEVEL1, SALES_LEVEL2, SALES_LEVEL3,QUOTE_CUSTOMER_NAME
                from  CPS_DSCI_BR.HTEC_QUOTES
                where  current_date between START_DATE and END_DATE
    ),
  dups as (select
              CONTRACT_NUMBER , count(0)
              from  CPS_DSCI_BR.HTEC_QUOTES
              where CONTRACT_NUMBER  is not null
              and current_date between START_DATE and END_DATE
              group by CONTRACT_NUMBER
              having count(0) > 1
              )
select
        htec.h_booking_contract,c.BOOKING_CONTRACT,
        htec.h_booking_theater_id,t.THEATER_NAME,
       htec.h_new_renew, 'UNK' as new_ren_dc,
       htec.h_booked_country,c.BOOKING_COUNTRY,
        htec.h_booked_revenue,c.CAM_REVENUE_USD,
        htec.h_CAM_COST,c.CAM_COST_USD,
        htec.h_audit_to_quote,'tbd_audit' as audit,
        htec.h_booked_allocation_TOTAL,nvl(c.SOLD_AS_SW_ALLOCATION,0)+nvl(SOLD_AS_HW_ALLOCATION,0) as total_int_allocation,
        htec.h_agreement_start_date,c.agreement_start_date,
        htec.h_agreement_end_date,c.agreement_end_date,
        htec.h_BOOKING_DATE, 'NO_DATE' as dc_booking_date,
        htec.SALES_LEVEL1, htec.SALES_LEVEL2, htec.SALES_LEVEL3,QUOTE_CUSTOMER_NAME
       from htec
    left join CPS_DSCI_API.DC_BOOKINGS_CONTRACTS c on ( c.BOOKING_CONTRACT=htec.h_booking_contract and c.IS_DELETED = 'F')
    left join CPS_DSCI_API.DC_THEATER t on (t.THEATER_ID=c.BOOKED_THEATER_ID )
    where htec.h_booking_contract not in (select dups.CONTRACT_NUMBER from dups)
order by c.booking_contract;"""

    # select h_booking_contract, count(0) from CPS_DSCI_API.tmp_htec_updates group by h_booking_contract
    # having count(0) >1;"""

    qry_1_done = con.execute(htec_query_1)

    htec_query_2 = """insert into CPS_DSCI_API.DC_BOOKINGS_CONTRACTS(BOOKING_CONTRACT, ACCOUNT_NAME, BOOKED_SAV_1, BOOKED_SAV_2,BOOKED_SAV_3,  BOOKED_THEATER_ID,
                                               SOLD_AS_SW_ALLOCATION, SOLD_AS_HW_ALLOCATION,
                                               CAM_REVENUE_USD, AGREEMENT_START_DATE, AGREEMENT_END_DATE,
                                               BOOKING_COUNTRY,
                                               CAM_COST_USD,
                                               CREATED_BY, CREATE_DTM,
                                               SOUCED_ALLOCATION,
                                               quote_for_audit,
                                               SOURCE_OF_ALLOCATION,
                                               derived_new_renew, UPDATE_DTM, booked_date, buying_program_type_id)
select u.h_booking_contract, QUOTE_CUSTOMER_NAME, SALES_LEVEL1, SALES_LEVEL2, SALES_LEVEL3, h_booking_theater_id, 0 as SW_allocation, h_booked_allocation_TOTAL as HW_ALLOCATION,
       u.h_booked_revenue,   u.h_agreement_start_date,   u.h_agreement_end_date, h_booked_country,h_CAM_COST, 'alanzen@cisco.com', current_timestamp,
       h_booked_allocation_TOTAL, 'HTEC_AUTOMATED_FEED', h_audit_to_quote, h_new_renew,current_timestamp, h_BOOKING_DATE, -1
       from CPS_DSCI_API.tmp_htec_updates u
       where BOOKING_CONTRACT is null
         and  u.h_booking_contract not in (select booking_contract from CPS_DSCI_API.DC_BOOKINGS_CONTRACTS );"""

    qry_2_done = con.execute(htec_query_2)

    return True


def get_sec_dir(pth):
    return os.path.join(os.getcwd(), pth)


@task(log_stdout=True)
def do_cxea(sf_env):
    con, engine = create_sf_conn(sf_env)
    cxea_qry = """MERGE INTO CPS_DB.CPS_DSCI_API.DATA_FEED_ACAT_CXEA d
              USING (

               SELECT
            sub_ref_id,
            deal_id,
            customer_id,
            customer_name,
            manual_acct_start_date,
            next_true_up_date,
           Aggreement_start_date as agreement_start_date ,
           aggreement_end_date as agreement_end_date,
           entitlement_end_date,
           creation_date,
           current_request_id,
           baseline_request_id,
           anniversary_date,
           qualification_identifier,
           auto_sweeps_flag,
           auto_add,
           gu_id,
           master_agreement_id,
           proposal_completion_date, offer_type, previous_anniversary_date, DISTI_BILL_TO_SITE_USE_ID,
           region
           FROM SERVICES_DB.SERVICES_IB_FBV.BV_IBSA_ACAT_CUSTOMER_MASTER m
            where  sub_ref_id is not null

              ) s
            ON      d.subscription_number = try_to_number(regexp_replace(s.sub_ref_id, '[^\\\d]',''))
          WHEN NOT MATCHED and try_to_number(regexp_replace(s.sub_ref_id, '[^\\\d]','')) is not null  THEN INSERT(subscription_number, SUB_REF_ID, DEAL_ID, CUSTOMER_ID, CUSTOMER_NAME, MANUAL_ACCT_START_DATE, NEXT_TRUE_UP_DATE, AGREEMENT_START_DATE, AGREEMENT_END_DATE,
                                       ENTITLEMENT_END_DATE, CREATION_DATE, CURRENT_REQUEST_ID, BASELINE_REQUEST_ID, ANNIVERSARY_DATE, QUALIFICATION_IDENTIFIER, AUTO_SWEEPS_FLAG,
                                       AUTO_ADD, GU_ID, MASTER_AGREEMENT_ID, PROPOSAL_COMPLETION_DATE, OFFER_TYPE, PREVIOUS_ANNIVERSARY_DATE, DISTI_BILL_TO_SITE_USE_ID
                                      ,create_dtm)
          VALUES (regexp_replace(sub_ref_id, '[^\\\d]','') , s.SUB_REF_ID, s.DEAL_ID, s.CUSTOMER_ID, s.CUSTOMER_NAME, s.MANUAL_ACCT_START_DATE, s.NEXT_TRUE_UP_DATE, s.AGREEMENT_START_DATE, s.AGREEMENT_END_DATE,
                                       ENTITLEMENT_END_DATE, s.CREATION_DATE, s.CURRENT_REQUEST_ID, s.BASELINE_REQUEST_ID, s.ANNIVERSARY_DATE, s.QUALIFICATION_IDENTIFIER, s.AUTO_SWEEPS_FLAG,
                                       AUTO_ADD, s.GU_ID, s.MASTER_AGREEMENT_ID, s.PROPOSAL_COMPLETION_DATE, s.OFFER_TYPE, s.PREVIOUS_ANNIVERSARY_DATE, s.DISTI_BILL_TO_SITE_USE_ID
                                      ,current_timestamp);"""

    cxea_qry2 = """
    insert into CPS_DSCI_API.DC_BOOKINGS_CONTRACTS(BOOKING_CONTRACT, ACCOUNT_NAME,BOOKED_THEATER_ID,
                                                   SOLD_AS_SERVICE_TYPE_ID, SOLD_AS_PRICING_TYPE_ID, BUYING_PROGRAM_TYPE_ID,
                                                   IB_CALC_SW_ALLOCATION, IB_CALC_HW_ALLOCATION, SOLD_AS_SW_ALLOCATION, SOUCED_ALLOCATION,
                                                   AGREEMENT_START_DATE, AGREEMENT_END_DATE,
                                                   CREATE_DTM, update_dtm)
                        select SUBSCRIPTION_NUMBER,  customer_name, 1, 
                        2, -- SOLD_AS_SERVICE_TYPE_ID as HW
                        1, -- SOLD_AS_PRICING_TYPE_ID as unknown
                        2,  -- BUYING_PROGRAM_TYPE_ID as designated and a process will set the value based on finance data
                        0,0,0,10,AGREEMENT_START_DATE,
                        AGREEMENT_END_DATE, CREATION_DATE, current_timestamp
                        from CPS_DB.CPS_DSCI_API.DATA_FEED_ACAT_CXEA
                        where subscription_number not in
                        (
                        select distinct c.BOOKING_CONTRACT  from CPS_DSCI_API.DC_BOOKINGS_CONTRACTS c
                        )
     """

    pre_cxea_count = pd.read_sql(
        "select count(*) from CPS_DSCI_API.DC_BOOKINGS_CONTRACTS", engine
    )

    print(pre_cxea_count)

    complete = con.execute(cxea_qry)

    complete = con.execute(cxea_qry2)

    post_cxea_count = pd.read_sql(
        "select count(*) from CPS_DSCI_API.DC_BOOKINGS_CONTRACTS", engine
    )

    print(post_cxea_count)

    return True


@task(log_stdout=True)
def do_cpq(sf_env):
    con, engine = create_sf_conn(sf_env)

    cpq_qry = """create or replace view CPS_DSCI_BR.CPQ_QUOTES as
        select
        q.quote_name,
        q.QUOTE_THEATER,
        q.theater,
        q.SALES_LEVEL_1,
        q.SALES_LEVEL_2,
        q.ESTIMATE_SUB_TYPE,
        q.CPQ_ESTIMATE_TYPE_NEW__C,
        q.CPQ_COUNTRY__C,
        q.status,
        q.ERP_ORDER_NUMBER,
        q.ORDERED,
        ql.net_price_usd as CAM_REVENUE,
        ql.total_cost_usd as CAM_COST,
        ql.total_cost,
       case when ql.net_price_usd = 0 then 0
            else  (ql.net_price_usd-ql.total_cost_usd)/ql.net_price_usd end as CAM_MARGIN,
        ql.erp_order_line_id, -- not null on good one when dups?
        ql.ERP_SO_NUMBER, -- not null on good one when dups?
        ql.CPQ_CONTRACT_NUMBER, -- not null on good one when dups?
        q.net_margin,
        q.ccw_quote_id,
        q.QUOTE_NUMBER,
        q.TOTAL_ONSITE_HOURS,
        q.TOTAL_REMOTE_HOURS,
        q.TOTAL_WORK_HOURS,
        q.TOTAL_WORK_HOURS_FORMULA,
        ql.total_hours,
        ql.total_hours/(round(DATEDIFF(days, q.START_DATE::date, q.END_DATE::date)/30.5)/12)/1768*100 as c2ll,
        --(q.total_work_hours*(12/duration))/(round(DATEDIFF(days, q.START_DATE::date, q.END_DATE::date)/30.5)/12)/1768*100 as c2ll_FAKE,
        round(DATEDIFF(days, ql.START_DATE::date, ql.END_DATE::date)/30.5) as duration_LL,
        round(DATEDIFF(days, q.START_DATE::date, q.END_DATE::date)/30.5) as duration,
        q.START_DATE::date as q_START_DATE,
        q.END_DATE::date as q_END_DATE,
        ql.START_DATE::date as ql_START_DATE,
        ql.END_DATE::date as ql_END_DATE,
        ql.EFFECTIVE_START_DATE::date as ql_EFFECTIVE_START_DATE,
        ql.EFFECTIVE_END_DATE::date as ql_EFFECTIVE_END_DATE,
        ql.EFFECTIVE_SUBSCRIPTION_TERM  as ql_EFFECTIVE_SUBSCRIPTION_TERM,
        END_DATE_OVERRIDE,
        EXCHANGE_RATE_DATETIME
     from SERVICES_DB.SERVICES_AS_FBV.BV_DSCI_CPQ_QUOTE q
        join SERVICES_DB.SERVICES_AS_FBV.BV_DSCI_CPQ_QUOTE_LINE ql on ( ql.SK_QUOTE_ID=q.SK_QUOTE_ID)
        where q.DELETED='FALSE'
        and ql.DELETED='FALSE'
        AND ql.ESTIMATE_TYPE='EC'
        and erp_so_number is not null
       and  product_name = 'Asset Management';"""

    cpq_qry2 = """create or replace transient   table CPS_DSCI_API.tmp_cpq_updates as
    with cpq as (
    select
        case
            when upper(sales_level_1) like  '%APJ%' then 4
            when upper(sales_level_1) like  '%EMEA%' then 3
            when upper(sales_level_1) like  '%AMER%' then 2
            else 1 end as c_booking_theater_id,
        case
            when upper(estimate_sub_type) like '%RENEW%' then 'RENEWAL'
            when upper(estimate_sub_type) like '%UPSE%' then 'UPSELL'
            when upper(estimate_sub_type) like '%NEW%' then 'NEW'
            else 'NOT KNOWN / NEW TYPE'
            end as c_new_renew,
                        cpq_country__C      as c_booked_country,
                        CAM_REVENUE         as c_booked_revenue,
                        cam_cost            as c_CAM_COST,
                        cpq_contract_number as c_booking_contract,
                        quote_name          as  ACCOUNT_NAME,
                        SALES_LEVEL_1,
                        SALES_LEVEL_2,
                        quote_number        as c_audit_to_quote,
                        c2ll                as c_booked_allocation_TOTAL,
                        Q_start_date        as c_agreement_start_date,
                        Q_end_date          as c_agreement_end_date,
                        so.ORACLE_BOOK_DATETIME::date as book_date
                 from CPS_DSCI_BR.cpq_quotes
                left join CPS_DB.CPS_DSCI_EBV.BV_SALES_ORDER_TV so on ( so.BK_SO_NUMBER_INT=ERP_ORDER_NUMBER and current_date between START_TV_DATETIME and END_TV_DATETIME)
    ),
      dups as (select
                    cpq_contract_number , count(0)
                  from  CPS_DSCI_BR.cpq_quotes
                  where cpq_contract_number  is not null
                  group by cpq_contract_number
                  having count(0) > 1
                  )
    select
            cpq.c_booking_contract,c.BOOKING_CONTRACT,
            cpq.c_booking_theater_id,
           cpq.c_new_renew,
           cpq.c_booked_country,
            cpq.c_booked_revenue,
            cpq.c_CAM_COST,
            cpq.c_audit_to_quote,
            cpq.c_booked_allocation_TOTAL,
            cpq.c_agreement_start_date,
            cpq.c_agreement_end_date,
            cpq.ACCOUNT_NAME,
            cpq.SALES_LEVEL_1,
            cpq.SALES_LEVEL_2,
            cpq.book_date
           from cpq
        left join CPS_DSCI_API.DC_BOOKINGS_CONTRACTS c on ( c.BOOKING_CONTRACT=cpq.c_booking_contract and c.IS_DELETED = 'F')
        left join CPS_DSCI_API.DC_THEATER t on (t.THEATER_ID=c.BOOKED_THEATER_ID )
        where cpq.c_booking_contract not in (select dups.CPQ_CONTRACT_NUMBER from dups)
    order by c.booking_contract;"""

    cpq_qry3 = """insert into CPS_DSCI_API.DC_BOOKINGS_CONTRACTS(BOOKING_CONTRACT, ACCOUNT_NAME, BOOKED_SAV_1, BOOKED_SAV_2,  BOOKED_THEATER_ID,
                                                   SOLD_AS_SW_ALLOCATION, SOLD_AS_HW_ALLOCATION,
                                                   CAM_REVENUE_USD, AGREEMENT_START_DATE, AGREEMENT_END_DATE,
                                                   BOOKING_COUNTRY,
                                                   CAM_COST_USD,
                                                   CREATED_BY, CREATE_DTM,
                                                   SOUCED_ALLOCATION,
                                                   quote_for_audit,
                                                   SOURCE_OF_ALLOCATION,
                                                   derived_new_renew, UPDATE_DTM, booked_date,buying_program_type_id)
    select u.c_BOOKING_CONTRACT, ACCOUNT_NAME, SALES_LEVEL_1, SALES_LEVEL_2, c_booking_theater_id, 0 as SW_allocation, c_booked_allocation_TOTAL as HW_ALLOCATION,
           u.c_booked_revenue,   u.C_AGREEMENT_START_DATE,   u.c_AGREEMENT_END_DATE, c_booked_country,C_CAM_COST, 'alanzen@cisco.com', current_timestamp,
           c_booked_allocation_TOTAL, 'CPQ_AUTOMATED_FEED', c_audit_to_quote, c_new_renew, current_timestamp,book_date,-1
           from CPS_DSCI_API.tmp_cpq_updates u where BOOKING_CONTRACT is null
    and  u.c_BOOKING_CONTRACT not in (select booking_contract from CPS_DSCI_API.DC_BOOKINGS_CONTRACTS );"""

    complete1 = con.execute(cpq_qry)

    complete2 = con.execute(cpq_qry2)

    complete3 = con.execute(cpq_qry3)

    return True


def get_src():
    return Path(__file__).parent.parent


storage_obj = Docker(
    base_image="837578041534.dkr.ecr.us-east-1.amazonaws.com/bases/prefect:0.15.13-python3.9",
    registry_url="containers.cisco.com/datacanvas",
    image_name="dc-p1-htec-etl",
    dockerignore=str(get_src() / ".dockerignore"),
    extra_dockerfile_commands=[
        """
        RUN python -m pip install --no-cache-dir -r /tmp/flow_requirements.txt
        """
    ],
    files={
        str(get_src() / "flow_requirements.txt"): "/tmp/flow_requirements.txt",
        str(get_src() / ".dockerignore"): "/tmp/.dockerignore",
        str(get_src() / "dc_p1_htec_etl" / "."): "/opt/dc_p1_htec_etl/",
    },
    path="/opt/dc_p1_htec_etl/main.py",
    env_vars={
        "PYTHONPATH": "${PYTHONPATH}:/opt/",
        "AWS_DEFAULT_REGION": "us-east-1",
    },
    secrets=["AWS_CREDENTIALS"],
    stored_as_script=True,
)


with Flow(
    "dc-p1-htec-etl",
    storage=storage_obj,
    run_config=KubernetesRun(
        labels=["runon", "thought-spot"],
        memory_request="1024Mi",
        memory_limit="2048Mi",
        cpu_request="1000m",
        cpu_limit="2000m",
        service_account_name="builder",
        job_template={
            "apiVersion": "batch/v1",
            "kind": "Job",
            "spec": {
                "template": {
                    "spec": {
                        "ttlSecondsAfterFinished": 300,
                        "containers": [{"name": "flow"}],
                    }
                }
            },
        },
    ),
    result=S3Result(
        bucket="cam-prefect-results",
        boto3_kwargs={
            "credentials": {
                "ACCESS_KEY": aws_sec.ACCESS_KEY,
                "SECRET_ACCESS_KEY": aws_sec.SECRET_KEY,
            }
        },
    ),
) as flow:
    sf_env = "prod"
    with Oracle_Connection() as conn:
        file_loc = pull_HTEC(conn)
        table_created = create_htec_table(file_loc, sf_env, upstream_tasks=[file_loc])
        complete = final_merge_query(sf_env, upstream_tasks=[table_created])

        cxea_complete = do_cxea(sf_env)

        cpq_complete = do_cpq(sf_env)


if __name__ == "__main__":
    flow.run()
