# import bulkload as bl
from common import sec
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine
import re
import pickle
import os
from pathlib import Path
import string
import awswrangler as wr
import tempfile
from common import file_ops
import s3fs
import json
import boto3
import sys
import psutil

snowflake_db = "CPS_DB"
dn_key_name = "prd_cps_dsci_etl_svc"
schema = "CPS_DSCI_ARCHIVE"
warehouseMed = "cps_dsci_etl_wh"  # Medium
warehouseXsmall = "CPS_DSCI_ETL_EXT1_WH"  # X-Small
warehouseSmall = "CPS_DSCI_ETL_EXT2_WH"  # Small
print("test")





def list_files_and_sizes_aws(bucket, ext):
    file_nfo = []
    for filename in wr.s3.list_objects(bucket, suffix=ext):
        file = f"""{filename.split("/", 20)[-3]}"""
        print(f"file = {file}")
        file_nfo.append([bucket, file])
    if len(file_nfo) > 0:
        return pd.DataFrame(file_nfo, columns=['path', 'file'])


import os

print("test2")
os.environ['ORACLE_HOME'] = "/usr/lib/oracle/12.2/client64"
os.environ[
    'PATH'] = "/home/alanzen/bin:/home/alanzen/.local/bin:/home/alanzen/anaconda3/envs/env_3.7/bin:/home/alanzen/anaconda3/condabin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin:/usr/lib/oracle/12.2/client64/bin:/usr/lib/oracle/12.2/client64/lib"
os.environ['LD_LIBRARY_PATH'] = "/usr/lib/oracle/12.2/client64/lib:/usr/lib/oracle/12.2/client64"


process = psutil.Process(os.getpid())
print(f"this pid : {process}")
print(int(process.memory_info().rss)/1024 ** 2)


def sync_with_gcp(src_dir, remote_dir):
    os.system("gsutil -m rsync -r {} {}".format(src_dir, remote_dir))





def fix_cols(df):
    cols = []
    for c in df.columns:
        cl = c.lower()
        cols.append(cl.strip().replace(' ', '_').replace('/', '_').replace('\\', '_'))
    return cols


print("test3")

remote_store = "s3://canvas-data-store-dev/ACAT_FILES/"


def get_completed_requests(bucket, ext='parquet'):
    engine = create_engine(sec.get_sf_pw(dn_key_name, warehouseXsmall, schema))
    df = pd.read_sql(
        f"""select REQUEST_ID, CUSTOMER_ID from CPS_DSCI_ARCHIVE.ACAT_CANVAS_DATA_SOURCE_META
""",
        engine,
    )
    done_requests = df['request_id'].to_list()

    return done_requests


print("test4")
cr_engine = create_engine(sec.acat_ro_connection, encoding='us-ascii',
                          connect_args={"encoding": "UTF-16", "nencoding": "UTF-16"})

engine = create_engine(sec.get_sf_pw(dn_key_name, warehouseXsmall, schema),
                       connect_args={"encoding": "UTF-16", "nencoding": "UTF-16"})
con = engine.connect()

print("checking if ACAT-src is already running....")
check_acat_lock = f""" select * from CPS_DSCI_ARCHIVE.SOURCING_LOCK where SRC_TYPE = 'ACAT' """
lock_df = pd.read_sql(check_acat_lock, engine)

print(lock_df['currently_processing'][0])

if lock_df['currently_processing'][0] == 'processing':
    sys.exit("ACAT is already running, please wait for it to complete before starting another sourcing batch..")
else:
    print("ACAT is not currently running , updating lock to begin this sourcing batch...")
    update_lock_table = f""" update CPS_DSCI_ARCHIVE.SOURCING_LOCK set CURRENTLY_PROCESSING ='processing' where SRC_TYPE = 'ACAT' """
    print(update_lock_table)
    con.execute(update_lock_table)
    print("CPS_DSCI_ARCHIVE.SOURCING_LOCK has been updated to 'processing' for ACAT... ")




# whats available
print("test5")
done_requests = get_completed_requests(remote_store)

print(done_requests)

to_run = [1543961853049, 1629321041178, 1600875852247, 110, 130, 209, 1595781361642, 1596132701177, 1635927512319,
          151156, 1621275244884, 232, 253, 1595609612059, 1636476390620, 1626782179187, 1614880980726, 1621435080763,
          306, 1568036628283, 202, 266, 1602120692039, 1606217346423, 261, 290, 20759, 129130, 841599, 1625751065069,
          1619776842233, 1600875148948, 1559147091491, 187, 114588, 207, 1557867324754, 123, 285, 161, 1602038762772,
          1597089974694, 142410, 198, 328, 1638346301947, 1628534123243, 1618935127398, 230, 267, 151174, 169174,
          1616422321716, 1548428986394]
print("test6")
sql = f"""SELECT d.request_id, customer_id, customer_name, created_by, to_char(creation_date,'YYYY-MM-DD') as creation_date
        FROM APPS.XXCSS_ACAT_DISCOVERY_SUM D
        WHERE 1=1
        and  REQUEST_TYPE = 'ON-DEMAND'
        and data_purged like 'RETAIN%'
        and total_lines > 0
        and creation_date> to_date('2021-06-01','YYYY-MM-DD')
        order by total_lines
        """
available_data = pd.read_sql(sql, cr_engine)



# only grab what you dont have

print(len(set(available_data.request_id)))
print(len(set(done_requests)))

print(len(list(set(available_data.request_id).difference(set(done_requests)))))

requests_needed = list(set(available_data.request_id).difference(set(done_requests)))

# oracle max is 1000 inlist
len_todo = len(requests_needed)
print(len_todo)
if 995 < len_todo:
    len_todo = 500
print(len_todo)

iter_list = ''
for v in requests_needed[:len_todo]:
    iter_list = '{},{}'.format(v, iter_list)
requests_needed_in_list = '{}'.format(iter_list[:-2])




# now what do i really want to grab
real_sql = """SELECT d.request_id, customer_id, customer_name,
        created_by, to_char(creation_date,'YYYY-MM-DD') as creation_date
        FROM APPS.XXCSS_ACAT_DISCOVERY_SUM D
        WHERE 1=1
        and request_id in ({in_list})
        and  REQUEST_TYPE = 'ON-DEMAND'
        and data_purged like 'RETAIN%'
        and total_lines > 0
        order by total_lines""".format(in_list=requests_needed_in_list)

real_data = pd.read_sql(real_sql, cr_engine)

import re



def clean_name(fld):
    return re.sub('[^0-9a-zA-Z]+', '-', fld)



real_data['customer_name_mod'] = real_data.apply(lambda x: clean_name(x['customer_name']), axis=1)




for i, row in real_data.iterrows():

    try:

        print(f"this pid : {process}")
        print(int(process.memory_info().rss)/1024 ** 2)
        req_id = row.request_id
        customer_id = row.customer_id
        company = row.customer_name_mod
        run_date = row.creation_date
        create_by = row.created_by
        fname = 'ACAT_NO_FILTERS_{c}_{crby}_{cid}_{rid}_{rd}.parquet'.format(crby=create_by, c=company, cid=customer_id,
                                                                             rid=req_id, rd=run_date)

        print(real_data.iloc[i].to_frame())
        this_row_df = real_data.iloc[i].to_frame().T
        this_row_df['file_path'] = str(os.path.join(remote_store, fname))
        this_row_df['src_proc_flag'] = 'processing'
        this_row_df.to_sql("acat_canvas_data_source_meta", con=con, if_exists='append', index=False)

        acat_sql = """SELECT
        sweeps_customer_name,sweeps_customer_number,report_type,exclude_flag,instance_id,instance_number,covered_status,
        serial_number,instance_status_desc,serialized_flag,item_type_flag,parent_instance_number,so_number,deal_id,
        sales_node_6,inventory_item_id,item_name,item_type,mapped_to_service_flag,product_family_description,product_family,
        product_pricing_category,ib_product_type,service_list_price,product_list_price,technology_group,install_at_site_use_id,
        install_party_name,install_address1,install_address2,install_address3,install_address4,install_state_province,install_city,
        install_postal_code,install_country,install_gu_id,install_gu_name,install_parent_party_id,install_parent_party_name,
        bill_to_site_use_id,bill_to_party_name,bill_to_gu_id,bill_to_gu_name,bill_to_parent_party_id,bill_to_parent_party_name,
        ship_to_site_use_id,ship_to_party_name,ship_to_gu_id,ship_to_gu_name,ship_to_parent_party_id,ship_to_parent_party_name,
        ship_to_city,ship_to_state_province,ship_to_country,ship_to_postal_code,covered_line_id,
        sts_code,maintenance_so_number,contract_number,service_line_name,contract_sts_code,contract_bill_to_site_use_id,
        contract_bill_to_customer_name,contract_billto_gu_id,contract_billto_gu_name,contract_bid_parent_party_id,contract_bid_parent_party_name,
        unquotable_instructions_flag,atc_priority,item_name_match,product_family_match,product_pricing_cat_match,install_country_match,
        install_state_province_match,install_city_match,bill_to_site_use_id_match,ship_to_site_use_id_match,ship_to_country_match,
        ship_to_state_province_match,ship_to_city_match,deal_id_match,sales_node_6_match,service_line_name_match,
        target_contract_number,target_service_line_name,target_install_site_id_flag,target_install_at_site_use_id,
        target_contract_bill_to_id,target_deal_id,project_id,aspt_quote_number,created_by,last_updated_by,request_id,acat_line_id,
        acat_request_id,acat_status,acat_approval_flag,cars_request_id,error_message,ldos_flag,services_full_coverage,
        sweeps_flag,holding_contract_flag,soon_to_expire_flag,manual_exclusion_flag,delete_manual_exclusion_flag,reason_code,
        install_at_site_use_id_match,divestiture_flag,msa_flag,duplicate_ib_code,config_covered_flag,rule_number,baseline_asset_status,
        acat_request_type,reference_id,mlb_offer_type,
        proposal_id,
        offer_ato_suite_name,service_billing_sku,subref_id,
        list_price_compute_flag,list_price_protected,
        target_dnr_code,
        delist_flag,install_hq_branch_ind,install_hq_party_id,contract_cxea_flag,
        offer_ato_suite_description,
        case when target_date_terminated       > TO_DATE('{mx_dte}', 'yyyy/mm/dd') then  TO_DATE('{mx_dte}', 'yyyy/mm/dd') when target_date_terminated       < TO_DATE('{min_dte}', 'yyyy/mm/dd') then  TO_DATE('{min_dte}', 'yyyy/mm/dd')else    target_date_terminated       end as target_date_terminated,
        case when invoice_start_date           > TO_DATE('{mx_dte}', 'yyyy/mm/dd') then  TO_DATE('{mx_dte}', 'yyyy/mm/dd') when invoice_start_date           < TO_DATE('{min_dte}', 'yyyy/mm/dd') then  TO_DATE('{min_dte}', 'yyyy/mm/dd')else    invoice_start_date           end as invoice_start_date,
        case when start_date                   > TO_DATE('{mx_dte}', 'yyyy/mm/dd') then  TO_DATE('{mx_dte}', 'yyyy/mm/dd') when start_date                   < TO_DATE('{min_dte}', 'yyyy/mm/dd') then  TO_DATE('{min_dte}', 'yyyy/mm/dd') else    start_date                   end as start_date,
        case when end_date                     > TO_DATE('{mx_dte}', 'yyyy/mm/dd') then  TO_DATE('{mx_dte}', 'yyyy/mm/dd') when end_date                     < TO_DATE('{min_dte}', 'yyyy/mm/dd') then  TO_DATE('{min_dte}', 'yyyy/mm/dd')else    end_date                     end as end_date,
        case when user_atc_coverage_start_date > TO_DATE('{mx_dte}', 'yyyy/mm/dd') then  TO_DATE('{mx_dte}', 'yyyy/mm/dd') when user_atc_coverage_start_date < TO_DATE('{min_dte}', 'yyyy/mm/dd') then  TO_DATE('{min_dte}', 'yyyy/mm/dd')else    user_atc_coverage_start_date end as user_atc_coverage_start_date,
        case when atc_coverage_end_date        > TO_DATE('{mx_dte}', 'yyyy/mm/dd') then  TO_DATE('{mx_dte}', 'yyyy/mm/dd') when atc_coverage_end_date        < TO_DATE('{min_dte}', 'yyyy/mm/dd') then  TO_DATE('{min_dte}', 'yyyy/mm/dd')else    atc_coverage_end_date        end as atc_coverage_end_date,
        case when ship_date                    > TO_DATE('{mx_dte}', 'yyyy/mm/dd') then  TO_DATE('{mx_dte}', 'yyyy/mm/dd') when ship_date                    < TO_DATE('{min_dte}', 'yyyy/mm/dd') then  TO_DATE('{min_dte}', 'yyyy/mm/dd')else    ship_date                    end as ship_date,
        case when atc_coverage_start_date      > TO_DATE('{mx_dte}', 'yyyy/mm/dd') then  TO_DATE('{mx_dte}', 'yyyy/mm/dd') when atc_coverage_start_date      < TO_DATE('{min_dte}', 'yyyy/mm/dd') then  TO_DATE('{min_dte}', 'yyyy/mm/dd')else    atc_coverage_start_date      end as atc_coverage_start_date,
        case when earliest_discovery_date      > TO_DATE('{mx_dte}', 'yyyy/mm/dd') then  TO_DATE('{mx_dte}', 'yyyy/mm/dd') when earliest_discovery_date      < TO_DATE('{min_dte}', 'yyyy/mm/dd') then  TO_DATE('{min_dte}', 'yyyy/mm/dd')else    earliest_discovery_date      end as earliest_discovery_date,
        case when instance_cvg_max_end_date    > TO_DATE('{mx_dte}', 'yyyy/mm/dd') then  TO_DATE('{mx_dte}', 'yyyy/mm/dd') when instance_cvg_max_end_date    < TO_DATE('{min_dte}', 'yyyy/mm/dd') then  TO_DATE('{min_dte}', 'yyyy/mm/dd')else    instance_cvg_max_end_date    end as instance_cvg_max_end_date,
        case when last_update_date             > TO_DATE('{mx_dte}', 'yyyy/mm/dd') then  TO_DATE('{mx_dte}', 'yyyy/mm/dd') when last_update_date             < TO_DATE('{min_dte}', 'yyyy/mm/dd') then  TO_DATE('{min_dte}', 'yyyy/mm/dd')else    last_update_date             end as last_update_date,
        case when creation_date                > TO_DATE('{mx_dte}', 'yyyy/mm/dd') then  TO_DATE('{mx_dte}', 'yyyy/mm/dd') when creation_date                < TO_DATE('{min_dte}', 'yyyy/mm/dd') then  TO_DATE('{min_dte}', 'yyyy/mm/dd')else    creation_date                end as creation_date,
        case when last_date_of_support         > TO_DATE('{mx_dte}', 'yyyy/mm/dd') then  TO_DATE('{mx_dte}', 'yyyy/mm/dd') when last_date_of_support         < TO_DATE('{min_dte}', 'yyyy/mm/dd') then  TO_DATE('{min_dte}', 'yyyy/mm/dd')else    last_date_of_support         end as last_date_of_support,
        case when instance_creation_date       > TO_DATE('{mx_dte}', 'yyyy/mm/dd') then  TO_DATE('{mx_dte}', 'yyyy/mm/dd') when instance_creation_date       < TO_DATE('{min_dte}', 'yyyy/mm/dd') then  TO_DATE('{min_dte}', 'yyyy/mm/dd')else    instance_creation_date       end as instance_creation_date,
        case when latest_cvg_date_terminated   > TO_DATE('{mx_dte}', 'yyyy/mm/dd') then  TO_DATE('{mx_dte}', 'yyyy/mm/dd') when latest_cvg_date_terminated   < TO_DATE('{min_dte}', 'yyyy/mm/dd') then  TO_DATE('{min_dte}', 'yyyy/mm/dd')else    latest_cvg_date_terminated   end as latest_cvg_date_terminated,
        case when ACAT_STATUS IN ('PENDING','MISSING-ATC','APPROVED','UNAPPROVED') then 1 else 0 end as OPPORTUNITY_NOT_EXCLUDED,
        case when COVERED_STATUS = 'A' AND EXCLUDE_FLAG  <> 'Y' AND MANUAL_EXCLUSION_FLAG = 'A' then 1 else 0 end as COVERED_IN_SCOPE_DATA
            FROM APPS.XXCSS_ACAT_DISCOVERY_DATA
            WHERE ACAT_REQUEST_ID ={req_id}
            """.format(req_id=req_id, mx_dte='2262/01/01', min_dte='1677/09/22')

        print(acat_sql)


        acat = pd.read_sql(acat_sql, cr_engine)

        acat.columns = fix_cols(acat)


        with tempfile.TemporaryDirectory() as temp_dir:
            acat.to_parquet(os.path.join(temp_dir, fname), engine='pyarrow', coerce_timestamps='ms',
                            compression='snappy', index=False)

            s3_path = f"""s3://canvas-data-store-dev/ACAT_FILES/{fname}"""
            s3 = s3fs.S3FileSystem()

            s3.put(os.path.join(temp_dir, fname), s3_path, recursive=True)

            print(f"""wrote parquet to {s3_path}""")




        update_acat_source_table = f""" update CPS_DSCI_ARCHIVE.ACAT_CANVAS_DATA_SOURCE_META set SRC_PROC_FLAG ='sourced' where REQUEST_ID = {req_id} """
        print(update_acat_source_table)
        con.execute(update_acat_source_table)

        print(req_id, i)
    except Exception as err:
        print(Exception, err)

print("Updating CPS_DSCI_ARCHIVE.SOURCING_LOCK  complete this sourcing batch...")
update_lock_table = f""" update CPS_DSCI_ARCHIVE.SOURCING_LOCK set CURRENTLY_PROCESSING ='' where SRC_TYPE = 'ACAT' """
print(update_lock_table)
con.execute(update_lock_table)
print("CPS_DSCI_ARCHIVE.SOURCING_LOCK has been updated to '' for ACAT... ")