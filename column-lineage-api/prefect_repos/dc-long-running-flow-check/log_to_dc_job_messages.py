
from sqlalchemy import create_engine
from common import sec

temp_base_location = '/mnt/newmt/ERP/home/alanzen/bulk_tmp'

def get_correct_schema(env):
    if env == 'prod':
        return 'CPS_DSCI_API'
    else:
        return 'CPS_DSCI_BR'




def check_env(env):
    print(env)
    if env == "dev":
        cn = "dev_cps_dsci_etl_svc"
    elif env == "stage":
        cn = "stg_cps_dsci_etl_svc"
    elif env == "prod":
        cn = "prd_cps_dsci_etl_svc"
    return cn

def log_to_dc_job_messages(sf_env,request_id, log_message):
    cn = check_env('prod')
    correct_schema = get_correct_schema(sf_env)

    engine = create_engine(
        sec.get_sf_pw(cn, "CPS_DSCI_ETL_EXT1_WH", correct_schema)
    )

    con = engine.connect()


    bia_qry = f"""
    insert into {correct_schema}.dc_job_messages(request_id,logged_message) values ({request_id},'{log_message}')
    """

    try:
        con.execute(bia_qry)
    except Exception as e:
        print(e)
        print(
            f"Failed while attempting to log message to : {correct_schema}.dc_job_messages"
        )

    return True