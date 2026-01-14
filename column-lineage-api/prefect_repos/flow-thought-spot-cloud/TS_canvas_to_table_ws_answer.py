import argparse
import json
import os
import re
import sys
import requests
from prefect.engine.signals import FAIL
import datetime as dt
from datetime import datetime
from datetime import date as day
import time
import ast
from deepdiff import DeepDiff
from prefect.tasks.secrets import PrefectSecret
from prefect.triggers import all_successful, all_failed, all_finished
from thoughtspot import ThoughtSpot, MetadataNames
from thoughtspot_tml import *
from prefect.engine.signals import SKIP
import pandas as pd
import json
import boto3
from my_sec import my_sec
from prefect import Flow, Parameter, task, case
from sqlalchemy import create_engine
from common import sec
import psutil
import oyaml
import awswrangler as wr
from tag_refresh_to_ts_table import refresh_tags_and_create_ts_table, check_env
from TS_delete_from_table import get_dependent_objects_for_table_name, get_dependent_objects_guid_map, get_table_guid

temp_base_location = "/tmp"
from prefect.engine.results.s3_result import S3Result
from prefect.executors.dask import LocalDaskExecutor
from prefect.run_configs.docker import DockerRun
from prefect.storage import Docker

schema = "CPS_DSCI_ARCHIVE"
warehouseMed = "cps_dsci_etl_wh"  # Medium
warehouseXsmall = "CPS_DSCI_ETL_EXT1_WH"  # X-Small
warehouseSmall = "CPS_DSCI_ETL_EXT2_WH"  # Small


# The connection_fetch_live_columns() REST API returns a response with column information for a given table
# pulled through JDBC connection in ThoughtSpot. This function iterates through an creates a List of column dictionaries
# to be added to a Table object using Table.add_columns
def create_tml_table_columns_from_rest_api_response(table_obj, rest_api_columns):
    new_columns = []
    for t in rest_api_columns:
        for c in rest_api_columns[t]:
            col_name = c['name']
            col_data_type = c['type']
            col_type = 'ATTRIBUTE'
            # Simple algorithm for making numeric columns MEASURE by default
            if col_data_type in ['DOUBLE', 'INT64']:
                col_type = 'MEASURE'
            new_column = table_obj.create_column(column_display_name=col_name,
                                                 db_column_name=col_name,
                                                 column_data_type=col_data_type, column_type=col_type)
            new_columns.append(new_column)
    return new_columns


@task(log_stdout=True)
def log_in_to_thoughtspot(ts_env):
    password = 's'
    username = 'generic_user'  # or type in yourself
    if ts_env == 'dev':
        password = my_sec.admin_pass
        server = 'https://cisco.thoughtspot.cloud/'
    elif ts_env == "prod":
        password = my_sec.admin_pass
        server = 'https://cisco.thoughtspot.cloud/'
    else:
        print("Please select a valid thoughtspot environment : dev, prod ")

    ts: ThoughtSpot = ThoughtSpot(server_url=server)
    try:
        print("trying to log in ")
        ts.login(username=username, password=password)
    except requests.exceptions.HTTPError as e:
        print("log in failed")
        print(e)
        print(e.response.content)
    password = 's'

    return ts


@task(log_stdout=True, max_retries=4, retry_delay=dt.timedelta(seconds=10),tags=["snowflake_xsmall"])
def get_canvas_meta_data(canvas_name, sf_env, action, request_id):
    client = boto3.client(
        "s3",
        aws_access_key_id=my_sec.ACCESS_KEY,
        aws_secret_access_key=my_sec.SECRET_KEY
    )
    sf_env = check_env(sf_env)
    s3 = boto3.resource('s3')
    qry = f"""select CAMCECID, FILE_PATH , UID as engagement_id from CPS_BIA_BR.DATA_CANVAS_HDR where CANVAS_ID = '{canvas_name}'"""
    # for testing
    # qry = f"""select CAMCECID, FILE_PATH , UID as engagement_id from CPS_BIA_BR.DATA_CANVAS_HDR where CANVAS_ID = 'CANVAS-188'"""
    print(qry)
    engine = create_engine(
        sec.get_sf_pw(sf_env, warehouseXsmall, sf_env)
    )
    df = pd.read_sql(qry, engine)
    print(df)

    ###################  Need to add to its own function .

    # sf_env = check_env(sf_env)
    # bia_engine = create_engine(
    #     sec.get_sf_pw(sf_env, warehouseXsmall, sf_env)
    # )
    bia_con = engine.connect()

    # # check if create is running, will need to convert to its own function later
    # check_if_running_qry = f"""
    # select * from CPS_DB.CPS_BIA_BR.DATA_CANVAS_TS_REPORTING
    # where REQUEST_ID  = '{int(request_id)}' and ( STATUS = 'InProgress')  and  ACTION = 'Create';
    # """
    # check_if_running_df = pd.read_sql(check_if_running_qry, engine)
    #
    # if check_if_running_df.empty:
    bia_qry = f"""
    UPDATE CPS_DB.CPS_BIA_BR.DATA_CANVAS_TS_REPORTING set STATUS = 'InProgress'
    where REQUEST_ID  = '{int(request_id)}' and  ACTION = '{action}';
    """

    try:
        bia_con.execute(bia_qry)
    except Exception as e:
        print(e)
        print("Data for this REQUEST_ID  is not in CPS_DB.CPS_BIA_BR.DATA_CANVAS_TS_REPORTING")
        pass


    ##########################
    return df


@task(log_stdout=True, max_retries=4, retry_delay=dt.timedelta(seconds=20))
def create_table_obj(cn_name, db_name, schema, db_table, connection_guid, ts):
    client = boto3.client(
        "s3",
        aws_access_key_id=my_sec.ACCESS_KEY,
        aws_secret_access_key=my_sec.SECRET_KEY
    )

    s3 = boto3.resource('s3')
    # Create the YAML string for the table with the desired properties
    table_tml_start = Table.generate_tml_from_scratch(connection_name=cn_name,
                                                      db_name=db_name,
                                                      schema=schema,
                                                      db_table=db_table,
                                                      table_name=db_table)  # pretty
    # print(table_tml_start)

    # TML objects are created from an OrderedDict, so this converts from raw YAML string to that OrderedDict
    yaml_ordereddict = YAMLTML.load_string(table_tml_start)
    table_obj = Table(yaml_ordereddict)

    # metadata/details provides the connection_config needed for the connection/create and connection/update commands
    connection_details = ts.tsrest.metadata_details(object_type=MetadataNames.CONNECTION,
                                                    object_guids=[connection_guid])

    # These helper functions parse out the parts you need from very complex connection_details object
    connection_config = ts.tsrest.get_connection_config_from_metadata_details(connection_details)
    connection_name = ts.tsrest.get_connection_name_from_metadata_details(connection_details)
    connection_type = ts.tsrest.get_connection_type_from_metadata_details(connection_details)

    # connection_fetch_live_columns retrieves all columns and types for a table via ThoughtSpot's JDBC connection
    columns = ts.tsrest.connection_fetch_live_columns(connection_guid=connection_guid,
                                                      config_json_string=json.dumps(connection_config),
                                                      database_name=db_name, schema_name=schema, table_name=db_table)

    # print(columns)

    tml_columns_dict_list = create_tml_table_columns_from_rest_api_response(table_obj, rest_api_columns=columns)
    table_obj.add_columns(tml_columns_dict_list)

    # Function parses the columns REST API response from above into the format for the Table.add_columns() method
    final_table_yaml_str = YAMLTML.dump_tml_object(table_obj)
    # print(final_table_yaml_str)

    final_table_filename = 'table_output_from_csv.table.tml'
    with open(final_table_filename, 'w') as fh:
        fh.write(final_table_yaml_str)

    # print(table_obj.tml)
    return table_obj


@task(log_stdout=True)
def create_ws_from_table(new_table_guid, new_worksheet_name, ts, db_table):
    # client = boto3.client(
    #     "s3",
    #     aws_access_key_id=my_sec.ACCESS_KEY,
    #     aws_secret_access_key=my_sec.SECRET_KEY
    # )

    session = boto3.Session(
        aws_access_key_id=my_sec.ACCESS_KEY,
        aws_secret_access_key=my_sec.SECRET_KEY
    )

    s3 = session.resource('s3')
    # get a valid table from ThoughtSpot as TML
    # Here we get the table object we publsihed earlier
    table_resp = ts.tsrest.metadata_tml_export(guid=new_table_guid)
    table_tml_obj = Table(table_resp)

    # Exports the necessary YAML string to create a Worksheet object
    ws_start = Worksheet.generate_tml_from_scratch(worksheet_name=new_worksheet_name,
                                                   table_name=table_tml_obj.content_name)
    # Build the Worksheet object from the initial YAML string
    ws_obj = Worksheet(YAMLTML.load_string(ws_start))
    # print(ws_start)

    # Automatically build the columns based on the columns in the table, then add to the Worksheet (starts with no columns)
    new_ws_cols = create_worksheet_columns_from_table_object(table_obj=table_tml_obj)
    ws_obj.add_worksheet_columns(new_ws_cols)

    # Add the Table GUID reference to make sure it connects without issue
    # We're not really remapping here, just swapping in the GUID instead of the name of the same Table object
    print("NEW TABLE GUID ______________")
    print(new_table_guid)

    ws_obj.remap_tables_to_new_fqn({db_table: new_table_guid})

    ###### code for updating the properties field from the master data dict json

    bucket = 'canvas-data-types'
    key = 'dev_properties_map.json'  # testing , needs to be moved to prod in actaul table when ready

    obj = s3.Object(bucket, key)
    data = obj.get()['Body'].read().decode('utf-8')

    json_data = oyaml.safe_load(data)

    for key in json_data:
        for col in ws_obj.tml['worksheet']['worksheet_columns']:
            if col['name'] == key.upper():
                col['properties'] = ast.literal_eval(json_data[key])

    print(ws_obj)
    return ws_obj


# Creating the table
@task(log_stdout=True, max_retries=7, retry_delay=dt.timedelta(seconds=20), nout=2)
def push_tml_to_thought_spot(tml_objs, create_new_on_server, validate_only, ts):
    """
    :param tml_objs: list of tml_objects
    :param create_new_on_server: boolean
    :param validate_only: boolean
    :param ts: thought spot connection
    :return: list of guids or single guid for created objects
    """
    session = boto3.Session(
        aws_access_key_id=my_sec.ACCESS_KEY,
        aws_secret_access_key=my_sec.SECRET_KEY
    )

    s3 = session.resource('s3')
    out_list = []
    new_guid_list = []

    for obj in tml_objs:
        print("OBJECT ********************************")
        print(obj)
        try:
            import_response = ts.tml.import_tml(tml=obj.tml, create_new_on_server=create_new_on_server,
                                                validate_only=validate_only)
            new_guid = get_guid_from_response(import_response, ts)
            new_guid_list.append(new_guid)


        except SyntaxError as e:
            print('TML import encountered error:')
            print(e)
            out_list.append(tml_objs[obj])

    # time.sleep(120)  # Sleep for 120 seconds
    if len(new_guid_list) == 1:
        return new_guid_list[0]
    else:
        return new_guid_list


def does_response_have_errors(j):
    if 'object' in j:
        print("object in j")
        for k in j['object']:
            print(k)
            if 'response' in k:
                if k['response']['status']['status_code'] == 'ERROR':
                    return True
                else:
                    return False
            else:
                return False


@task(log_stdout=True, max_retries=6, retry_delay=dt.timedelta(seconds=20), nout=2)
def validate_tmls(list_of_tmls, ts):

    # time.sleep(120)
    session = boto3.Session(
        aws_access_key_id=my_sec.ACCESS_KEY,
        aws_secret_access_key=my_sec.SECRET_KEY
    )

    s3 = session.resource('s3')
    out_dict = {}
    in_list = []
    for obj in list_of_tmls:
        try:
            import_response = ts.tml.import_tml(tml=obj.tml, create_new_on_server=False, validate_only=True)
            error_check = does_response_have_errors(import_response)
            if error_check == False:
                in_list.append(obj)
            else:
                out_dict[list_of_tmls[obj]] = import_response  # if object doesnt validate, add object to outlist

        except SyntaxError as e:
            print('TML import encountered error:')
            print(e)
            if "Invalid data source guid:" in str(e):
                raise ValueError("syntax error")
            else:
                out_dict[list_of_tmls[obj]] = e

    return in_list, out_dict


# Get the GUID from the newly created object
# @task(log_stdout=True, max_retries=4, retry_delay=dt.timedelta(seconds=10))
def get_guid_from_response(import_response, ts):
    new_guids = ts.tsrest.guids_from_imported_tml(import_response)
    new_guid = new_guids[0]
    print(new_guid)
    return new_guid


@task(log_stdout=True)
def share_content(username_to_share, new_table_guid, new_ws_guid, new_answer_guids, new_pinboard_guids, ts):
    if "," in username_to_share:
        username_to_share = username_to_share.split(",")
    if isinstance(username_to_share, list):
        list_of_usr_guid_to_share = []
        for name in username_to_share:
            username_to_share = f"""{name}@cisco.com"""
            try:
                user_guid = ts.user.find_guid(username_to_share)
                list_of_usr_guid_to_share.append(user_guid)
            except:
                print(
                    f"Sharing: The user id :{username_to_share} is not a Thought Spot User, please create an account and retry")

    else:
        username_to_share = f"""{username_to_share}@cisco.com"""
        # Get the GUIDs for users or groups you want to share the content to
        # group_guid = ts.group.find_guid(group_name_to_share)
        try:
            list_of_usr_guid_to_share = [ts.user.find_guid(username_to_share)]
        except:
            print(
                f"Sharing: The user id :{username_to_share} is not a Thought Spot User, please create an account and retry")
            return True

    # Create the Share structure
    view_perms = ts.table.create_share_permissions(read_only_users_or_groups_guids=list_of_usr_guid_to_share)
    # Share the object

    edit_perms = ts.table.create_share_permissions(edit_access_users_or_groups_guids=list_of_usr_guid_to_share)

    print('sharing')
    print(username_to_share)
    print(list_of_usr_guid_to_share)
    # print(new_table_guid)
    # print(new_answer_guids)
    # print(new_pinboard_guids)
    # print(new_ws_guid)

    ts.table.share([new_table_guid], view_perms)
    time.sleep(3)
    ts.worksheet.share([new_ws_guid], edit_perms)
    time.sleep(3)
    if type(new_answer_guids) == list:
        ts.answer.share(new_answer_guids, edit_perms)
    else:
        ts.answer.share([new_answer_guids], edit_perms)
    time.sleep(3)
    if type(new_pinboard_guids) == list:
        ts.pinboard.share(new_pinboard_guids, edit_perms)
    else:
        ts.pinboard.share([new_pinboard_guids], edit_perms)

    ts.pinboard.share(['88078eb4-602e-4ef5-9bd8-561060b59cc5'], view_perms)  # sharing landing page guid
    return True
    # ts.logout()


def get_core_file_list(s3Path):
    fls = wr.s3.list_objects(s3Path)

    thinned = []
    for f in fls:
        # print(f)
        if f.endswith('.parquet'):
            thinned.append(f)
    # print(f"thinned total files : {len(thinned)}")
    print(f"thinned CORE FILES: {thinned}")

    return thinned


def get_ts_tml_objects_from_s3(file_path, file_type):
    print(file_path)
    session = boto3.Session(
        aws_access_key_id=my_sec.ACCESS_KEY,
        aws_secret_access_key=my_sec.SECRET_KEY
    )

    s3 = session.resource('s3')
    bucket = 'thought.spot.tml'
    key = "/".join(
        file_path.split("/")[
        3:])  # get the key from the s3 path , this could break if s3 path changes, TODO: make this better
    print(key)
    try:
        obj = s3.Object(bucket, key)
        data = obj.get()['Body'].read().decode('utf-8')
        json_data = oyaml.safe_load(data)
        if file_type == "answer":
            tml_try = Answer(json_data)
        if file_type == "pinboard":
            tml_try = Liveboard(json_data)
    except:
        print(f"{key} failed to import from S3, please check verify that the tml is correctly formed")
    return tml_try


# Grabbing a template tml from s3
# TODO CONVERT THIS TO WORK FOR PIN BOARDS AND WORKSHEETS
@task(log_stdout=True, max_retries=4, retry_delay=dt.timedelta(seconds=5), nout=4)
def get_templates_from_s3(converted_canvas_name, engagement_id, ts_env):
    session = boto3.Session(
        aws_access_key_id=my_sec.ACCESS_KEY,
        aws_secret_access_key=my_sec.SECRET_KEY
    )

    # s3 = session.resource('s3')
    eng_folder = f"""eng_{engagement_id}"""

    # get answers for this engagement
    tml_path = f"""s3://thought.spot.tml/{ts_env}/{eng_folder}/answer_"""  # awswrangler treats answer_ as a wildcard selection
    # for testing
    # tml_path = f"""s3://thought.spot.tml/{ts_env}/eng_1234/answer_"""
    eng_answer_tmls = wr.s3.list_objects(tml_path, suffix='.tml', boto3_session=session)
    print(eng_answer_tmls)

    common_tml_path = f"""s3://thought.spot.tml/{ts_env}/common/answer_"""
    common_answer_tmls = wr.s3.list_objects(common_tml_path, suffix='.tml', boto3_session=session)
    print(common_answer_tmls)

    # get pinboards for this engaement
    # pinboard_tml_path = f"""s3://thought.spot.tml/{ts_env}/eng_1234/pinboard_"""
    pinboard_tml_path = f"""s3://thought.spot.tml/{ts_env}/{eng_folder}/pinboard_"""
    eng_pinboard_tmls = wr.s3.list_objects(pinboard_tml_path, suffix='.tml', boto3_session=session)
    print(eng_pinboard_tmls)
    # get common pinboards
    common_pinboard_tml_path = f"""s3://thought.spot.tml/{ts_env}/common/pinboard_"""
    common_pinboard_tmls = wr.s3.list_objects(common_pinboard_tml_path, suffix='.tml', boto3_session=session)
    print(common_pinboard_tmls)

    list_of_answers = []
    for f in eng_answer_tmls:
        tml_try = get_ts_tml_objects_from_s3(f, 'answer')
        list_of_answers.append(tml_try)

    for f in common_answer_tmls:
        tml_try = get_ts_tml_objects_from_s3(f, 'answer')
        list_of_answers.append(tml_try)

    list_of_pinboards = []
    for f in eng_pinboard_tmls:
        tml_try = get_ts_tml_objects_from_s3(f, 'pinboard')
        list_of_pinboards.append(tml_try)

    for f in common_pinboard_tmls:
        tml_try = get_ts_tml_objects_from_s3(f, 'pinboard')
        list_of_pinboards.append(tml_try)

    eng_answer_tmls.extend(common_answer_tmls)  # concat the two answer lists to be able to zip to the list of answers

    all_answer_locs = dict(zip(list_of_answers, eng_answer_tmls))

    for key in all_answer_locs:
        print(key, ":", all_answer_locs[key])

    eng_pinboard_tmls.extend(
        common_pinboard_tmls)  # concat the two answer lists to be able to zip to the list of answers

    all_pinboard_locs = dict(zip(list_of_pinboards, eng_pinboard_tmls))

    for key in all_pinboard_locs:
        print(key, ":", all_pinboard_locs[key])

    return list_of_answers, list_of_pinboards, all_answer_locs, all_pinboard_locs


def create_worksheet_columns_from_table_object(table_obj: Table, ws_table_path_id: str = None):
    if ws_table_path_id is None:
        # Default is just to add "_1" to the table name
        ws_table_path_id = table_obj.content_name + "_1"
    table_cols = table_obj.columns
    ws_cols = []
    for c in table_cols:
        # print(c)
        if 'index' in c['properties']:
            index_type = c['properties']['index']
        else:
            index_type = 'DEFAULT'
        new_ws_col = Worksheet.create_worksheet_column(column_display_name=c['name'], ws_table_path_id=ws_table_path_id,
                                                       table_column_name=c['name'],
                                                       column_type=c['properties']['column_type'],
                                                       index_type=index_type)
        ws_cols.append(new_ws_col)
    return ws_cols


@task(log_stdout=True)
def convert_answer_template_to_current(tml_dict, converted_canvas_name, new_worksheet_name, new_ws_guid):
    for tml in tml_dict:
        tml.remove_guid()
        # Linking the template answer to the new worksheet
        tml.replace_worksheet(new_worksheet_name=new_worksheet_name, new_worksheet_guid_for_fqn=new_ws_guid)

        # tml.content_name = f'''{tml.content_name}_for_{converted_canvas_name}'''
        print(tml.tml)

    return tml_dict


@task(log_stdout=True)
def convert_pinboard_template_to_current(tml_dict, new_worksheet_name, new_ws_guid):
    for tml in tml_dict:
        try:
            tml.replace_worksheet_on_all_visualizations(new_worksheet_name, new_ws_guid)
        except:
            print("your pinboard is malformed")

    return tml_dict


@task(log_stdout=True, nout=5)
def get_created_params(canvas_name, canvas_meta_df):
    converted_canvas_name = canvas_name.replace("-", '_')
    db_table = f'''{converted_canvas_name}_THOUGHT_SPOT'''.upper() #testing
    # db_table =  "CANVAS_1888_THOUGHT_SPOT" # for testing
    new_worksheet_name = f'''ws_{canvas_name}'''.lower()
    engagement_id = canvas_meta_df['engagement_id'][0]
    # canvas_creator = canvas_meta_df['camcecid'][0] #comment out for testing
    # canvas_creator = 'ejurotic'
    canvas_parquet_path = canvas_meta_df['file_path'][0]
    if canvas_parquet_path.endswith('.xlsx'):
        canvas_parquet_path = "/".join(canvas_parquet_path.split("/")[
                                       :-1])  # get the key from the s3 path , this could break if s3 path changes, TODO: make this better
        canvas_parquet_path = canvas_parquet_path + "/"
    print(canvas_parquet_path)

    return converted_canvas_name, db_table, new_worksheet_name, engagement_id, canvas_parquet_path


@task(log_stdout=True, max_retries=4, retry_delay=dt.timedelta(seconds=5),tags=["snowflake_xsmall"])
def update_canvas_to_ts_log_table(new_table_guid, new_ws_guid, new_answer_guids, pin_board_guids, canvas_meta_df,
                                  db_table, new_worksheet_name, request_id, sf_env):
    # update a table with guids for table, ws, list for answers, pin boards
    session = boto3.Session(
        aws_access_key_id=my_sec.ACCESS_KEY,
        aws_secret_access_key=my_sec.SECRET_KEY
    )

    s3 = session.resource('s3')
    user_id = canvas_meta_df['camcecid'][0]
    date_created = datetime.now().date().isoformat()
    sf_env = check_env(sf_env)
    engine = create_engine(
        sec.get_sf_pw('prd_cps_dsci_etl_svc', warehouseXsmall, 'prd_cps_dsci_etl_svc')
    )
    con = engine.connect()

    creation_data_dict = {'ts_request': request_id,  # this will come from sandeeps team
                          'ts_table_name': db_table,
                          'ts_table_guid': new_table_guid,
                          'ts_worksheet_name': new_worksheet_name,
                          'ts_asnwer_guids': new_answer_guids,
                          'ts_pin_guids': pin_board_guids
                          }

    creation_data_json = json.dumps(creation_data_dict)

    meta_qry = f"""
        INSERT INTO CPS_DB.CPS_DSCI_API.TS_CREATION_LOGGING (
                                                        CREATED_BY ,
                                                        CREATION_DATA,
                                                        STATUS,
                                                        DATE_UPDATED
                                                                    )
        SELECT 
        '{user_id}',
         parse_json($${creation_data_json}$$),
         'Success',
        '{date_created}'
        ;
    """

    print(meta_qry)
    try:
        con.execute(meta_qry)
    except Exception as e:
        print(e)
        print("Data for this canvas_id is not in CPS_DB.CPS_BIA_BR.DATA_CANVAS_HDR")
        pass

    return True


@task(log_stdout=True, trigger=all_finished,tags=["snowflake_xsmall"])
def log_to_bia_table(sf_env, request_id, action, canvas_name):
    landing_page_link = f"""https://cisco.thoughtspot.cloud/?col1=DASHBOARD%20NAME&op1=EQ&val1={canvas_name}#/pinboard/88078eb4-602e-4ef5-9bd8-561060b59cc5/"""
    sf_env = check_env(sf_env)
    bia_engine = create_engine(
        sec.get_sf_pw(sf_env, warehouseXsmall, sf_env)
    )
    bia_con = bia_engine.connect()
    print(landing_page_link)

    bia_qry = f"""
    UPDATE CPS_DB.CPS_BIA_BR.DATA_CANVAS_TS_REPORTING set STATUS = 'Completed',
                                                TS_PINBOARD_URL = '{landing_page_link}'
    where REQUEST_ID  = '{int(request_id)}' and  ACTION = '{action}';
    """

    try:
        bia_con.execute(bia_qry)
    except Exception as e:
        print(e)
        print("Data for this REQUEST_ID  is not in CPS_DB.CPS_BIA_BR.DATA_CANVAS_TS_REPORTING")
        pass


@task(log_stdout=True,tags=["snowflake_xsmall"])
def log_invalidated_tmls(invalidated_answer_tmls,
                         invalidated_pin_board_guids,
                         new_table_guid,
                         new_ws_guid,
                         canvas_meta_df,
                         db_table,
                         new_worksheet_name,
                         request_id,
                         sf_env):
    session = boto3.Session(
        aws_access_key_id=my_sec.ACCESS_KEY,
        aws_secret_access_key=my_sec.SECRET_KEY
    )

    s3 = session.resource('s3')
    # update a table with guids for table, ws, list for answers, pin boards
    user_id = canvas_meta_df['camcecid'][0]
    date_created = datetime.now().date().isoformat()
    sf_env = check_env(sf_env)
    engine = create_engine(
        sec.get_sf_pw('prd_cps_dsci_etl_svc', warehouseXsmall, 'prd_cps_dsci_etl_svc')
    )
    con = engine.connect()

    creation_data_dict = {'ts_request': request_id,  # this will come from sandeeps team
                          'ts_table_name': db_table,
                          'ts_worksheet_name': new_worksheet_name,
                          'failed_ts_asnwers': str(invalidated_answer_tmls),
                          'failed_ts_pins': str(invalidated_pin_board_guids)
                          }

    creation_data_json = json.dumps(creation_data_dict)

    meta_qry = f"""
        INSERT INTO CPS_DB.CPS_DSCI_API.TS_INVALIDATED_TML_LOGGING (
                                                        CREATED_BY ,
                                                        CREATION_DATA,
                                                        STATUS,
                                                        DATE_UPDATED
                                                                    )
        SELECT 
        '{user_id}',
         parse_json($${creation_data_json}$$),
         'Success',
        '{date_created}'
        ;
    """

    print(meta_qry)
    try:
        con.execute(meta_qry)
    except Exception as e:
        print(e)
        print("Failed to log to the TS_INVALIDATED_TML_LOGGING table")
        pass

    print("INVALIDATED TMLS")
    print(invalidated_answer_tmls)
    return True


@task(trigger=all_finished)
def log_out_of_ts(ts):
    ts.logout()
    print("Logged out of Thought Spot...")
    return True


@task(log_stdout=True,tags=["snowflake_xsmall"])
def create_landing_page(new_answer_guids, new_pinboard_guids, new_ws_guid, new_table_guid, request_by, canvas_name, ts):
    guid_list = []
    guid_list.append(
        ('table', request_by, canvas_name, f"""https://cisco.thoughtspot.cloud/#/data/tables/{new_table_guid}"""))

    guid_list.append(
        ('worksheet', request_by, canvas_name, f"""https://cisco.thoughtspot.cloud/#/data/tables/{new_ws_guid}"""))
    print('new_answer_guids')
    print(new_answer_guids)

    if type(new_answer_guids) == 'list' and len(new_answer_guids) > 0:
        for guid in new_answer_guids:
            print(f"https://cisco.thoughtspot.cloud/#/saved-answer/{guid}")
            print('answer guid')
            print(guid)
            print('answer guid[0]')
            print(guid[0])
            answer_tml = ts.tml.download_tml(guid)
            answer_name = answer_tml['answer']['name']
            guid_list.append(
                (answer_name, request_by, canvas_name, f"https://cisco.thoughtspot.cloud/#/saved-answer/{guid}"))
    # else:
    #     answer_tml = ts.tml.download_tml(new_answer_guids)
    #     answer_name = answer_tml['answer']['name']
    #     guid_list.append((answer_name, request_by, canvas_name,
    #                       f"https://thoughtspot.cisco.com/#/saved-answer/{new_answer_guids}"))

    print("new_pinboard_guids")
    print(new_pinboard_guids)
    print(type(new_pinboard_guids))

    if isinstance(new_pinboard_guids, list) and (len(new_pinboard_guids) > 0):
        for pinboard in new_pinboard_guids:
            print(f"https://cisco.thoughtspot.cloud/#/pinboard/{pinboard}/")
            print('pinboard guid')
            print(pinboard)
            print('pinboard guid[0]')
            print(pinboard[0])
            pinboard_tml = ts.tml.download_tml(pinboard)
            pinboard_name = pinboard_tml['pinboard']['name']
            guid_list.append(
                (pinboard_name, request_by, canvas_name, f"https://cisco.thoughtspot.cloud/#/pinboard/{pinboard}/"))

    elif isinstance(new_pinboard_guids, str):
        pinboard_guid = new_pinboard_guids
        print("******************************")
        print(pinboard_guid)
        print("******************************")
        pinboard_tml = ts.tml.download_tml(pinboard_guid)
        pinboard_name = pinboard_tml['pinboard']['name']
        guid_list.append(
            (pinboard_name, request_by, canvas_name, f"https://cisco.thoughtspot.cloud/#/pinboard/{pinboard_guid}/"))


    df = pd.DataFrame(guid_list, columns=['PINBOARD_NAME', 'USER', 'DASHBOARD_NAME', 'LINK'])

    print(df.head())
    engine = create_engine(
        sec.get_sf_pw('prd_cps_dsci_etl_svc', 'CPS_DSCI_ETL_EXT1_WH', 'prd_cps_dsci_etl_svc')
    )
    df.to_sql('CPS_PINBOARD_NAME'.lower(), engine, schema="CPS_DSCI_API", index=False, if_exists="append")

    landing_page_link = f"""https://cisco.thoughtspot.cloud.com/?col1=DASHBOARD%20NAME&op1=EQ&val1={canvas_name}#/pinboard/204f25ff-273a-47dd-8090-0cc1e88dff68/"""
    print(landing_page_link)
    return True


@task(log_stdout=True,tags=["snowflake_xsmall"])
def delete_from_ts_landing_page_table(canvas_name):
    engine = create_engine(
        sec.get_sf_pw('prd_cps_dsci_etl_svc', 'CPS_DSCI_ETL_EXT1_WH', 'prd_cps_dsci_etl_svc')
    )
    del_qry = f"""delete from CPS_DSCI_API.CPS_PINBOARD_NAME where DASHBOARD_NAME = '{canvas_name}'"""
    con = engine.connect()
    con.execute(del_qry)
    return True


@task(log_stdout=True)
def update_ws_display_names(new_ws_guid, ts):
    ws_dict = ts.tsrest.metadata_tml_export(guid=new_ws_guid)
    ws_obj = Worksheet(ws_dict)

    session = boto3.Session(
        aws_access_key_id=my_sec.ACCESS_KEY,
        aws_secret_access_key=my_sec.SECRET_KEY
    )

    s3 = session.resource('s3')

    bucket = 'canvas-data-types'
    key = 'ts_display_name_map.json'

    obj = s3.Object(bucket, key)
    data = obj.get()['Body'].read().decode('utf-8')

    ts_display_name_json_data = oyaml.safe_load(data)

    for key in ts_display_name_json_data:
        if key != "zone_description":
            for col in ws_obj.tml['worksheet']['worksheet_columns']:
                if col['name'] == key.upper():
                    col['name'] = ts_display_name_json_data[key]

    print("updated ws %%%%%%%%%%%%%%%%%%")
    print(ws_obj.tml)

    return ws_obj


# use this code for checking difference in tmls when the time comes
def check_for_diff(tml1, tml2):
    ddiff = DeepDiff(tml1.tml, tml2.tml)
    if ddiff:
        print("Tmls are not the same")
        print(ddiff)


def get_json_from_s3(bucket, key):
    session = boto3.Session(
        aws_access_key_id=my_sec.ACCESS_KEY,
        aws_secret_access_key=my_sec.SECRET_KEY
    )

    s3 = session.resource('s3')
    obj = s3.Object(bucket, key)
    data = obj.get()['Body'].read().decode('utf-8')
    json_data = oyaml.safe_load(data)
    return json_data


@task(log_stdout=True)
def create_ws_name_for_premade(pre_made_table_name):
    ws_name = f"""ws_{pre_made_table_name}"""
    return ws_name

def fix_cols(df):
    cols = []
    for c in df.columns:
        cl = c.lower()
        cols.append(cl.strip().replace(' ', '_').replace('/', '_').replace('\\', '_').replace('-', '_'))
    return cols


@task(log_stdout=True,tags=["snowflake_xsmall"])
def rename_cols_in_preexsisting_table(src_tbl, schema, sf_env):
    sf_env = check_env(sf_env)

    engine = create_engine(sec.get_sf_pw(sf_env, 'CPS_DSCI_ETL_EXT1_WH', sf_env))
    con = engine.connect()
    df = pd.DataFrame(con.execute(f"desc table {schema.lower()}.{src_tbl.lower()}").fetchall())
    print('############################################################')
    print(df.columns)
    print('############################################################')
    df.columns = fix_cols(df)
    df.rename(columns={'name': 'column_name'}, inplace=True)

    for i, row in df.iterrows():
        row['column_name'] = row['column_name'].strip().replace(' ', '_').replace('/', '_').replace('\\', '_').replace(
            '-', '_').lower()
    df['rename'] = df.column_name

    # dont run this for TS TABLES  its doing direct into canvas_data
    final_rename_ts = get_json_from_s3('canvas-data-types', 'ts_display_name_map.json')
    for c in final_rename_ts:
        df.loc[(df.column_name == c), 'rename'] = final_rename_ts.get(c, c)

    for i, row in df.iterrows():
        row['rename'] = row['rename'].strip().replace(' ', '_').replace('/', '_').replace('\\', '_').replace('-',
                                                                                                             '_').upper()

    for i, row in df.iterrows():
        row['column_name'] = row['column_name'].strip().replace(' ', '_').replace('/', '_').replace('\\', '_').replace(
            '-', '_').upper()

    for i, row in df.iterrows():
        if row['column_name'] != row['rename']:
            print(row['rename'])
            df = pd.DataFrame(
                con.execute(
                    f'alter table {schema.lower()}.{src_tbl.lower()} rename column {row["column_name"]} to "{row["rename"]}"').fetchall())

    return src_tbl


@task(log_stdout=True)
def compare_tml_and_push_if_different(list_of_answers, list_of_pinboards, table_name, env, engagement_id, ts):
    session = boto3.Session(
        aws_access_key_id=my_sec.ACCESS_KEY,
        aws_secret_access_key=my_sec.SECRET_KEY
    )

    s3 = session.resource('s3')


    from deepdiff import DeepDiff
    eng_folder = f"""eng_{engagement_id}"""

    guid_list = []

    print(f"searching for the guid for table : {table_name}")
    table_guid = get_table_guid(table_name, ts)  # thi is in ts_delete

    try:
        dep_objs = ts.table.get_dependent_objects(table_guids=[table_guid])
        print('#####################################')
        print("Dependent objects response for table {} :".format(table_guid))
        print(dep_objs)
        print('#####################################')
        dep_objs_guid_map = get_dependent_objects_guid_map(dep_objs)  # this is in ts_delete
        print("Dependent objects mapping by type for table {}".format((table_guid)))
        print(dep_objs_guid_map)
        print('#####################################')
        for obj_type in dep_objs_guid_map:
            if len(dep_objs_guid_map[obj_type]) > 0:
                print('#####################################')
                guid_list.append(dep_objs_guid_map[obj_type])
                print(dep_objs_guid_map[obj_type])

    except requests.exceptions.HTTPError as e:
        print(e.request.url)
        print(e.response.status_code)
        print(e.response.content)
        exit()

        #write all pinboard and answer tmls to back up
    for g_list in guid_list:
        for guid in g_list:
            tml = ts.tml.download_tml(guid)
            print(guid)
            today = str(day.today())
            if 'answer' in tml.keys():  # first check if this is an answer or pin board
                answer_name = tml['answer']['name']  # get the answer name
                s3object = s3.Object('thought.spot.tml', f'{env}/back_up/{eng_folder}/{today}/{answer_name}.tml')

                s3object.put(
                    Body=(bytes(json.dumps(tml).encode('UTF-8'))))
                break

            elif 'pinboard' in tml.keys():  # only need to check for pins and answers for now
                pinboard_name = tml['pinboard']['name']

                s3object = s3.Object('thought.spot.tml', f'{env}/back_up/{eng_folder}/{today}/{pinboard_name}.tml')

                s3object.put(
                    Body=(bytes(json.dumps(tml).encode('UTF-8'))))
                break

            elif 'liveboard' in tml.keys():  # only need to check for pins and answers for now
                pinboard_name = tml['liveboard']['name']

                s3object = s3.Object('thought.spot.tml', f'{env}/back_up/{eng_folder}/{today}/{pinboard_name}.tml')

                s3object.put(
                    Body=(bytes(json.dumps(tml).encode('UTF-8'))))
                break

            else:
                print("not a pin or answer")



    # compare , check if the tml has changed, and write to the eng folder as a custom file if it has
    print("""**** THIS IS THE GUID LIST FOR DELETE *****""")
    print(guid_list)
    print("""**********************""")
    for g_list in guid_list:
        for guid in g_list:
            tml = ts.tml.download_tml(guid)
            print(guid)
            print("################################################################")
            if 'answer' in tml.keys():  # first check if this is an answer or pin board
                answer_name = tml['answer']['name']  # get the answer name
                for answer in list_of_answers:  # loop through list of answers and check if the name matches
                    print(f"""{answer.tml['answer']['name']}   ==   {answer_name}""")
                    if answer.tml['answer']['name'] == answer_name:  # if names match then check for differences in tml
                        print("answer names matched")
                        ddiff = DeepDiff(tml, answer.tml)
                        if len(ddiff) == 0:  # if no diff, do nothing
                            break
                        # elif "values_changed" in ddiff: #dont save if only the guid has changed
                        #     if "root['guid']" in ddiff['values_changed']:
                        #         break
                        else:  # if a diff, write the tml to the eng s3 bucket
                            print(ddiff)
                            print('writing this answer to the eng s3 bucket')
                            if "custom_" in answer_name:
                                tml['answer']['name'] = answer_name
                            else:
                                tml['answer']['name'] = f"""custom_{answer_name}"""
                            s3 = boto3.resource('s3')
                            if "custom_" in answer_name:
                                s3object = s3.Object('thought.spot.tml', f'{env}/{eng_folder}/answer_{answer_name}.tml')
                            else:
                                s3object = s3.Object('thought.spot.tml',
                                                     f'{env}/{eng_folder}/answer_custom_{answer_name}.tml')

                            s3object.put(
                                Body=(bytes(json.dumps(tml).encode('UTF-8'))))
                            break

            elif 'pinboard' in tml.keys():  # only need to check for pins and answers for now
                pinboard_name = tml['pinboard']['name']
                for pinboard in list_of_pinboards:
                    print(f"""{pinboard.tml['pinboard']['name']}   ==   {pinboard_name}""")
                    if pinboard.tml['pinboard']['name'] == pinboard_name:
                        print("pn names matched")

                        ddiff = DeepDiff(tml, pinboard.tml)
                        print(ddiff)
                        if len(ddiff) == 0:
                            break
                        else:
                            print('writing this answer to the eng s3 bucket')
                            if "custom_" in pinboard_name:
                                tml['pinboard']['name'] = pinboard_name
                            else:
                                tml['pinboard']['name'] = f"""custom_{pinboard_name}"""
                            if "custom_" in pinboard_name:
                                s3object = s3.Object('thought.spot.tml', f'{env}/{eng_folder}/pinboard_{pinboard_name}.tml')
                            else:
                                s3object = s3.Object('thought.spot.tml',
                                                     f'{env}/{eng_folder}/pinboard_custom_{pinboard_name}.tml')
                            s3object.put(
                                Body=(bytes(json.dumps(tml).encode('UTF-8'))))
                            break


            else:
                print("not a pin or answer")

@task(log_stdout=True,tags=["snowflake_xsmall"])
def delete_sf_table(db_table,sf_env,schema,del_sf_table):
    if del_sf_table:
        sf_env = check_env(sf_env)
        engine = create_engine(
            sec.get_sf_pw(sf_env, warehouseXsmall, sf_env)
        )


        con = engine.connect()
        print(f"""drop table if exists {schema.lower()}.{db_table.lower()};""")
        delete_done = con.execute(f"""drop table if exists {schema.lower()}.{db_table.lower()};""")

    return True


@task(log_stdout=True)
def check_if_canvas_is_locked(locked_canvas_list,canvas_name):
    """
    Checks if a canvas has been locked from delete/create/update ,
    inorder to protect against cams deleting tmls that other cams are working on.

    :param locked_canvas_list:  list of locked canvas's from prefect secrets
    :param canvas_name: current canvas name
    :return: Exits flow if canvas is locked, else returns True
    """
    print(locked_canvas_list)
    if canvas_name in locked_canvas_list:
        print(f"{canvas_name} is locked from being modified. Please remove from LOCKED_CANVAS_LIST in https://cloud.prefect.io/team/secrets if you would like to run this")
        # sys.exit(1)
        raise SKIP()
    else:
        return True



storage_obj = Docker(
    base_image="prefecthq/prefect:0.15.3-python3.8",
    python_dependencies=[
        "pandas==1.4.2",
        "awswrangler==2.10.0",
        "numpy==1.19.2",
        "elasticsearch==7.14.0",
        "boto3==1.18.16",
        "aiohttp",
        "hvac==0.11.2",
        "snowflake-sqlalchemy==1.2.4",
        "s3fs==0.4",
        "SQLAlchemy==1.4.37",
        "awswrangler>2.10.0",
        "fastparquet>0.7.1",
        "XlsxWriter>3.0.1",
        "oyaml",
        "thoughtspot_rest_api_v1==1.0.8",
        "thoughtspot_tml==1.0.14",
        "deepdiff"

    ],
    registry_url="837578041534.dkr.ecr.us-east-1.amazonaws.com/flows",
    files={
        """/Users/ejurotic/PycharmProjects/Cloud-ThoughtSpot-Flows/common/new_bulkload.py""": "/root/.prefect/flows/common/new_bulkload.py",
        """/Users/ejurotic/PycharmProjects/Cloud-ThoughtSpot-Flows/common/sec.py""": "/root/.prefect/flows/common/sec.py",
        """/Users/ejurotic/PycharmProjects/Cloud-ThoughtSpot-Flows/common/sql_pool.py""": "/root/.prefect/flows/common/sql_pool.py",
        """/Users/ejurotic/PycharmProjects/Cloud-ThoughtSpot-Flows/my_sec/my_sec.py""": "/root/.prefect/flows/my_sec/my_sec.py",
        """/Users/ejurotic/PycharmProjects/Cloud-ThoughtSpot-Flows/endpoint_method_classes.py""": "/root/.prefect/flows/endpoint_method_classes.py",
        """/Users/ejurotic/PycharmProjects/Cloud-ThoughtSpot-Flows/tag_refresh_to_ts_table.py""": "/root/.prefect/flows/tag_refresh_to_ts_table.py",
        """/Users/ejurotic/PycharmProjects/Cloud-ThoughtSpot-Flows/thoughtspot.py""": "/root/.prefect/flows/thoughtspot.py",
        """/Users/ejurotic/PycharmProjects/Cloud-ThoughtSpot-Flows/TS_canvas_to_table_ws_answer.py""": "/root/.prefect/flows/TS_canvas_to_table_ws_answer.py",
        """/Users/ejurotic/PycharmProjects/Cloud-ThoughtSpot-Flows/TS_delete_from_table.py""": "/root/.prefect/flows/TS_delete_from_table.py",
        """/Users/ejurotic/PycharmProjects/Cloud-ThoughtSpot-Flows/thoughtspot_rest_api_v1.py""": "/root/.prefect/flows/thoughtspot_rest_api_v1.py",
        """/Users/ejurotic/PycharmProjects/Cloud-ThoughtSpot-Flows/tml.py""": "/root/.prefect/flows/tml.py"
    },
    env_vars={"PYTHONPATH": "${PYTHONPATH}:/root/.prefect/flows/"},
)

with Flow(
        "canvas-to-cloud-thoughtspot",
        storage=storage_obj,
        run_config=DockerRun(),
        executor=LocalDaskExecutor(scheduler="processes", num_workers=psutil.cpu_count(logical=True)),
        # executor=LocalDaskExecutor(scheduler="processes", num_workers=16),
        result=S3Result(bucket="cam-prefect-results", boto3_kwargs={"credentials":{"ACCESS_KEY":my_sec.ACCESS_KEY ,"SECRET_ACCESS_KEY": my_sec.SECRET_KEY}})
) as flow:
    canvas_name = Parameter("canvas_name", required=False)
    request_id = Parameter("request_id", required=False)
    requestedBy = Parameter("requestedBy", required=True)
    action = Parameter("action", required=True)
    date = Parameter("date", required=True)
    cn_name = Parameter("cn_name", required=True)
    db_name = Parameter("db_name", required=True)
    schema = Parameter("schema", required=True)
    connection_guid = Parameter("connection_guid", required=True)
    ts_env = Parameter("ts_env", required=True)
    action = Parameter("action", default='create', required=True)
    pre_made_table_name = Parameter("pre_made_table_name", default='none', required=False)
    sf_env = Parameter("sf_env", required=True)
    del_sf_table = Parameter("del_sf_table", default=True, required=False)
    # canvas_parquet_path = Parameter("canvas_parquet_path", required=True)

    locked_canvas_list = PrefectSecret("LOCKED_CANVAS_LIST")

    not_locked = check_if_canvas_is_locked(locked_canvas_list,canvas_name)

    with case(action, "delete"):
        ts = log_in_to_thoughtspot(ts_env,upstream_tasks=[not_locked])

        canvas_meta_df = get_canvas_meta_data(canvas_name, sf_env, action, request_id, upstream_tasks=[ts])

        converted_canvas_name, db_table, new_worksheet_name, engagement_id, canvas_parquet_path = get_created_params(
            canvas_name, canvas_meta_df, upstream_tasks=[canvas_meta_df])
        ########

        std_tml_answers, std_tml_pinboards, all_answer_locs, all_pinboard_locs = get_templates_from_s3(
            converted_canvas_name, engagement_id, ts_env)

        changes_pushed = compare_tml_and_push_if_different(std_tml_answers, std_tml_pinboards, db_table, ts_env,
                                                           engagement_id, ts,
                                                           upstream_tasks=[std_tml_answers, std_tml_pinboards,
                                                                           all_answer_locs, all_pinboard_locs])

        ##########
        deleted = get_dependent_objects_for_table_name(db_table, ts,connection_guid, upstream_tasks=[converted_canvas_name, db_table,
                                                                                     new_worksheet_name, engagement_id,
                                                                                     canvas_parquet_path,changes_pushed
                                                                                     ])
        # need to log this has been deleted
        landing_page_deleted = delete_from_ts_landing_page_table(canvas_name)

        table_deleted = delete_sf_table(db_table,sf_env,schema,del_sf_table, upstream_tasks=[landing_page_deleted])

        logged_to_bia = log_to_bia_table(sf_env, request_id, 'Delete', canvas_name,
                                         upstream_tasks=[deleted, landing_page_deleted])

    with case(action, "update", ):
        ts = log_in_to_thoughtspot(ts_env,upstream_tasks=[not_locked])

        canvas_meta_df = get_canvas_meta_data(canvas_name, sf_env, action, request_id, upstream_tasks=[ts])

        converted_canvas_name, db_table, new_worksheet_name, engagement_id, canvas_parquet_path = get_created_params(
            canvas_name, canvas_meta_df, upstream_tasks=[canvas_meta_df])

        table_created = refresh_tags_and_create_ts_table(engagement_id, schema, db_table, sf_env, canvas_parquet_path)
        logged_to_bia = log_to_bia_table(sf_env, request_id, 'Update', canvas_name, upstream_tasks=[table_created])


    with case(action, "create"):
        ts = log_in_to_thoughtspot(ts_env,upstream_tasks=[not_locked])

        canvas_meta_df = get_canvas_meta_data(canvas_name, sf_env, action, request_id, upstream_tasks=[ts])

        converted_canvas_name, db_table, new_worksheet_name, engagement_id, canvas_parquet_path = get_created_params(
            canvas_name, canvas_meta_df, upstream_tasks=[canvas_meta_df])

        table_created_name = refresh_tags_and_create_ts_table(engagement_id, schema, db_table, sf_env,
                                                              canvas_parquet_path)

        table_created = rename_cols_in_preexsisting_table(table_created_name, schema,sf_env,
                                                          upstream_tasks=[table_created_name])

        table_obj = create_table_obj(cn_name, db_name, schema, db_table, connection_guid, ts,
                                     upstream_tasks=[converted_canvas_name, db_table, new_worksheet_name, engagement_id,
                                                     canvas_parquet_path, table_created])

        new_table_guid = push_tml_to_thought_spot([table_obj], True, False, ts, upstream_tasks=[table_obj])

        ws_obj = create_ws_from_table(new_table_guid, new_worksheet_name, ts, db_table, upstream_tasks=[new_table_guid])

        new_ws_guid = push_tml_to_thought_spot([ws_obj], True, False, ts, upstream_tasks=[ws_obj, new_table_guid])

        # updated_ws = update_ws_display_names(new_ws_guid,ts) Commented out because we do this in teh table now

        std_tml_answers, std_tml_pinboards, all_answer_locs, all_pinboard_locs = get_templates_from_s3(
            converted_canvas_name, engagement_id, ts_env, upstream_tasks=[new_ws_guid])

        answer_tmls = convert_answer_template_to_current(all_answer_locs, converted_canvas_name, new_worksheet_name,
                                                         new_ws_guid, upstream_tasks=[std_tml_answers, std_tml_pinboards, all_answer_locs, all_pinboard_locs])

        pinboard_tmls = convert_pinboard_template_to_current(all_pinboard_locs, new_worksheet_name, new_ws_guid)
        #### TODO add convert_answer_template_to_current for pinboards

        validated_answer_tmls, invalidated_answer_tmls = validate_tmls(answer_tmls, ts, upstream_tasks=[answer_tmls,new_ws_guid])

        validated_pinboard_tmls, invalidated_pinboard_tmls = validate_tmls(pinboard_tmls, ts,
                                                                           upstream_tasks=[pinboard_tmls,new_ws_guid])

        new_answer_guids = push_tml_to_thought_spot(validated_answer_tmls, True, False, ts,
                                                    upstream_tasks=[validated_answer_tmls])

        new_pinboard_guids = push_tml_to_thought_spot(validated_pinboard_tmls, True, False, ts,
                                                      upstream_tasks=[validated_pinboard_tmls])

        # updated_ws_guid = push_tml_to_thought_spot([updated_ws], False, False, ts, upstream_tasks=[new_pinboard_guids])

        ### TODO add function to move invalidated tmls to failed folder in s3
        landing_page_link = create_landing_page(new_answer_guids, new_pinboard_guids, new_ws_guid, new_table_guid,
                                                requestedBy, canvas_name, ts, upstream_tasks=[new_pinboard_guids])

        logged = log_invalidated_tmls(invalidated_answer_tmls,
                                      invalidated_pinboard_tmls,
                                      new_table_guid,
                                      new_ws_guid,
                                      canvas_meta_df,
                                      db_table,
                                      new_worksheet_name,
                                      request_id,
                                      sf_env,
                                      upstream_tasks=[new_answer_guids])

        shared = share_content(requestedBy, new_table_guid, new_ws_guid, new_answer_guids, new_pinboard_guids, ts,
                               upstream_tasks=[new_answer_guids])
        #
        log_updated = update_canvas_to_ts_log_table(new_table_guid, new_ws_guid, new_answer_guids, new_pinboard_guids,
                                                    canvas_meta_df, db_table, new_worksheet_name, request_id, sf_env)

        logged_to_bia = log_to_bia_table(sf_env, request_id, 'Create', canvas_name,
                                         upstream_tasks=[log_updated, landing_page_link])

        logged_out = log_out_of_ts(ts, upstream_tasks=[new_answer_guids, log_updated, shared, logged_to_bia])

    with case(action, "premade_table"):
        ts = log_in_to_thoughtspot(ts_env,upstream_tasks=[not_locked])

        canvas_meta_df = get_canvas_meta_data(canvas_name, sf_env, action, request_id, upstream_tasks=[ts])

        converted_canvas_name, db_table, new_worksheet_name, engagement_id, canvas_parquet_path = get_created_params(
            canvas_name, canvas_meta_df, upstream_tasks=[canvas_meta_df])

        table_created = rename_cols_in_preexsisting_table(pre_made_table_name, schema, sf_env)

        new_worksheet_name = create_ws_name_for_premade(pre_made_table_name, upstream_tasks=[table_created])

        table_obj = create_table_obj(cn_name, db_name, schema, pre_made_table_name, connection_guid, ts,
                                     upstream_tasks=[ts])

        new_table_guid = push_tml_to_thought_spot([table_obj], True, False, ts, upstream_tasks=[table_obj])

        ws_obj = create_ws_from_table(new_table_guid, new_worksheet_name, ts, pre_made_table_name,
                                      upstream_tasks=[new_table_guid])

        new_ws_guid = push_tml_to_thought_spot([ws_obj], True, False, ts, upstream_tasks=[ws_obj])

        std_tml_answers, std_tml_pinboards, all_answer_locs, all_pinboard_locs = get_templates_from_s3(
            converted_canvas_name, engagement_id, ts_env, upstream_tasks=[new_ws_guid])

        answer_tmls = convert_answer_template_to_current(all_answer_locs, converted_canvas_name, new_worksheet_name,
                                                         new_ws_guid, upstream_tasks=[std_tml_answers, std_tml_pinboards, all_answer_locs, all_pinboard_locs])

        pinboard_tmls = convert_pinboard_template_to_current(all_pinboard_locs, new_worksheet_name, new_ws_guid)
        #### TODO add convert_answer_template_to_current for pinboards

        validated_answer_tmls, invalidated_answer_tmls = validate_tmls(answer_tmls, ts, upstream_tasks=[answer_tmls,new_ws_guid])

        validated_pinboard_tmls, invalidated_pinboard_tmls = validate_tmls(pinboard_tmls, ts,
                                                                           upstream_tasks=[pinboard_tmls,new_ws_guid])

        new_answer_guids = push_tml_to_thought_spot(validated_answer_tmls, True, False, ts,
                                                    upstream_tasks=[validated_answer_tmls])

        new_pinboard_guids = push_tml_to_thought_spot(validated_pinboard_tmls, True, False, ts,
                                                      upstream_tasks=[validated_pinboard_tmls])



        shared = share_content(requestedBy, new_table_guid, new_ws_guid, new_answer_guids, new_pinboard_guids, ts,
                               upstream_tasks=[new_ws_guid])

        # updated_ws = update_ws_display_names(new_ws_guid,ts,upstream_tasks=[new_ws_guid])

        logged_out = log_out_of_ts(ts, upstream_tasks=[shared])




if __name__ == "__main__":
    flow.run(

        parameters=       {
  "action": "delete",
  "canvas_name": "CANVAS-123",
  "cn_name": "8_11_con",
  "connection_guid": "8f9c4b17-f909-412e-8d39-6a4cfff6ab89",
  "db_name": "CPS_DB",
  "del_sf_table": True,
  "pre_made_table_name": "none",
  "request_id": "57723",
  "requestedBy": "alanzen,benjacob",
  "schema": "CPS_DSCI_ARCHIVE",
  "sf_env": "prod",
  "ts_env": "prod"
}
    )




# {
#                   "action": "create",
#                   "canvas_name": "CANVAS-406",
#                   "cn_name": "cloud_gen_prod_sf",
#                   "connection_guid": "3e90f4dc-f4f5-4d64-b304-b7bbab50ff58",
#                   "db_name": "CPS_DB",
#                   "pre_made_table_name": "none",
#                   "request_id": "55562",
#                   "requestedBy": "adoncarm,ejurotic",
#                   "schema": "CPS_DSCI_ARCHIVE",
#                   "sf_env": "prod",
#                   "ts_env": "prod"
#                 }
#
#
# {
#               "action": "delete",
#               "canvas_name": "CANVAS-406",
#               "cn_name": "cloud_gen_prod_sf",
#               "connection_guid": "3e90f4dc-f4f5-4d64-b304-b7bbab50ff58",
#               "db_name": "CPS_DB",
#               "pre_made_table_name": "none",
#               "request_id": "55562",
#               "requestedBy": "adoncarm,ejurotic",
#               "schema": "CPS_DSCI_ARCHIVE",
#               "sf_env": "prod",
#               "ts_env": "prod"
#             }