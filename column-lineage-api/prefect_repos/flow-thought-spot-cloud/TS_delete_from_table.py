# http://172.18.138.27:8090/notebooks/canvas_refresh_tags.ipynb#
# http://172.18.138.27:8090/notebooks/canvas_to_snowflake_for_thoughtspot_flow-EJ.ipynb
from my_sec import my_sec
import json
import math
import os
from datetime import datetime
from pathlib import Path
import shutil
import awswrangler as wr
import boto3
import numpy as np
import pandas as pd
from prefect import Flow, Parameter, task
from sqlalchemy import create_engine
import oyaml
import sqlalchemy
from common import new_bulkload as bl
from common import sec
import os
import requests.exceptions
import json
from collections import OrderedDict
from thoughtspot import ThoughtSpot, MetadataNames

# Parses the dependency responses to return only the object types and a list of GUIDs,
# which can be input into other commands to take action
def get_dependent_objects_guid_map(dependent_objects_response):
    # You might have a reason to deal with object types in a particular order
    # For example, if you are deleting things, you would do Liveboards and Answers first, then Worksheets,
    # before the tables themselves
    obj_type_guid_map = OrderedDict(
        {
            MetadataNames.LIVEBOARD: [],
            MetadataNames.ANSWER: [],
            MetadataNames.WORKSHEEET: [],
            MetadataNames.TABLE: []
        }
    )
    dep_objs = dependent_objects_response
    # First level of response is the GUID of the requested object
    for obj in dep_objs:
        # For every object_type, there is a key for a List of the objects of that type
        for obj_type in dep_objs[obj]:
            #print(obj_type)
            # The objects of each type as object structures, with 'id' property and other metadata details
            for o in dep_objs[obj][obj_type]:
                #print(o)
                obj_type_guid_map[obj_type].append(o['id'])
    return obj_type_guid_map


def get_table_guid(table_name,ts):
    table_guid = ts.table.find_guid(table_name)
    return table_guid



@task(log_stdout=True)
def get_dependent_objects_for_table_name(table_name: str,ts, connection_id ):
    #print('Dependencies for Table')
    print(f"searching for the guid for table : {table_name}")
    table_guid = get_table_guid(table_name,ts)

    try:
        dep_objs = ts.table.get_dependent_objects(table_guids=[table_guid])
        print("Dependent objects response for table {} :".format(table_guid))
        print(dep_objs)

        dep_objs_guid_map = get_dependent_objects_guid_map(dep_objs)
        print("Dependent objects mapping by type for table {}".format((table_guid)))
        print(dep_objs_guid_map)

        for obj_type in dep_objs_guid_map:
            if len(dep_objs_guid_map[obj_type]) > 0:
                #
                #
                # HERE YOU WOULD PUT THE ACTION YOU WANT TO ACCOMPLISH WITH THE DEPENDENCIES
                #
                #
#                 pass  # comment out and add section for what you'd like to do, examples below

#                 Example action of deleting all of the objects
                print('Deleting {} objects: {}'.format(obj_type, dep_objs_guid_map[obj_type]))
                ts.tsrest.metadata_delete(object_type=obj_type, guids=dep_objs_guid_map[obj_type])

                # Alternatively, you could TAG each object
                # tag_guids = [ts.tag.find_guid('Tag To Use')]
                # print('Tagging {} objects: {}'.format(obj_type, dep_objs_guid_map[obj_type]))
                # ts.tag.assign_tags(object_guids=dep_objs_guid_map[obj_type], tag_guids=tag_guids)
        print(f"Deleting table {table_name}")
        # ts.tsrest.metadata_delete(object_type="LOGICAL_TABLE", guids=[table_guid])
        print(connection_id)
        print(table_guid)
        table_tml = ts.tml.download_tml(table_guid)

        connection_id = ts.connection.find_guid(table_tml['table']['connection']['name'])

        ts.tsrest.remove_table(connection_id = connection_id, guids=[{"id": table_guid}])
        # ts.tsrest.remove_table(connection_id, [{"id": table_guid}])
    except requests.exceptions.HTTPError as e:
        print(e.request.url)
        print(e.response.status_code)
        print(e.response.content)
        exit()

