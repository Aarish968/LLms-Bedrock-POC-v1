# %load_ext autoreload
# %autoreload 2

import os
import math
from prefect.executors import LocalExecutor
import requests
from prefect.engine.results import S3Result
from prefect import Flow, Parameter, task
from prefect.storage import Docker
from sqlalchemy import create_engine
from prefect.engine.signals import FAIL , SUCCESS
import json
import boto3
from datetime import datetime

import flow_variables
from common.config import  RunSettings
from common import sec, config
from sqlalchemy import create_engine, text, bindparam, inspect, BIGINT
from sqlalchemy.sql import quoted_name
import pandas as pd
from prefect.run_configs.docker import DockerRun
from prefect.run_configs.kubernetes import KubernetesRun
from log_to_dc_job_messages import log_to_dc_job_messages, final_flow_state_message,final_failed_flow_state_message
import wb as wb
from prefect.triggers import all_successful, all_failed, all_finished,any_failed

#md
import numpy as np
import prefect

# Global variables
sf_env = None
RANKING_WEIGHTS = None
PRUNING_PENALTIES = None

#engine = None



### CORE ###
############

# Function to align the DataFrame to the expected schema
def align_dataframe_columns(df, all_columns):
    # Add missing columns as None (null in the database)
    for column in all_columns:
        if column not in df.columns:
            df[column] = None
    # Reorder and select only the expected columns
    return df[all_columns]

def convert_column_to_numeric(df, column_name):
    # Convert column to pandas numeric type, coerce errors to NaN
    df[column_name] = pd.to_numeric(df[column_name], errors='coerce')
    return df


### core
## 3.2 Load mapping data - all
def load_eng_gu_acat_id_mappings():

    #engine = create_sf_connection_engine(sf_env)
    cn = check_env(sf_env)
    correct_schema = get_correct_schema(sf_env)
    engine = create_engine(
        sec.get_sf_pw(cn, flow_variables.warehouseXsmall, correct_schema)
    )

    sql_query = """
    
    with active as (-- active bookings to engagements with party data ideally
        select distinct p.CR_PARTY_ID , p.DC_ENGAGEMENT_ID
        from CPS_DB.CPS_DSCI_API.DC_ENGAGEMENT_HDR e
            left join CPS_DB.CPS_DSCI_API.DC_PARTY_LINKS p   on (p.DC_ENGAGEMENT_ID = e.DC_ENGAGEMENT_ID and p.IS_DELETED = 'F')
        where e.IS_DELETED = 'F'
     )
       , actual_gus as (
                select distinct active.DC_ENGAGEMENT_ID, h.GLOBAL_ULTIMATE_ID  --
                from active
                join EDW_MASTER_ETL_DB.SS.CRT_XXNGCR_DNM_RELATIONSHIPS h
                on (h.PARTY_ID = active.CR_PARTY_ID)
        )
    , actual_gus_with_ACAT as (
                select distinct p.ACAT_CUSTOMER_ID, p.DC_ENGAGEMENT_ID
        from CPS_DB.CPS_DSCI_API.DC_ENGAGEMENT_HDR e
            left join CPS_DB.CPS_DSCI_API.DC_ACAT_LINKS  p   on (p.DC_ENGAGEMENT_ID = e.DC_ENGAGEMENT_ID and p.IS_DELETED = 'F')
        where e.IS_DELETED = 'F'
        )
       select actual_gus.DC_ENGAGEMENT_ID, listagg(distinct actual_gus.GLOBAL_ULTIMATE_ID,',') as GLOBAL_ULTIMATE_IDs , listagg(distinct actual_gus_with_ACAT.ACAT_CUSTOMER_ID,',') as ACAT_CUSTOMER_IDs
       from actual_gus left join actual_gus_with_ACAT on (actual_gus.DC_ENGAGEMENT_ID=  actual_gus_with_ACAT.DC_ENGAGEMENT_ID )
       group by actual_gus.DC_ENGAGEMENT_ID
    

    """



    # Loading into pandas
    try:
        with engine.connect() as connection:
            df = pd.read_sql(sql_query, connection)
            print(df.head())
    except Exception as e:
        df = pd.DataFrame()
        print("An error occurred:", e)

    # It is important to explicitly dispose of the engine once done
    engine.dispose()

    return df


## 3.3 Load mapping data - one engagement ID
def load_eng_gu_acat_id_mapping(ENG_ID):
    """
    Load engagement, global ultimate, and ACAT customer ID mappings for a specific engagement ID.

    Parameters:
    ENG_ID (int): The engagement ID to filter the data.

    Returns:
    pd.DataFrame: DataFrame containing the mappings.
    """

    sql_query = f"""
    WITH active AS (
        SELECT DISTINCT p.CR_PARTY_ID, p.DC_ENGAGEMENT_ID
        FROM CPS_DB.CPS_DSCI_API.DC_ENGAGEMENT_HDR e
        LEFT JOIN CPS_DB.CPS_DSCI_API.DC_PARTY_LINKS p ON (p.DC_ENGAGEMENT_ID = e.DC_ENGAGEMENT_ID AND p.IS_DELETED = 'F')
        WHERE e.IS_DELETED = 'F' AND e.DC_ENGAGEMENT_ID = '{ENG_ID}'
    ),
    actual_gus AS (
        SELECT DISTINCT active.DC_ENGAGEMENT_ID, h.GLOBAL_ULTIMATE_ID
        FROM active
        JOIN EDW_MASTER_ETL_DB.SS.CRT_XXNGCR_DNM_RELATIONSHIPS h ON (h.PARTY_ID = active.CR_PARTY_ID)
    ),
    actual_gus_with_ACAT AS (
        SELECT DISTINCT p.ACAT_CUSTOMER_ID, p.DC_ENGAGEMENT_ID
        FROM CPS_DB.CPS_DSCI_API.DC_ENGAGEMENT_HDR e
        LEFT JOIN CPS_DB.CPS_DSCI_API.DC_ACAT_LINKS p ON (p.DC_ENGAGEMENT_ID = e.DC_ENGAGEMENT_ID AND p.IS_DELETED = 'F')
        WHERE e.IS_DELETED = 'F' AND e.DC_ENGAGEMENT_ID = '{ENG_ID}'
    )
    SELECT actual_gus.DC_ENGAGEMENT_ID, 
           LISTAGG(DISTINCT actual_gus.GLOBAL_ULTIMATE_ID, ',') AS GLOBAL_ULTIMATE_IDs, 
           LISTAGG(DISTINCT actual_gus_with_ACAT.ACAT_CUSTOMER_ID, ',') AS ACAT_CUSTOMER_IDs
    FROM actual_gus 
    LEFT JOIN actual_gus_with_ACAT ON (actual_gus.DC_ENGAGEMENT_ID = actual_gus_with_ACAT.DC_ENGAGEMENT_ID)
    GROUP BY actual_gus.DC_ENGAGEMENT_ID
    """

    cn = check_env(sf_env)
    correct_schema = get_correct_schema(sf_env)
    engine = create_engine(
        sec.get_sf_pw(cn, flow_variables.warehouseXsmall, correct_schema)
    )

    # Loading into pandas
    try:
        with engine.connect() as connection:
            df = pd.read_sql(sql_query, connection)
            print(df.shape, df.head())
    except Exception as e:
        df = pd.DataFrame()
        print("An error occurred:", e)
    finally:
        # It is important to explicitly dispose of the engine once done
        engine.dispose()

    return df

def get_mapping_ids(df_mapping):
    """
    Extract and print Global Ultimate IDs and ACAT Customer IDs from the DataFrame.

    Parameters:
    df_mapping (pd.DataFrame): DataFrame containing the mappings.

    Returns:
    tuple: A tuple containing lists of Global Ultimate IDs and ACAT Customer IDs.
    """
    GU_IDs = df_mapping['global_ultimate_ids'].loc[0]
    print("\nGU IDs to process:", GU_IDs)

    GU_ID_LIST = [int(i) for i in GU_IDs.split(",")]
    print("GU ID LIST:", GU_ID_LIST)

    ACAT_CUST_IDs = df_mapping['acat_customer_ids'].loc[0]
    print("ACAT Customer IDs:", ACAT_CUST_IDs)

    return GU_IDs, ACAT_CUST_IDs, GU_ID_LIST



## 3.4 Load ACAT site IDs
def load_acat_site_ids(ACAT_CUST_IDs, GU_IDs):
    """
    Retrieve site IDs based on ACAT customer IDs.

    Parameters:
    ACAT_CUST_IDs (str): Comma-separated string of ACAT customer IDs.
    credentials: Snowflake credentials.
    warehouse: Snowflake warehouse.
    role: Snowflake role.
    db: Snowflake database.

    Returns:
    pd.DataFrame: DataFrame containing the site IDs.
    """
    #engine = create_sf_connection_engine(sf_env)

    sql_query = f"""
    WITH rules AS (
        SELECT SITE_USE_ID, CAV_BU_ID,
               CASE
                   WHEN EXISTS (SELECT * FROM SERVICES_DB.SERVICES_IB_FBV.BV_N_IBSA_ACAT_INCLUXCLUATC_SETUP I
                                WHERE I.CUSTOMER_ID IN ({ACAT_CUST_IDs})
                                  AND INCEXCFLAG = 'INC'
                                  AND DATALEVEL = 'GU'
                                  AND I.GU_ID = GU.GU_ID) THEN 'Y'
                   ELSE 'N' END GU_INCLUDED,
               CASE
                   WHEN EXISTS (SELECT * FROM SERVICES_DB.SERVICES_IB_FBV.BV_N_IBSA_ACAT_INCLUXCLUATC_SETUP I
                                WHERE I.CUSTOMER_ID IN ({ACAT_CUST_IDs})
                                  AND INCEXCFLAG = 'INC'
                                  AND BU_STATUS = 'FULL'
                                  AND CUSTOMER_TYPE = 'CXEA'
                                  AND GU.CAV_BU_ID = I.BU_ID) THEN 'Y'
                   ELSE 'N' END CAV_BU_INCLUDED,
               CASE
                   WHEN EXISTS (SELECT * FROM SERVICES_DB.SERVICES_IB_FBV.BV_N_IBSA_ACAT_INCLUXCLUATC_SETUP I
                                WHERE I.CUSTOMER_ID IN ({ACAT_CUST_IDs})
                                  AND INCEXCFLAG = 'INC'
                                  AND DATALEVEL = 'PARTY'
                                  AND I.PARTY_ID = GU.CR_PARTY_ID) THEN 'Y'
                   WHEN EXISTS (SELECT * FROM SERVICES_DB.SERVICES_IB_FBV.BV_N_IBSA_ACAT_INCLUXCLUATC_SETUP I
                                WHERE I.CUSTOMER_ID IN ({ACAT_CUST_IDs})
                                  AND INCEXCFLAG = 'INC'
                                  AND BU_STATUS = 'PARTIAL'
                                  AND CUSTOMER_TYPE = 'CXEA'
                                  AND COALESCE(I.PARTY_ID, I.CR_PARTY_ID, I.HQ_CR_PARTY_ID) = GU.CR_PARTY_ID) THEN 'Y'
                   ELSE 'N' END CR_PARTY_INCLUDED,
               CASE
                   WHEN EXISTS (SELECT * FROM SERVICES_DB.SERVICES_IB_FBV.BV_N_IBSA_ACAT_INCLUXCLUATC_SETUP I
                                WHERE I.CUSTOMER_ID IN ({ACAT_CUST_IDs})
                                  AND INCEXCFLAG = 'INC'
                                  AND DATALEVEL = 'SITE'
                                  AND SITE_USE_CODE = 'INSTALL_AT'
                                  AND I.PARTY_ID = GU.CR_PARTY_ID) THEN 'Y'
                   ELSE 'N' END SITE_INCLUDED,
               CASE
                   WHEN EXISTS (SELECT * FROM SERVICES_DB.SERVICES_IB_FBV.BV_N_IBSA_ACAT_INCLUXCLUATC_SETUP I
                                WHERE I.CUSTOMER_ID IN ({ACAT_CUST_IDs})
                                  AND INCEXCFLAG = 'EXC'
                                  AND DATALEVEL = 'GU'
                                  AND I.GU_ID = GU.GU_ID) THEN 'Y'
                   ELSE 'N' END GU_EXCLUDED,
               CASE
                   WHEN EXISTS (SELECT * FROM SERVICES_DB.SERVICES_IB_FBV.BV_N_IBSA_ACAT_INCLUXCLUATC_SETUP I
                                WHERE I.CUSTOMER_ID IN ({ACAT_CUST_IDs})
                                  AND INCEXCFLAG = 'EXC'
                                  AND DATALEVEL = 'PARTY'
                                  AND I.PARTY_ID = GU.CR_PARTY_ID) THEN 'Y'
                   ELSE 'N' END CR_PARTY_EXCLUDED,
               CASE
                   WHEN EXISTS (SELECT * FROM SERVICES_DB.SERVICES_IB_FBV.BV_N_IBSA_ACAT_INCLUXCLUATC_SETUP I
                                WHERE I.CUSTOMER_ID IN ({ACAT_CUST_IDs})
                                  AND INCEXCFLAG = 'EXC'
                                  AND DATALEVEL = 'SITE'
                                  AND SITE_USE_CODE = 'INSTALL_AT'
                                  AND COALESCE(I.PARTY_ID, I.CR_PARTY_ID, I.HQ_CR_PARTY_ID) = GU.CR_PARTY_ID) THEN 'Y'
                   ELSE 'N' END SITE_EXCLUDED,
               CASE
                   WHEN NOT EXISTS (SELECT * FROM SERVICES_DB.SERVICES_IB_FBV.BV_N_IBSA_ACAT_INCLUXCLUATC_SETUP I
                                    WHERE I.CUSTOMER_ID IN ({ACAT_CUST_IDs})
                                      AND INCEXCFLAG = 'INC_ADD'
                                      AND INSTALL_AT_COUNTRY IS NOT NULL) THEN 'Y'
                   WHEN EXISTS (SELECT * FROM SERVICES_DB.SERVICES_IB_FBV.BV_N_IBSA_ACAT_INCLUXCLUATC_SETUP I
                                WHERE I.CUSTOMER_ID IN ({ACAT_CUST_IDs})
                                  AND INCEXCFLAG = 'INC_ADD'
                                  AND INSTALL_AT_COUNTRY = GU.COUNTRY) THEN 'Y'
                   ELSE 'N' END INSTALL_COUNTRY_INCLUDED,
               CASE
                   WHEN EXISTS (SELECT * FROM SERVICES_DB.SERVICES_IB_FBV.BV_N_IBSA_ACAT_INCLUXCLUATC_SETUP I
                                WHERE I.CUSTOMER_ID IN ({ACAT_CUST_IDs})
                                  AND INCEXCFLAG = 'INC_ADD'
                                  AND INSTALL_AT_COUNTRY = GU.COUNTRY) THEN 'Y'
                   ELSE 'N' END INSTALL_COUNTRY_EXCLUDED
        FROM CPS_DB.CPS_DSCI_BR.CAM_DS_SITE_GU_DENORM GU
        WHERE GU_ID IN ({GU_IDs})
    )
    SELECT SITE_USE_ID
    FROM rules
    WHERE (GU_INCLUDED = 'Y' OR CAV_BU_INCLUDED = 'Y' OR SITE_INCLUDED = 'Y' OR CR_PARTY_INCLUDED = 'Y') -- included by at least 1
      AND (INSTALL_COUNTRY_EXCLUDED != 'Y') -- not excluded by country
      AND (INSTALL_COUNTRY_INCLUDED = 'Y')  -- affirmed as country in
      AND (GU_EXCLUDED <> 'Y' OR SITE_EXCLUDED <> 'Y' OR CR_PARTY_EXCLUDED <> 'Y')
    """

    cn = check_env(sf_env)
    correct_schema = get_correct_schema(sf_env)
    engine = create_engine(
        sec.get_sf_pw(cn, flow_variables.warehouseXsmall, correct_schema)
    )

    # Loading into pandas
    try:
        with engine.connect() as connection:
            df = pd.read_sql(sql_query, connection)
            print(df.shape, df.head())
    except Exception as e:
        df = pd.DataFrame()
        print("An error occurred:", e)
    finally:
        # It is important to explicitly dispose of the engine once done
        engine.dispose()

    return df


## 3.5 Load cluster and existing ranking data
def load_cluster_data(GU_IDs):

#     sql_query = f"""
#         SELECT * FROM CPS_DB.CPS_DSCI_API.DEMO_MASTER_SITE_CLUSTERS
#         WHERE GU_ID IN ({GU_IDs})

#     """

    sql_query = f"""
        SELECT * FROM CPS_DB.CPS_DSCI_API.DC_MASTER_SITE
        WHERE GU_ID IN ({GU_IDs})
    
    """

    #engine = create_sf_connection_engine(CREDENTIALS, WAREHOUSE, ROLE, DB)

    #engine = create_sf_connection_engine(sf_env)

    cn = check_env(sf_env)
    correct_schema = get_correct_schema(sf_env)
    engine = create_engine(
        sec.get_sf_pw(cn, flow_variables.warehouseXsmall, correct_schema)
    )


    # Loading into pandas
    try:
        with engine.connect() as connection:
            df = pd.read_sql(sql_query, connection)

            ### NOTE - site_use_id comes with many nulls, as some site_use_ids were missing demo_master_site when then cluster was generated
            print("NOTE: Using install_site_id as site_use_id.")
            df = df.drop(['site_use_id'], axis=1)
            df = df.rename(columns = {"install_site_id": "site_use_id"})


            print(df.shape)
    except Exception as e:
        df = pd.DataFrame()
        print("An error occurred:", e)
    finally:
        # It is important to explicitly dispose of the engine once done
        engine.dispose()

    return df





def merge_cluster_acat_df(df_cluster, df_acat_site_ids):

    print("df_cluster       :", df_cluster.shape)
    print("df_acat_site_ids :", df_acat_site_ids.shape)

    # Perform a left merge
    df = pd.merge(df_cluster, df_acat_site_ids, on='site_use_id', how='left', indicator=True)

    #df = pd.merge(df_cluster, df_acat_site_ids, left_on = 'install_site_id', right_on='site_use_id', how='left', indicator=True)

    # Create the ACAT column
    df['acat'] = df['_merge'].apply(lambda x: 'Included' if x == 'both' else 'Not Included')

    # Drop the _merge column as it's no longer needed
    df = df.drop(columns=['_merge'])

    # add two extra columns master_address_site_id and acat_scope for later
    df.loc[:, 'master_address_site_id'] = np.nan

    df['acat_scope'] = np.nan
    df['acat_scope'] = df['acat'].replace({"Not Included":"invalid_acat", "Included": "valid_acat"})

    print("DF Merged        :", df.shape)

    return df


## 3.7 De-preference sites that are NOT included in ACAT
def de_preference_acat_not_included_sites(df):

    df.loc[df['acat'] == 'Not Included', 'weighted_score'] -= 99999999999

    return df


## 3.8 Re-rank, update Master Sites, and Re-prune Secondary Sites


def prune_low_quality_address(df, PRUNING_PENALTIES):

    # Set geo distance jitter threshold
    geo_jitter_threshold_value  =  PRUNING_PENALTIES['geo_jitter_threshold_value'] #30000

    # Initialize the new columns with default values
    df.loc[:, 'prune']                 = False
    df.loc[:, 'prune_explanation']     = ''
    df.loc[:, 'address_quality']       = 1.0  # Default to 1 for all rows initially

    # Define penalties for various conditions
    penalty_high_geo_jitter_diff = PRUNING_PENALTIES['penalty_high_geo_jitter_diff'] #0.2
    penalty_cleansing_status     = PRUNING_PENALTIES['penalty_cleansing_status'] #0.2
    penalty_has_holds            = PRUNING_PENALTIES['penalty_has_holds'] #0.1
    penalty_addr_type            = PRUNING_PENALTIES['penalty_addr_type'] #0.2
    penalty_completeness_status  = PRUNING_PENALTIES['penalty_completeness_status'] #0.2
    penalty_party_id_mismatch    = PRUNING_PENALTIES['penalty_party_id_mismatch'] #0.1


    # Get the master addresses with their party_id and cluster label
    master_addresses = df[df['is_master_address']][['party_id', 'dbscan_cluster_label']]
    #print(master_addresses)

    # helper function to determine the prune reason and calculate address quality
    def assess_row(row, master_party_id):

        reasons = []
        quality = 1.0 if row['is_master_address'] else 1.0  # Master address has a default quality score of 1

        # Check for bad geo_jitter_diff_mean_meter
        if row['geo_jitter_diff_mean_sqr_meter'] >= geo_jitter_threshold_value:  # Define the threshold_value as appropriate
            reasons.append("geo_jitter_diff_mean_sqr_meter: HIGH " + str(row['geo_jitter_diff_mean_sqr_meter']))
            quality -= penalty_high_geo_jitter_diff

        # Check for bad cleansing_status
        if row['cleansing_status'] in ['NOT_CLEANSED', None, 'None', 'SYSTEM_ERROR']:
            reasons.append('cleansing_status: ' + str(row['cleansing_status']))
            quality -= penalty_cleansing_status

        # Check for has_holds
        if row['has_holds'] == 'ON HOLD':
            reasons.append('has_holds: ' + str(row['has_holds']))
            quality -= penalty_has_holds

        # Check for bad addr_type
        if row['addr_type'] in [None, 'None', 'Locality', 'Postal', 'PostalLoc']:
            reasons.append('addr_type: ' + str(row['addr_type']))
            quality -= penalty_addr_type

        # Check for bad completeness_status
        if row['completeness_status'] in ['INCOMPLETE', None, 'None']:
            reasons.append('completeness_status: ' + str(row['completeness_status']))
            quality -= penalty_completeness_status

        # Check for party_id match
        if (len(reasons)>0) and (not row['is_master_address'] and row['party_id'] != master_party_id):
            reasons.append(f'party_id_mismatch: {row["party_id"]} did not match master_party_id: {master_party_id}')
            quality -= penalty_party_id_mismatch

        # Ensure quality is within the range [0, 1]
        quality = max(min(quality, 1.0), 0.0)

        return ', '.join(reasons), quality

    # Iterate over the DataFrame
    for index, row in df.iterrows():

        # Skip the master address itself
        if row['is_master_address']:
            continue
        # Skip if  ib_count_coverable is zero
        if row['ib_count_coverable'] <= 0:
            continue
        # Get the master address's party_id for the current cluster
        master_party_id = master_addresses.loc[
            master_addresses['dbscan_cluster_label'] == row['dbscan_cluster_label'], 'party_id'
        ].iloc[0]  # there's only one master address per cluster
        #print(row['party_id'], master_party_id)

        # Assess the row and get prune reason and address quality
        prune_reason, address_quality = assess_row(row, master_party_id)
        #print(prune_reason)

        # Set prune flag and explanation if there's a reason. Party id must not match
        if (prune_reason) and (row['party_id'] != master_party_id):
                df.at[index, 'prune'] = True
                df.at[index, 'prune_explanation'] = prune_reason

        # Set the address quality
        df.at[index, 'address_quality'] = address_quality

    return df



def update_master_address(df, GU_ID):

    print("GU_ID:", GU_ID)
    SITE_USE_ORG_IDS = list(df[df['gu_id'] == GU_ID]['site_use_org_id'].unique())

    print("SITE_USE_ORG_IDS:", SITE_USE_ORG_IDS)

    if len(SITE_USE_ORG_IDS) == 0:
        print(f"No SITE_USE_ORG_IDs found for GU_ID: {GU_ID}. Continuing to next GU_ID.")
        return df

    for SITE_USE_ORG_ID in SITE_USE_ORG_IDS:
        print(f"PROCESSING GU_ID : {GU_ID} AND OU_ID: {SITE_USE_ORG_ID}")

        # Get unique cluster labels for the current GU_ID and SITE_USE_ORG_ID
        cluster_labels = df[(df['gu_id'] == GU_ID) & (df['site_use_org_id'] == SITE_USE_ORG_ID)]['dbscan_cluster_label'].dropna().unique()

        for cluster_label in cluster_labels:
            # Filter the DF for the current cluster label
            df_temp_with_scores = df[(df['gu_id'] == GU_ID) &
                                     (df['site_use_org_id'] == SITE_USE_ORG_ID) &
                                     (df['dbscan_cluster_label'] == cluster_label)]

            # If scores were calculated, select the master address - for weighted_score and master site
            if not df_temp_with_scores.empty:
                # Sort by weighted_score in descending order
                df_temp_with_scores = df_temp_with_scores.sort_values(by='weighted_score', ascending=False)

                # Find the row with the highest score where acat is 'Included' and weighted_score is positive
                master_address_row = df_temp_with_scores[ (df_temp_with_scores['acat'] == 'Included') ] #& (df_temp_with_scores['weighted_score'] > 0)


                if not master_address_row.empty:
                    master_address_row     = master_address_row.iloc[0]
                    master_address         = master_address_row['place_addr']
                    master_coordinates     = [master_address_row['latitude'], master_address_row['longitude']]
                    master_address_site_id = master_address_row['site_use_id']

                    # Get the indices of the rows in the cluster
                    cluster_indices = df_temp_with_scores.index

                    # Update the master address information for all rows in the cluster
                    df.loc[cluster_indices, 'is_master_address']      = df.loc[cluster_indices].index == master_address_row.name
                    df.loc[cluster_indices, 'master_address']         = master_address
                    df.loc[cluster_indices, 'master_coordinates']     = str(master_coordinates)
                    df.loc[cluster_indices, 'master_address_site_id'] = master_address_site_id

                    # add one more field - acat scope - valid acat or invalid acat and no-acat definition provided
                else:
                    # If no 'Included' acat with positive score, set the master address information to NaN
                    cluster_indices = df_temp_with_scores.index
                    ## NOTE: this is just reference the master sites for invalid acats
                    #master_address_row_acat_valid_invalid = df_temp_with_scores.iloc[0]  # delete it if needed
                    #master_address_site_id_acat_invalid   = master_address_row_acat_valid_invalid['site_use_id'] #master_address_row['site_use_id'] # delete it if needed
                    #df.loc[cluster_indices, 'master_address_site_id'] = master_address_site_id_acat_invalid

#                     df.loc[cluster_indices, 'is_master_address']      = np.nan
#                     df.loc[cluster_indices, 'master_address']         = np.nan
#                     df.loc[cluster_indices, 'master_coordinates']     = np.nan
                    df.loc[cluster_indices, 'master_address_site_id'] = np.nan


            # Filter the DF for the current cluster label for pruning
            df_temp_w_master_addr = df[(df['gu_id'] == GU_ID) &
                                       (df['site_use_org_id'] == SITE_USE_ORG_ID) &
                                       (df['dbscan_cluster_label'] == cluster_label)]

            if (not df_temp_w_master_addr.empty) and (not master_address_row.empty):
                df_temp_w_master_addr = prune_low_quality_address(df_temp_w_master_addr, PRUNING_PENALTIES)

                # Update the prune information for all rows in the cluster
                df.loc[df_temp_w_master_addr.index, 'prune'] = df_temp_w_master_addr['prune']
                df.loc[df_temp_w_master_addr.index, 'prune_explanation'] = df_temp_w_master_addr['prune_explanation']
                df.loc[df_temp_w_master_addr.index, 'address_quality'] = df_temp_w_master_addr['address_quality']


    # Convert the 'weighted_score' column to numeric, coercing errors to NaN
    #df['master_address_site_id'] = pd.to_numeric(df['master_address_site_id'], errors='coerce')

    # Convert the column to Int64 type, which supports NaN
    df['master_address_site_id'] = df['master_address_site_id'].astype('Int64')


#     # Select columns and rearrange  order
#     df = df[['customer_name', 'gu_name', 'gu_id', 'site_use_id', 'cr_party_id', 'party_id', 'customer_id', 'address_id',
#                             'site_use_org_id', 'site_business_entity',

#      # address related columns
#      'address1', 'address2', 'address3','address4', 'parsed_street_number', 'city', 'state','country','orig_country', 'postal_code', 'parsed_extra6','parsed_street_name', 'parsed_country_desc',  'short_label',
#      #address quality
#      'cleansing_message', 'cleansing_status', 'completeness_status','has_holds',
#      #geo
#       'x_min', 'x_max', 'y_min', 'y_max','x_diff', 'y_diff', 'x_diff_hs', 'y_diff_hs', 'geo_jitter_diff_mean_sqr_meter',
#      # fields to compute weighted score
#       'ib_count_covered', 'ib_count_uncovered', 'ib_count_never_covered', 'ib_count_total', 'ib_count_coverable', 'geo_precision','geo_precision_inverted',
#      'rank','location_score',

#      # main fields
#      'place_addr','addr_type', 'longitude','latitude',

#    'dbscan_average_euclidean_distance_from_centroid_point',
#    'dbscan_average_haversine_distance_to_centroid_meter',
#    'dbscan_average_location_score',
#    'dbscan_cluster_centroid_point',

#    'dbscan_average_haversine_distance_within_cluster_meter',
#     # master site
#     'is_master_address','acat_scope', 'master_address_site_id',
#    'master_address', 'master_coordinates', 'dbscan_number_of_addresses_in_cluster', 'dbscan_cluster_label', 'weighted_score',

#     # Prune
#     'prune', 'prune_explanation', 'address_quality', 'acat'

#                             ]]

    df = df.sort_values(by=['gu_id', 'site_use_org_id', 'dbscan_cluster_label', 'weighted_score'], ascending=False)




    return df


## 3.9 End-to-end - ACAT vs Regular (Non_ACAT) Ranking
def acat_ranking(ACAT_CUST_IDs, GU_IDs, GU_ID_LIST):

    # 3.4 Load ACAT site IDs
    df_acat_site_ids = load_acat_site_ids(ACAT_CUST_IDs, GU_IDs)

    if len(df_acat_site_ids) == 0:
        print(f"No ACAT site IDs found for the ACAT_CUST_ID(s) {ACAT_CUST_IDs}!. All the records will be tagged as acat_scope=invalid_acat!")

    # 3.5 Load cluster and existing ranking Data
    df_cluster = load_cluster_data(GU_IDs)

    if len(df_cluster) == 0:
        print(f"No master site cluster data found for the GU(s) {GU_IDs}! Skipping to another GU ID...")
        df = pd.DataFrame()
        return df

    # 3.6 Merge cluster df (left) and ACAT site df (right) - left merge
    df = merge_cluster_acat_df(df_cluster, df_acat_site_ids)

    # 3.7 De-preference sites that are NOT included in ACAT
    df = de_preference_acat_not_included_sites(df)

    # 3.8 Re-rank, update Master Sites, and Re-prune Secondary Sites - NOTE: do paralle processing
    for GU_ID in GU_ID_LIST:

        print(f"processing {GU_ID}. DF {df.shape}" )

        df    = update_master_address(df, GU_ID)


    # Create the site_classification column based on is_master_address
    df['site_classification'] = df['is_master_address'].apply(
        lambda x: 'primary site' if x is True else ('secondary site' if x is False else 'stand alone site')
    )

    df.loc[df['acat_scope'] == 'invalid_acat', 'master_address_site_id'] = np.nan


    return df



def _assign_master_address_site_id(df):

    # Identify the master address for each cluster
    master_addresses = df[df['is_master_address'] == True][['gu_id', 'site_use_org_id', 'dbscan_cluster_label', 'site_use_id']]
    master_addresses = master_addresses.rename(columns={'site_use_id': 'master_address_site_id'})

    # Merge the master addresses back to the original DataFrame
    df = df.merge(master_addresses, on=['gu_id', 'site_use_org_id', 'dbscan_cluster_label'], how='left')

    # Update the master_address_site_id column
    df['master_address_site_id'] = df['master_address_site_id_y'].combine_first(df['site_use_id'])
    df = df.drop(columns=['master_address_site_id_y'])

    df['master_address_site_id'] = df['master_address_site_id'].astype(int)

    df = df.drop(['master_address_site_id_x'], axis=1)

    return df


def regular_ranking(ACAT_CUST_IDs, GU_IDs):

    # 3.5 Load cluster and existing ranking Data
    df = load_cluster_data(GU_IDs)

    if len(df) == 0:
        print(f"\nNo master site cluster data found for the GU(s) {GU_IDs}!. Skipping to another GU ID...")
        df = pd.DataFrame()
        return df

    # add two extra columns master_address_site_id and acat_scope for later
    df.loc[:, 'master_address_site_id'] = np.nan
    df['acat']       = "Not Applicable"
    df['acat_scope'] = 'no_acat_definition_provided'

    df = _assign_master_address_site_id(df)

    # Create the site_classification column based on is_master_address
    df['site_classification'] = df['is_master_address'].apply(
        lambda x: 'primary site' if x is True else ('secondary site' if x is False else 'stand alone site')
    )

    df = df.sort_values(by=['gu_id', 'site_use_org_id', 'dbscan_cluster_label', 'weighted_score'], ascending=False)

    print(df.shape)

    return df


# 4. Process One Engagement ID

def process_one_eng(ENG_ID):

    #update_model_status(process_date, ENG_ID, 'running', RUN_DIR)

    try:
        start_time = datetime.now()

        # 3.3 Load mapping data - one engagement ID
        df_mapping = load_eng_gu_acat_id_mapping(ENG_ID)

        print("ENG_ID:", ENG_ID)

        print("df_mapping:", df_mapping.head())

        GU_IDs, ACAT_CUST_IDs, GU_ID_LIST = get_mapping_ids(df_mapping)

        #keep_processing = True
        # if no gu id found, nothing to process
        if len(GU_ID_LIST) == 0:
            print(f"No GU IDs found for the engagement id {ENG_ID}. Skipping to another engagement.")
            #keep_processing = False
            #print("keep_processing:", keep_processing)
            return

        if (ACAT_CUST_IDs == '') or (ACAT_CUST_IDs == np.nan) or (ACAT_CUST_IDs == None):
            # regular NON_ACAT Ranking
            print(f"No ACAT CUSTOMER IDs found for the engagement id {ENG_ID}. Running REGULAR (NON_ACAT) ranking...")
            df_final = regular_ranking(ACAT_CUST_IDs, GU_IDs)

        else:
            # ACAT Ranking
            print(f"ACAT CUSTOMER IDs found for the engagement id {ENG_ID}. Running ACAT ranking...")
            df_final = acat_ranking(ACAT_CUST_IDs, GU_IDs, GU_ID_LIST)


        df_final = df_final.rename(columns={"master_address_site_id": "master_site_id",
                                            "site_classification"   : "customer_site_classification"})


        all_columns = ['gu_id', 'site_use_org_id', 'dbscan_cluster_label', 'customer_name', 'gu_name', 'site_business_entity', 'place_addr',

        # master site - major
        'site_use_id', 'master_site_id', 'customer_site_classification', 'acat_scope','weighted_score',
        # master site   - optional
        'is_master_address', 'master_address', 'master_coordinates', 'dbscan_number_of_addresses_in_cluster',
        # Prune
        'prune', 'prune_explanation', 'address_quality',
        # fields to compute weighted score
        'ib_count_covered', 'ib_count_uncovered', 'ib_count_never_covered', 'ib_count_total', 'ib_count_coverable',
        #'geo_precision','geo_precision_inverted', 'rank','location_score',

        # address related columns
         'address1', 'address2', 'address3','address4', 'parsed_street_number', 'city', 'state','country','orig_country', 'postal_code', 'parsed_extra6','parsed_street_name', 'parsed_country_desc',  'short_label',
         #address quality
         'cleansing_message', 'cleansing_status', 'completeness_status','has_holds',
         #geo
          #'x_min', 'x_max', 'y_min', 'y_max','x_diff', 'y_diff', 'x_diff_hs', 'y_diff_hs', 'geo_jitter_diff_mean_sqr_meter',

         # main fields
         'addr_type', 'longitude','latitude',
         'cr_party_id', 'party_id', 'customer_id', 'address_id',

         #'dbscan_average_euclidean_distance_from_centroid_point',
         #'dbscan_average_haversine_distance_to_centroid_meter',
         #'dbscan_average_location_score',
         #'dbscan_cluster_centroid_point',

         'dbscan_average_haversine_distance_within_cluster_meter',
         'acat' ]


        # if a column is missing, fill with None
        df_final = align_dataframe_columns(df_final, all_columns)

        # Select columns and rearrange  order
        df_final = df_final[all_columns]

        df_final = df_final.reset_index(drop=True)

        print("DF Final:", df_final.shape)

        #update_model_status(process_date, ENG_ID, 'completed', RUN_DIR) # running, completed, failed

        #print("DONE")

        end_time = datetime.now()
        print('Process One Engagement Duration: {}'.format(end_time - start_time))

        return df_final, GU_IDs, ACAT_CUST_IDs


    except Exception as e:
        # Capture the full traceback
        error_traceback = traceback.format_exc()
        print(f"An error occurred: {e}\nFull traceback:\n{error_traceback}")

        # write the error with traceback to a log file
        with open('error_log.txt', 'a') as f:
            f.write(f"Error processing ENG_ID {ENG_ID}: {e}\nFull traceback:\n{error_traceback}\n")

        #update_model_status(process_date, ENG_ID, 'failed', RUN_DIR) # running, completed, failed

        # Re-raise the exception to propagate it to the main thread
        #raise

        return None, None, None


# 5. Metrics

def load_cluster_metrics_data(GU_IDs):

    sql_query = f"""
        SELECT * FROM CPS_DB.CPS_DSCI_API.DEMO_MASTER_SITE_CLUSTERS_METRICS
        WHERE GU_ID IN ({GU_IDs})
    
    """

    #engine = create_sf_connection_engine(sf_env)
    cn = check_env(sf_env)
    correct_schema = get_correct_schema(sf_env)
    engine = create_engine(
        sec.get_sf_pw(cn, flow_variables.warehouseXsmall, correct_schema)
    )

    # Loading into pandas
    try:
        with engine.connect() as connection:
            metrics_df = pd.read_sql(sql_query, connection)
            print(metrics_df.shape)

            metrics_df.loc[:, 'timestamp'] = pd.to_datetime(metrics_df['timestamp'])

            #metrics_df = metrics_df.sort_values(by=['timestamp'], ascending=False)

            # Sort the DataFrame by 'gu_id', 'site_use_org_id', and 'timestamp' in descending order
            metrics_df = metrics_df.sort_values(by=['gu_id', 'site_use_org_id', 'timestamp'], ascending=[True, True, False])

            # Drop duplicates, keeping the first occurrence (which is the latest due to sorting)
            metrics_df = metrics_df.drop_duplicates(subset=['gu_id', 'site_use_org_id'], keep='first')
    except Exception as e:
        metrics_df = pd.DataFrame()
        print("An error occurred:", e)
    finally:
        # It is important to explicitly dispose of the engine once done
        engine.dispose()

    return metrics_df


def metrics(ENG_ID,GU_IDs,  ACAT_CUST_IDs, updated_df_full_master_sites):

    print("Loading metrics df...")
    metrics_df = load_cluster_metrics_data(GU_IDs)

    print("Getting stats...")

    site_use_org_ids = list(updated_df_full_master_sites['site_use_org_id'].unique())

    #updated_df_full_master_sites.dropna(subset=[''])


    num_total_sites = updated_df_full_master_sites.shape[0]
    print("Number of total sites/records:", num_total_sites)


    num_sites_with_cluster_label = updated_df_full_master_sites.dropna(subset=['dbscan_cluster_label']).shape[0]
    print("Number of sites with a cluster label:", num_sites_with_cluster_label)

    num_sites_with_cluster_label_prcnt = round((num_sites_with_cluster_label/num_total_sites)*100, 2)

    print(f"Percentage of sites with a cluster a label {num_sites_with_cluster_label_prcnt}%")

    num_of_pruned_sites = updated_df_full_master_sites[updated_df_full_master_sites['prune']== True].shape[0]
    print("Number of pruned sites:", num_of_pruned_sites)


    number_of_primary_sites = updated_df_full_master_sites[updated_df_full_master_sites['customer_site_classification']== "primary site"].shape[0]
    print("Number of primary sites:", number_of_primary_sites)

    cluster_compression = f"From {num_sites_with_cluster_label} To {number_of_primary_sites} sites"

    print(f"Cluster compression: From {num_sites_with_cluster_label} To {number_of_primary_sites} sites")

    cluster_compression_prcnt = round(100 - (number_of_primary_sites/num_sites_with_cluster_label),2)
    print(f"Cluster compression rate: {cluster_compression_prcnt} %")

    site_compression_prcnt =  round( (1 - ((num_total_sites + (number_of_primary_sites-num_sites_with_cluster_label))/num_total_sites)) * 100, 2)
    print(f"Site compression rate: {site_compression_prcnt} %")


    stats = {
        "dc_engagement_id":ENG_ID,
        "global_ultimate_ids": GU_IDs,
        "acat_customer_ids":ACAT_CUST_IDs,


        "number_site_use_org_ids": len(site_use_org_ids),
        "num_total_sites": num_total_sites,
        "num_sites_with_cluster_label": num_sites_with_cluster_label,
        "num_sites_with_cluster_label_prcnt": num_sites_with_cluster_label_prcnt,
        "num_of_pruned_sites": num_of_pruned_sites,
        "number_of_primary_sites": number_of_primary_sites,
        "cluster_compression": cluster_compression,
        "cluster_compression_prcnt": cluster_compression_prcnt,
        "site_compression_prcnt": site_compression_prcnt


    }

    stats_df = pd.DataFrame([stats])

    stats_df.columns = [col.upper() for col in stats_df.columns]
    metrics_df.columns = [col.upper() for col in metrics_df.columns]



#     print("Storing...")
#     filename = f"{RUN_DIR_RESULTS}{ENG_ID}_customer_site_list.xlsx"
#     print(filename)

    # Create a Pandas Excel writer using XlsxWriter as the engine.
#     with pd.ExcelWriter(filename, engine='xlsxwriter') as writer:
#         # Write each DataFrame to a different worksheet.
#         updated_df_full_master_sites.to_excel(writer, sheet_name='customer_site_list', index=False)
#         stats_df.to_excel(writer, sheet_name='stats_eng', index=False)
#         metrics_df.to_excel(writer, sheet_name='stats_gu_ou', index=False)

        # Close the Pandas Excel writer and output the Excel file.
        #writer.save()

    stats_df   = stats_df.reset_index(drop=True)
    metrics_df = metrics_df.reset_index(drop=True)

    return stats_df, metrics_df



### PREFECT ###
###############

@task(log_stdout=True)
def demo_cognito_api_auth(env,service_name,region_name):
    secret_id = f"{env}/Cognito"


    session = boto3.session.Session()
    client_ssm = session.client(
        service_name=service_name,
        region_name=region_name,
        
        
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


# def check_env(env):
#     if env == "dev":
#         cn = "dev_cps_dsci_etl_svc"
#     elif env == "stage":
#         cn = "stg_cps_dsci_etl_svc"
#     elif env == "prod":
#         cn = "prd_cps_dsci_etl_svc"
#     else:
#         cn = env
#     return cn

@task(log_stdout=True)
def get_flow_params(sf_env,engagement_id,request_id,requested_by, demo_cognito_api_auth,notification_id) -> RunSettings:
    return config.FlowParams(engagement_id,sf_env,request_id,requested_by, demo_cognito_api_auth,notification_id)

@task(log_stdout=True)
def get_run_settings() -> RunSettings:
    return config.RunSettings()

def get_sec_dir(pth):
    return os.path.join(os.getcwd(), pth)

@task(log_stdout=True, nout= 3)
def parse_request_json(request_json):
    print(type(request_json))
    dc_engagement_id = request_json['engagement_id']


    return  dc_engagement_id

@task(log_stdout=True)
def get_user_id(requested_by,env):
    correct_schema = get_correct_schema(env)

    sf_env = check_env('prod')
    engine = create_engine(
        sec.get_sf_pw(sf_env, flow_variables.warehouseXsmall, correct_schema)
    )


    # tagsest_qry = f"""select tag_id, tagset_id from DC_TAGS where TAG_ID in (649,639,640,628,582, 13714, 581, 1414)"""
    tagsest_qry = f"""select * from {correct_schema}.DC_USERS where CISCO_CCO_ID = '{requested_by}'"""

    user_id_df = pd.read_sql(tagsest_qry,engine )

    return int(user_id_df['user_id'][0])

@task()
def get_global_ultimate(flow_params,run_settings):
    correct_schema = get_correct_schema(flow_params.sf_env)

    sf_env = check_env('prod')
    engine = create_engine(
        sec.get_sf_pw(sf_env, flow_variables.warehouseXsmall, correct_schema)
    )


    gu_qry = f"""select distinct h.GLOBAL_ULTIMATE_ID
                        from CPS_DSCI_API.DC_ENGAGEMENT_HDR e
                                  join CPS_DSCI_API.DC_PARTY_LINKS p  on (p.DC_ENGAGEMENT_ID = e.DC_ENGAGEMENT_ID and p.IS_DELETED = 'F')
                                  join EDW_MASTER_ETL_DB.SS.CRT_XXNGCR_DNM_RELATIONSHIPS h on (h.PARTY_ID=p.CR_PARTY_ID)
                        where e.IS_DELETED = 'F' and e.DC_ENGAGEMENT_ID = {flow_params.engagement_id} and nvl(h.edwsf_source_deleted_flag,'N') = 'N' """

    gu_df  = pd.read_sql(gu_qry,engine )

    return int(gu_df['global_ultimate_id'][0])

@task( log_stdout=True, tags=["snowflake_xsmall"])
def get_df_for_api_call(flow_params, run_settings):
    cn = check_env('prod')
    correct_schema = get_correct_schema(flow_params.sf_env)

    engine = create_engine(
        sec.get_sf_pw(cn, run_settings.wh_xsmall, correct_schema)
    )

    con = engine.connect()
    date_created = datetime.now().isoformat()
    print("starting get_df_for_api_call")

    print(flow_params.engagement_id)

    query = f"""
                with DISCOVERY AS (with dc_needs as
                             (select ACAT_CUSTOMER_ID, l.DC_ENGAGEMENT_ID
                              from DC_ACAT_LINKS l
                                       join DC_ENGAGEMENT_HDR h
                                            on (h.DC_ENGAGEMENT_ID = l.DC_ENGAGEMENT_ID)
                              where l.is_deleted = 'F'
                                and h.is_deleted = 'F'
                                and l.DC_ENGAGEMENT_ID = {flow_params.engagement_id}),
                         latest as
                             (select d.REQUEST_ID, d.CUSTOMER_ID, d.LAST_UPDATE_DATE, count(0) as cnt
                              FROM SERVICES_DB.SERVICES_IB_FBV.BV_IBSA_ACAT_DISCOVERY_SUM D
                                       join SERVICES_DB.SERVICES_IB_FBV.BV_IBSA_ACAT_CUSTOMER_MASTER m
                                            on (m.CUSTOMER_ID = D.CUSTOMER_ID)
                                       join dc_needs on (dc_needs.ACAT_CUSTOMER_ID = D.CUSTOMER_ID)
                                       left join SERVICES_DB.SERVICES_IB_FBV.BV_IBSA_ACAT_DISCOVERY_DATA a
                                                 on (d.REQUEST_ID = a.ACAT_REQUEST_ID and
                                                     m.CUSTOMER_ID = SWEEPS_CUSTOMER_NUMBER)
                              where d.TOTAL_LINES > 0
                                and d.REQUEST_TYPE in ('ON-DEMAND', 'Discovery(System)')
                                and d.data_purged like 'RETAIN%'
                              group by d.REQUEST_ID, d.CUSTOMER_ID, d.LAST_UPDATE_DATE),
                         ranked as
                             (select REQUEST_ID,
                                     CUSTOMER_ID,
                                     LAST_UPDATE_DATE,
                                     cnt,
                                     rank() over ( partition by CUSTOMER_ID order by LAST_UPDATE_DATE desc ) as orderv
                              from latest
                              where cnt > 5),
                         picked as
                             (select distinct ranked.*
                              from DC_ACAT_LINKS l
                                       left join ranked on (l.ACAT_CUSTOMER_ID = ranked.CUSTOMER_ID)
                              where orderv = 1)
                    select UNCOVERED_CATEGORY      as ACAT_UNCOVERED_CATEGORY,
                           REASON_CODE             as ACAT_REASON_CODE,
                           EXCLUDE_FLAG            as ACAT_EXCLUDE_FLAG,
                           EARLIEST_DISCOVERY_DATE as ACAT_EARLIEST_DISCOVERY_DATE,
                           picked.LAST_UPDATE_DATE as ACAT_LAST_UPDATE_DATE,
                           C.*
                    from SERVICES_DB.SERVICES_IB_FBV.BV_IBSA_ACAT_DISCOVERY_DATA d
                             join picked on (picked.CUSTOMER_ID = d.SWEEPS_CUSTOMER_NUMBER and
                                             picked.REQUEST_ID = d.ACAT_REQUEST_ID)
                             JOIN CPS_DSCI_API.DAILY_IB_2024_10_08 C ON (C.INSTANCE_ID = D.instance_id)
                    WHERE ACAT_UNCOVERED_CATEGORY NOT IN ('COVERED')
                      AND EXCLUDE_FLAG IN ('N')
        ), STD_TAGS AS (
            -- current validated IB tags
            SELECT INSTANCE_ID, TT.TAG_ID,TT.TAG_NAME
            FROM DC_ENGAGEMENT_TAGS_{flow_params.engagement_id} T
            JOIN DC_TAGS TT ON ( TT.TAG_ID = T.TAG_ID)
                WHERE T.TAGSET_ID = 267 AND T.IS_DELETED = 'F'
    ),prior_coverage AS (
    select INSTANCE_ID,
           listagg(distinct SERVICE_LEVEL,',') as service_level_history,
           listagg(distinct concat(CONTRACT_NUMBER, '-', SERVICE_PARTNER, '(', STS_CODE , ')'),',') as contract_number_history,
           max(LAST_COVERAGE_DATE)
    from CPS_DSCI_API.daily_coverage_contract_2024_10_08
    where CONTRACT_NUMBER in
    (
    SELECT CONTRACT_NUMBER::varchar
    FROM DC_MANAGED_SERVICE_CONTRACTS
    where DC_ENGAGEMENT_ID = {flow_params.engagement_id}  -- got lucky here so pick 1 or more explicitly

    )
    group by INSTANCE_ID
)
    SELECT d.*,
           coverage_status, acat_earliest_discovery_date, acat_uncovered_category,
           CASE
                WHEN STD_TAGS.TAG_ID IN (1381,1379,1380,1382 ) THEN 'IN SCOPE'
                WHEN STD_TAGS.TAG_ID IN (1358, 1360 ) THEN 'OUT OF SCOPE'
                WHEN STD_TAGS.TAG_ID IN (1367,1362,1361,1359) THEN 'CONFLICT'

               WHEN STD_TAGS.TAG_ID IN (1364) THEN 'TBD IN DIRECTION'

                WHEN STD_TAGS.TAG_ID IN (1363,11176) THEN 'TBD NO CLEAR DIRECTION'
                    ELSE 'NOT IB TAGGED' END AS ITEM_TAG_REASON,  -- any IN SCOPE are pretty much AUTO GO  DARK GREEN
            STD_TAGS.TAG_NAME as VALIDATED_IB_TAG_NAME,
            prior_coverage.contract_number_history,
            prior_coverage.service_level_history

    FROM DISCOVERY D left join STD_TAGS on (STD_TAGS.INSTANCE_ID=d.INSTANCE_ID )
    left join prior_coverage on ( prior_coverage.INSTANCE_ID=d.INSTANCE_ID )

  """

    with engine.begin() as conn:
        acat_disc_df = pd.read_sql(query, conn)

    print("finished get_df_for_api_call")

    return acat_disc_df


@task(log_stdout=True,trigger=all_finished )
def make_api_call_to_notifications(run_settings,dc_engagement_id,auth_token, logged_user, env,request_id,user_id,flow_params):
    res_log = []
    stats_json = {
        "excel_location" : flow_params.excel_output_uri
    }

    params_list = [
        {
            "tree_id": 502,
            "notification_category": "result",
            "subject": "Site report",
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

    headers = {'Authorization': f'Bearer {auth_token}', 'Content-Type': 'application/json'}

    r = requests.post(full_request_uri,
                        headers=headers, verify=False, json =params_list )

    log_to_dc_job_messages(env, request_id,
                           f"INFO: Completed API call {iter} for {dc_engagement_id} with response : Status Code: {r.status_code}",
                           flow_params.requested_by, flow_params.notification_id)
    print(f"Status Code: {r.status_code}, Response: {r.json()}")
    res = r.json()
    res_log.append(res)

    print(res_log)
    # build_error_response.run(res_log, flow_params, request_id)


    raise SUCCESS()
    return res_log




storage_obj = Docker(
    # base_image="containers.cisco.com/ejurotic/prefect_15_13_python_3_8",
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
        "SQLAlchemy===1.4.35",
        "awswrangler==2.12.1",
        "fastparquet==0.7.2",
        "XlsxWriter==3.1.2",
        "oyaml==1.0",
        "networkx==2.8",
        "binpacking==1.5.2",
        "cloudpickle==2.0.0"




    ],
    # registry_url="containers.cisco.com/ejurotic",
    registry_url="837578041534.dkr.ecr.us-east-1.amazonaws.com/dc/p1",
    path="main.py",
    files={
        get_sec_dir('common/new_bulkload.py') : "/common/new_bulkload.py",
        get_sec_dir('common/sec.py') : "/common/sec.py",
        get_sec_dir('common/config.py') : "/common/config.py",
        get_sec_dir('flow_variables.py'): "/flow_variables.py",
        get_sec_dir('common/sql_pool.py'): "/common/sql_pool.py",
        get_sec_dir('main.py'): "main.py",
        get_sec_dir('wb.py'): "wb.py",
        get_sec_dir("log_to_dc_job_messages.py"): "/log_to_dc_job_messages.py",

    },
    secrets=["AWS_CREDENTIALS"],
    env_vars={"PYTHONPATH": "${PYTHONPATH}:/"},
    stored_as_script=True,
    # ignore_healthchecks=True,

)


with Flow(
    "dc-acat-discovery",
    storage=storage_obj,
        run_config=KubernetesRun(
            # memory_request="1024Mi",
            # memory_limit="2048Mi",
            # cpu_request="1000m",
            # cpu_limit="2000m",
            # service_account_name="builder",
            labels=["dev"]
        ),
    # run_config=DockerRun(labels=["thought-spot", "ds-server-docker"]),
    executor=LocalExecutor(),
    # executor=LocalDaskExecutor(scheduler="processes", num_workers=8),
    result=S3Result(bucket="cam-prefect-results")
) as flow:
    env = Parameter("env", required=True)
    request_id = Parameter("request_id", required=True),
    request_json = Parameter("request_json", required=True),
    requested_by = Parameter("requested_by", required=True)
    notification_id = Parameter("notification_id", required=False, default=0)
    dc_engagement_id  = parse_request_json(request_json[0])
    user_id = get_user_id(requested_by, env)
    run_settings = get_run_settings(upstream_tasks=[dc_engagement_id ])



    demo_cognito_api_auth_result = demo_cognito_api_auth(env,
        region_name = "us-east-1", service_name="secretsmanager", upstream_tasks=[run_settings]
    )

    flow_params = get_flow_params(env,dc_engagement_id,request_id[0],requested_by, demo_cognito_api_auth_result,notification_id)



    acat_disc_df = get_df_for_api_call(
        flow_params=flow_params,
        run_settings=run_settings,
        upstream_tasks=[demo_cognito_api_auth_result],
    )


    excel_uri = wb.package_workbook(
        site_report_df = acat_disc_df,
        flow_params    = flow_params,
        run_settings   = run_settings,
        upstream_tasks = [acat_disc_df],
    )


    all_done = final_flow_state_message(env, notification_id, requested_by,flow_params, upstream_tasks=[excel_uri])



if __name__ == "__main__":


    flow.run(
        parameters=

        {
            "env": "dev",
            "notification_id": 19303,
            "request_id": 212792,
            "request_json": {
                "engagement_id": 113
            },
            "requested_by": "hafulton@cisco.com"
        }

        #         {
#               "env": "prod",
#               "notification_id": 53952,
#               "request_id": 260170,
#               "request_json": {
#                 "engagement_id": 727
#               },
#               "requested_by": "mdarahma@cisco.com"
#         }


    )





