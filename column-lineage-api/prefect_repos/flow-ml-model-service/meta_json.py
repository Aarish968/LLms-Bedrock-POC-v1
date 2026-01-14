ml_service_json = {
    "Configuration": {
        "Global" : {
            "id" : {},
            "OPENBLAS_NUM_THREADS" : 1
        },
        "DataSourcing": {
            "snowflake_db" : "CPS_DB",
            "schema" : "CPS_DSCI_ARCHIVE",
            "warehouse" : "cps_dsci_etl_wh",
            "engine_arg1" : "prd_cps_dsci_etl_svc",
            "engine_arg2" : "cps_dsci_etl_wh"
        },
        "DataPreProcess": {
            "sup_data_arg1" : "alcon_covered_training.parquet",
            "std_exclusion" : ("AIR100", "AIR1000", "AIR100U", "AIR1040", "AIR110A", "AIR110U", "AIR11A", "AIR11U", "AIR120A", "AIR120R", "AIR120U", "AIR12A", "AIR12U", "AIR130A" "AIR130U", "AIR13A", "AIR140A", "AIR14U", "AIR150U", "AIR1540", "AIR1560", "AIR1570", "AIR15U", "AIR1700", "AIR1800", "AIR1810", "AIR1815", "AIR1830", "AIR1850", "AIR2000", "AIR2700", "AIR2800", "AIR3000", "AIR340", "AIR350", "AIR3500", "AIR35SE", "AIR35SI", "AIR3700", "AIR3800", "AIR4800", "AIR500A", "AIR500U", "AIRANT", "AIRAP", "AIRBNDL", "AIRCA", "AIRCELL", "AIRCI", "AIRCMN", "AIRIA", "AIRINFA", "AIRINFE", "AIRINFU", "AIRIW", "AIRMGMA", "AIRMGMU", "AIRMHW", "AIRMHW2", "AIRMHW3", "AIRMHW4", "AIRMOD", "AIRMSTH", "AIRNCS", "AIROLD", "AIRPWR", "AIRSNSR", "AIRWAN", "C9130AX", "C9115AX", "C9117AX", "C9120AX", "IPPHONE", "PHON3PC", "PHONCOL", "PHONE", "PHONVID", "PHONVOC", "SBPHONE", "WPHONE", "CNSWTCH", "CNWRL", "CNCOMM", "CNSEC", "CNEMM", "CNVSN", "GRHW", "CNAPM", "GSHW", "GRLIC"),
            "cols_to_exclude" : ("ship_date_days_prior","erp_list_price","rnd"),
            "testing_df_drop" : ("ship_date","warranty_end_date","last_date_of_renewal","last_date_of_service_attach"),
            "testing_df_parquet" : "eng_578_training_autosklearn.parquet"
        },
        "Training" : {
            "target_col" : "cam_managed",
            "categorical_columns" : ("business_unit","country_code_iso","customer_name_mod","gen_location","product_family"),
            "numerical_columns" : ("erp_list_price","ship_date_days_prior","instance_id")
        }}}