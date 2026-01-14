

# Modules
## step_zero.py:
Description: Determining what factors should be participating in this expirimetn and what should be collected as a part of the output, also define the test, A/B
### Input:
#### TBD
### Output:
#### TBD


## sourcing.py: 
### Description: 
#### Sources data about what instance_id's are mangaed or not managed, based on set of guids and contracts for an engagement
#### Query is in the managed_query.py
### Input:
```json
{
      "eng_id": 578
}
```
### Output:
#### A json to s3 loc: trigger_processing/578_model_EshHzKQNmP.json
```json
{
  "file_path": "s3://ds-data-store-prod/sourced_data/578/2022_08_08/578_model_EshHzKQNmP.parquet",
"eng_id": 578
}
```



## preprocessing.py:
### Description: 
### Input:
#### Yet to be determind what will trigger this flow, however inputs are in a json as below
```json
{
  "file_path": "s3://ds-data-store-prod/sourced_data/578/2022_08_08/578_model_EshHzKQNmP.parquet",
"eng_id": 578
}
```
### Output:
#### A json to s3 loc: trigger_training/578_model_EshHzKQNmP.json
```json
{
  "file_path": "s3://ds-data-store-prod/prepped_data/578/2022_08_08/578_model_AHrTuOJHWE.parquet",
"eng_id": 578
}
```

## train_model.py:
### Description: 
### Input:
```json
{
  "file_path": "s3://ds-data-store-prod/prepped_data/578/2022_08_08/578_model_AHrTuOJHWE.parquet",
"eng_id": 578
}
```
### Output:
#### A pickle file of a trained model saved to s3 :  trained_models/578_model_AHrTuOJHWE.sav
#### No json is currently output from this flow, just a .sav file. We should probbaly write meta to a table or a json 





## predict.py:
### Description: 
### Input:
#### Takes a json with the below fields
```json
{
"eng_id": 578,
"model_file_key": "trained_models/578_model_AHrTuOJHWE.sav",
"file_loc": "s3://ds-data-store-prod/prepped_data/578/2022_08_08/578_model_AHrTuOJHWE.parquet"
}
```

### Output:
#### The predictions are output to a table CPS_DSCI_ARCHIVE.PREDICT_MANAGED, with a key of EngId_InstanceId
#### This is to keep the scope at the engagement level. Should we change this? 



