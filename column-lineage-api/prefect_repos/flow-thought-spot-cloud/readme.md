
#NAMING CONVENTION
naming convention will tell us if answer, ws, or pinbord ie

first word before the _ should be answer, pinboard, ws, or table:

    answer_Total INSTANCE_ID by SERVICE_LEVEL.tml

#FOLDER STRUCTRES IN S3:
thought.spot.tml/dev | stage | prod  
thought.spot.tml/dev/eng_123/data.tml  
thought.spot.tml/dev/eng_123/backup/2022_4_3/data.tml  
thought.spot.tml/dev/common/defualt.tml

data.canvas.thought.spot.generic.upload.cisco.com/ prod | stage | dev
data.canvas.thought.spot.generic.upload.cisco.com/prod/requests/request_id.json
data.canvas.thought.spot.generic.upload.cisco.com/prod/requested_files/req.xlsx
data.canvas.thought.spot.generic.upload.cisco.com/prod/output_files/request_id/any files

data.canvas.thought.spot.messaging.cisco.com/dev/requests  
data.canvas.thought.spot.messaging.cisco.com/prod/requests   
data.canvas.thought.spot.messaging.cisco.com/stage/requests



needs a populated canvas_thought_spot snowflake table  
needs a line in CPS_BIA_BR.DATA_CANVAS_HDR  


table dml for table to log guids per run   

       create or replace table CPS_DSCI_API.TS_CREATION_LOGGING
    (
        CREATED_BY VARCHAR,
        CREATION_DATA VARIANT,
        STATUS VARCHAR,
        DATE_UPDATED DATE
    );
