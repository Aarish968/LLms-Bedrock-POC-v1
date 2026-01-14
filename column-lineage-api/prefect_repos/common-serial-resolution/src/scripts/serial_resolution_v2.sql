CREATE OR REPLACE PROCEDURE SERIAL_RESOLUTION_V2( INVALUE VARCHAR(16777216), LOGGED_USER VARCHAR(16777216) )
    RETURNS VARIANT
    LANGUAGE SQL
    STRICT
    EXECUTE AS OWNER
AS
$$
    -- Expected payload:
    -- dc_engagement_id : INT
    -- request_id : INT
    -- cisco_cco_id : VARCHAR
    -- comment : VARCHAR
    -- ddl_action : VARCHAR
    -- snowflake_uri : VARCHAR (reference to staged file containing serial_numbers in JSON format)
        -- Should consist of an array of strings
    -- Returns
        -- resolved_temp_tbl : VARCHAR , A transient table, caller should drop it after reading
        -- resolved_ranked_temp_tbl : VARCHAR, A transient table, caller should drop it after reading



DECLARE
    PARSED_JSON              VARIANT;
    DC_ENGAGEMENT_ID         INT;
    REQUEST_ID               INT;
    CISCO_CCO_ID             VARCHAR;
    SNOWFLAKE_URI            VARCHAR;
    SNOWFLAKE_EXPORT_URI     VARCHAR;
    LOGS                     ARRAY := ARRAY_CONSTRUCT( );
    DDL_ACTION               VARCHAR;
    SP_INVALUE               VARCHAR;
    COMMENTS                 VARCHAR;
    RESOLVED_TEMP_TBL        VARCHAR;
    RESOLVED_RANKED_TEMP_TBL VARCHAR;
    ENG_TAGS_TABLE           VARCHAR;
    EXPORT_TEMP_TBL          VARCHAR;
    DYNAMIC_SQL              VARCHAR;
    DYNAMIC_EXPORT_SQL       VARCHAR;
    N_SERIAL_NUMBERS         INT;


BEGIN
    PARSED_JSON := TRY_PARSE_JSON( INVALUE );
    IF (PARSED_JSON IS NULL)
        THEN
            RETURN OBJECT_CONSTRUCT( 'message', 'Invalid JSON input', 'success', FALSE, 'code', 400 );
    END IF;

    DC_ENGAGEMENT_ID := PARSED_JSON:dc_engagement_id::INT;
    CISCO_CCO_ID := PARSED_JSON:cisco_cco_id::VARCHAR;
    REQUEST_ID := PARSED_JSON:request_id::INT;
    COMMENTS := PARSED_JSON:comment::VARCHAR;
    DDL_ACTION := PARSED_JSON:ddl_action::VARCHAR;
    SNOWFLAKE_URI := PARSED_JSON:snowflake_uri::VARCHAR;
    SNOWFLAKE_EXPORT_URI := '@CPS_DSCI_STG.MY_CSV_STAGE/json/sp/tag_instances_v2/' || UUID_STRING() || :REQUEST_ID::VARCHAR || '.json.gz';

    -- Basic validation on input
    IF (DC_ENGAGEMENT_ID IS NULL OR CISCO_CCO_ID IS NULL OR REQUEST_ID IS NULL OR DDL_ACTION IS NULL OR SNOWFLAKE_URI IS NULL)
        THEN
            RETURN OBJECT_CONSTRUCT( 'message', 'Missing required fields', 'success', FALSE, 'code', 400 );
    END IF;

    COMMENTS := NVL( COMMENTS, 'No comment provided' );


    ENG_TAGS_TABLE := CONCAT( 'DC_ENGAGEMENT_TAGS_', :DC_ENGAGEMENT_ID );
    RESOLVED_TEMP_TBL := CONCAT( 'SN_SERIAL_RESOLVED_', :REQUEST_ID, '_TMP' );
    RESOLVED_RANKED_TEMP_TBL := CONCAT( 'SN_SERIAL_RANKED_', :REQUEST_ID, '_TMP' );
    EXPORT_TEMP_TBL := CONCAT( 'SN_SERIAL_EXPORT_', :REQUEST_ID, '_TMP' );

    LOGS := ARRAY_APPEND( LOGS, CONCAT( 'Using Resolved Temp Table: ', :RESOLVED_TEMP_TBL, ' and Ranked Temp Table: ',
                                        :RESOLVED_RANKED_TEMP_TBL ) );


    CREATE OR REPLACE TEMPORARY TABLE CPS_DSCI_STG.SERIAL_NUMBER_LOAD
    (
        SERIAL_NUMBER VARCHAR
    );


    -- We have to do this dynamically because we need the file format
    DYNAMIC_SQL := 'INSERT INTO CPS_DSCI_STG.SERIAL_NUMBER_LOAD
                    SELECT DISTINCT
                        $1:serial_number::VARCHAR AS SERIAL_NUMBER
                    FROM ' || :SNOWFLAKE_URI || '
                    (FILE_FORMAT => JSON_FILE_FORMAT)';

    EXECUTE IMMEDIATE :DYNAMIC_SQL;

    SELECT COUNT( SERIAL_NUMBER ) INTO :N_SERIAL_NUMBERS FROM CPS_DSCI_STG.SERIAL_NUMBER_LOAD WHERE SERIAL_NUMBER IS NOT NULL;

    LOGS := ARRAY_APPEND( LOGS, CONCAT( 'Number of Distinct Serial Numbers Loaded: ', :N_SERIAL_NUMBERS ) );

    IF (:N_SERIAL_NUMBERS = 0)
        THEN
            RETURN OBJECT_CONSTRUCT( 'message', 'No serial numbers found in the input file - exiting', 'success', TRUE,
                                     'code', 200 );
    END IF;


    CREATE OR REPLACE TRANSIENT TABLE IDENTIFIER (:RESOLVED_TEMP_TBL) AS
    WITH FILTER AS (
    WITH SER AS
        (
            SELECT DISTINCT SERIAL_NUMBER FROM CPS_DSCI_STG.SERIAL_NUMBER_LOAD WHERE SERIAL_NUMBER IS NOT NULL
        )
    SELECT DISTINCT
        SER.SERIAL_NUMBER AS REQUESTED_SERIAL,
        NVL( IB.SERIAL_NUMBER, IB.DUP_SERIAL_NUMBER ) AS SERIAL_NUMBER,
        IB.INSTANCE_ID,
        IB.PARENT_INSTANCE_ID,
        CASE WHEN ISITE.GU_ID IN (
            SELECT DISTINCT DNM.GLOBAL_ULTIMATE_ID
            FROM DC_PARTY_LINKS L
            JOIN EDW_MASTER_ETL_DB.SS.CRT_XXNGCR_DNM_RELATIONSHIPS DNM ON (CR_PARTY_ID = DNM.PARTY_ID)
            WHERE L.DC_ENGAGEMENT_ID = :DC_ENGAGEMENT_ID AND L.IS_DELETED = 'F'
        ) THEN 'Y' ELSE 'N' END AS IS_GUID,
        CASE WHEN IB.INSTANCE_STATUS_DESC = 'Latest-INSTALLED' THEN 'Y' ELSE 'N' END AS IS_GOOD_STATUS,
        IB.BILL_TO_SITE_USE_ID,
        IB.COVERED_STATUS,
        IB.INSTANCE_STATUS_DESC,
        DUPLICATE_IB_FLAG,
        SO_NUMBER AS PRODUCT_SO,
        ISITE.GU_ID AS INSTALLED_AT_GU_ID,
        CPS_DSCI_ARCHIVE.FIX_DATES( ITEM.LAST_DATE_OF_SUPPORT::DATE ) AS PRODUCT_LAST_DATE_OF_SUPPORT_LDOS,
        IFF( ITEM.MAPPED_TO_SERVICE_FLAG = 'YES WITH SPM', 'T', 'F' ) AS MAPPED_TO_SERVICE_FLAG,
        NVL( CVD_LINE.MAINTENANCE_SO_NUMBER, '0' )::VARCHAR AS MX_MAINTENANCE_SO_NUMBER,
        CASE WHEN NVL( HDR_CORE.CONTRACT_NUMBER, 0 ) IN
        (
            SELECT DISTINCT CONTRACT_NUMBER
            FROM DC_ENGAGEMENT_TO_BOOKINGS_RESPONSIBLE_USER ERU
            JOIN DC_MANAGED_SERVICE_CONTRACTS SC ON
            (ERU.BOOKING_CONTRACT = SC.BOOKING_CONTRACT AND SC.DC_ENGAGEMENT_ID = ERU.DC_ENGAGEMENT_ID)
            WHERE SC.DC_ENGAGEMENT_ID = :DC_ENGAGEMENT_ID AND ERU.IS_DELETED = 'F' AND SC.IS_DELETED = 'F'
        ) THEN 'Y' ELSE 'N' END AS IS_MANAGED_CONTRACT,
            T.INSTANCE_ID AS RESOLVED_INSTANCE,
         RANK() OVER ( PARTITION BY RESOLVED_INSTANCE ORDER BY MX_MAINTENANCE_SO_NUMBER  DESC) AS u_SCORE_RANK
        FROM SER
        LEFT JOIN EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_INSTANCE_DETAIL IB
            ON (SER.SERIAL_NUMBER = NVL(IB.SERIAL_NUMBER, IB.DUP_SERIAL_NUMBER) AND IB.EDWSF_SOURCE_DELETED_FLAG = 'N')
        LEFT JOIN IDENTIFIER (:ENG_TAGS_TABLE) T
            ON (T.INSTANCE_ID = IB.INSTANCE_ID AND T.TAG_ID = 1411 AND T.IS_DELETED = 'F')
        LEFT JOIN EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_SITE_GU_DENORM ISITE ON
        (
            IB.INSTALL_AT_SITE_USE_ID = ISITE.SITE_USE_ID
            AND NVL( ISITE.EDWSF_SOURCE_DELETED_FLAG, 'N' ) = 'N'
            AND ISITE.SITE_USE_CODE = 'SHIP_TO'
        )
        LEFT JOIN EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_SAIB_ITEMS ITEM ON
        (
            ITEM.INVENTORY_ITEM_ID = IB.INVENTORY_ITEM_ID
            AND NVL( ITEM.EDWSF_SOURCE_DELETED_FLAG, 'N' ) = 'N'
        )
        LEFT JOIN EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_CVDPRDLINE_DETAIL CVD_LINE ON
        (
            IB.INSTANCE_ID = CVD_LINE.INSTANCE_ID
            AND NVL( CVD_LINE.EDWSF_SOURCE_DELETED_FLAG, 'N' ) = 'N'
        )
        LEFT JOIN EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_SAHDR_CORE HDR_CORE ON
        (
            CVD_LINE.CONTRACT_ID = HDR_CORE.CONTRACT_ID AND CVD_LINE.SERVICE_LINE_ID = HDR_CORE.SERVICE_LINE_ID
            AND NVL( HDR_CORE.EDWSF_SOURCE_DELETED_FLAG, 'N' ) = 'N'
            )
        )
    SELECT * FROM FILTER WHERE U_SCORE_RANK = 1;


    -- ranked table to use all over
    CREATE OR REPLACE TRANSIENT TABLE IDENTIFIER (:RESOLVED_RANKED_TEMP_TBL) AS
    WITH
        PRE_DECISIONED AS
        (
            SELECT DISTINCT R.INSTANCE_ID, R.REQUESTED_SERIAL FROM IDENTIFIER (:RESOLVED_TEMP_TBL) R
            JOIN IDENTIFIER (:ENG_TAGS_TABLE) T ON (T.INSTANCE_ID = R.INSTANCE_ID)
            AND T.TAG_ID = 1411 AND T.IS_DELETED = 'F'
        ),
        DUPS AS
        (
            SELECT REQUESTED_SERIAL AS DUPLICATED_SERIAL, COUNT( 0 ) AS DUP_CNT
            FROM IDENTIFIER ( :RESOLVED_TEMP_TBL )
            GROUP BY REQUESTED_SERIAL
            HAVING COUNT( 0 ) > 1
        ),
        -- previously decisioned serials, we will retrieve the resolved instance on these
        -- select * from RESOLVED_DUPS  -- these are the correct answers for a given serial
        RESOLVED_DUPS AS (
            SELECT DUPLICATED_SERIAL, INSTANCE_ID
            FROM PRE_DECISIONED P JOIN DUPS ON (P.REQUESTED_SERIAL = DUPS.DUPLICATED_SERIAL)
        ),
        RANK AS
            -- Ranking algorithm
        (
        SELECT   INSTANCE_ID,
                 SERIAL_NUMBER,
                 CASE WHEN RESOLVED_INSTANCE = INSTANCE_ID THEN 1.25 ELSE 0 END +
                 IFF( NVL( IS_GUID, 'N' ) = 'Y', .55, 0 ) +
                 IFF( NVL( IS_MANAGED_CONTRACT, 'N' ) = 'Y', .25, 0 ) +
                 CASE WHEN TRY_TO_NUMBER(MX_MAINTENANCE_SO_NUMBER, 0) > 0 THEN .1 ELSE 0 END +
                 IFF( NVL( IS_GOOD_STATUS, 'N' ) = 'Y', .1, -.1 ) AS SCORE,
                 ROW_NUMBER( ) OVER ( PARTITION BY REQUESTED_SERIAL ORDER BY SCORE DESC,INSTANCE_ID ) AS SCORE_RANK
                 FROM IDENTIFIER ( :RESOLVED_TEMP_TBL )
                 WHERE SERIAL_NUMBER IS NOT NULL
        ),
        FINAL AS ( -- for requested tagging
            SELECT DISTINCT BEGINNING.*, RANK.SCORE, RANK.SCORE_RANK, RESOLVED_DUPS.INSTANCE_ID AS RESOLVED_INSTANCE_ID, NVL( DUPS.DUP_CNT, 1 ) AS DUP_CNT
                FROM IDENTIFIER (:RESOLVED_TEMP_TBL) BEGINNING
                JOIN RANK ON (BEGINNING.INSTANCE_ID = RANK.INSTANCE_ID)
                LEFT JOIN PRE_DECISIONED ON (PRE_DECISIONED.INSTANCE_ID = RANK.INSTANCE_ID)
                LEFT JOIN DUPS ON (DUPS.DUPLICATED_SERIAL = RANK.SERIAL_NUMBER)
                LEFT JOIN RESOLVED_DUPS ON (RANK.SERIAL_NUMBER = RESOLVED_DUPS.DUPLICATED_SERIAL and RESOLVED_DUPS.INSTANCE_ID =  RANK.INSTANCE_ID )
                ORDER BY REQUESTED_SERIAL, SCORE_RANK
            )
    SELECT *
        FROM FINAL WHERE SCORE_RANK = 1;

    -- now we need to tag the best picks as resolved in ranked above
    -- To do this, we need to call tag_instances_v2 procedure which expects
    -- A snowflake uri, similar to the one used in the original procedure

    LOGS := ARRAY_APPEND( LOGS, CONCAT( 'Resolved Temp Table Created: ', :RESOLVED_TEMP_TBL ) );
    LOGS := ARRAY_APPEND( LOGS, CONCAT( 'Resolved Ranked Temp Table Created: ', :RESOLVED_RANKED_TEMP_TBL ) );
    LOGS := ARRAY_APPEND( LOGS, CONCAT( 'URL for Export: ', :SNOWFLAKE_EXPORT_URI ) );

    CREATE OR REPLACE TEMPORARY TABLE IDENTIFIER(:EXPORT_TEMP_TBL) (
        INSTANCE_ID BIGINT
        );

    DYNAMIC_EXPORT_SQL := 'COPY INTO ' || :SNOWFLAKE_EXPORT_URI || '
     FROM (
        SELECT OBJECT_CONSTRUCT(
            ''instance_id'', INSTANCE_ID
            )
        FROM IDENTIFIER(?)
     )
    FILE_FORMAT = (TYPE = ''JSON'' COMPRESSION = ''GZIP'')';

    EXECUTE IMMEDIATE :DYNAMIC_EXPORT_SQL USING (RESOLVED_RANKED_TEMP_TBL);

    CALL TAG_INSTANCES_V2(TO_VARCHAR(
            OBJECT_CONSTRUCT('snowflake_uri', :SNOWFLAKE_EXPORT_URI,
                            'ddl_action', 'set-null',
                            'tagsetId', 284,
                            'tagId', 1411,
                            'engagementId', :DC_ENGAGEMENT_ID,
                            'comment', :COMMENTS,
                            'userId', :CISCO_CCO_ID
                          )
         ));

    CALL CREATE_TAGS_TABLE(TO_VARCHAR(OBJECT_CONSTRUCT('dc_engagement_id', :DC_ENGAGEMENT_ID)));

    RETURN OBJECT_CONSTRUCT( 'message', 'Serial Resolution Completed Successfully',
                            'success', TRUE, 'code', 200,
                             'resolved_temp_tbl', :RESOLVED_TEMP_TBL,
                             'resolved_ranked_temp_tbl', :RESOLVED_RANKED_TEMP_TBL,
                             'logs', :LOGS);
END;
$$;
