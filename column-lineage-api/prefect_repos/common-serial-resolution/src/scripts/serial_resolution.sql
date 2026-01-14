CREATE OR REPLACE PROCEDURE SERIAL_RESOLUTION( INVALUE VARCHAR(16777216), LOGGED_USER VARCHAR(16777216) )
RETURNS VARIANT
LANGUAGE SQL
STRICT
EXECUTE AS OWNER
AS
$$
-- all this does is create the core table for further processing and resolve multi instance serials based on a ranking
-- how to report the facts as one request when the post is iterative?
DECLARE
    dc_engagement INT;
    request INT;
    cco VARCHAR;
    LOGS ARRAY := ARRAY_CONSTRUCT();
    ddl_action VARCHAR;
    sp_invalue VARCHAR;
    comments VARCHAR;
    requested_temp_tbl VARCHAR;
    resolved_temp_tbl VARCHAR;
    resolved_ranked_temp_tbl VARCHAR;
    tags_tbl VARCHAR;

BEGIN
    dc_engagement      := (SELECT parse_json(:invalue):engagement_id::INT);
    cco                := (SELECT parse_json(:invalue):cisco_cco_id::VARCHAR);
    request            := (SELECT parse_json(:invalue):request_id::INT);
    comments           := (SELECT parse_json(:invalue):comment::VARCHAR);
    ddl_action         := (SELECT parse_json(:invalue):ddl_action::VARCHAR);
    requested_temp_tbl := CONCAT('SN_SERIAL_', parse_json(:invalue):request_id, '_TMP');
    resolved_temp_tbl  := CONCAT('SN_SERIAL_RESOLVED_', parse_json(:invalue):request_id, '_TMP');
    resolved_ranked_temp_tbl := CONCAT('SN_SERIAL_RANKED_', parse_json(:invalue):request_id, '_TMP');
    tags_tbl           := CONCAT('DC_ENGAGEMENT_TAGS_', parse_json(:invalue):engagement_id::INT);
    
    CREATE OR REPLACE TRANSIENT TABLE IDENTIFIER(:resolved_temp_tbl) AS
    WITH ser AS
    (
        SELECT DISTINCT SERIAL_NUMBER FROM IDENTIFIER(:requested_temp_tbl) WHERE SERIAL_NUMBER IS NOT NULL
    )
    SELECT DISTINCT 
        ser.serial_number AS requested_serial,
        NVL(ib.serial_number, ib.dup_serial_number) AS serial_number,
        ib.instance_id,
        ib.parent_instance_id,
        CASE WHEN isite.gu_id IN (
            SELECT DISTINCT global_ultimate_id
            FROM DC_PARTY_LINKS l
            JOIN EDW_MASTER_ETL_DB.SS.CRT_XXNGCR_DNM_RELATIONSHIPS dnm ON (cr_party_id = dnm.party_id)
            WHERE l.DC_ENGAGEMENT_ID = :dc_engagement AND l.is_deleted = 'F'
        ) THEN 'Y' ELSE 'N' END AS is_guid,
        CASE WHEN ib.INSTANCE_STATUS_DESC = 'Latest-INSTALLED' THEN 'Y' ELSE 'N' END AS is_good_status,
        ib.bill_to_site_use_id,
        ib.covered_status,
        ib.instance_status_desc,
        duplicate_ib_flag,
        so_number AS product_so,
        isite.gu_id AS installed_at_gu_id,
        CPS_DSCI_ARCHIVE.FIX_DATES(item.last_date_of_support::DATE) AS product_last_date_of_support_ldos,
        IFF(item.mapped_to_service_flag = 'YES WITH SPM', 'T', 'F') AS mapped_to_service_flag,
        NVL(MAINTENANCE_SO_NUMBER, 0) AS mx_MAINTENANCE_SO_NUMBER,
        CASE WHEN NVL(hdr_core.contract_number, 0) IN
        (
            SELECT DISTINCT contract_number
            FROM DC_ENGAGEMENT_TO_BOOKINGS_RESPONSIBLE_USER eru 
            JOIN DC_MANAGED_SERVICE_CONTRACTS sc ON
            (eru.booking_contract = sc.booking_contract AND sc.dc_engagement_id = eru.dc_engagement_id)
            WHERE sc.dc_engagement_id = :dc_engagement AND eru.is_deleted = 'F' AND sc.is_deleted = 'F'
        )
        THEN 'Y' ELSE 'N' END AS is_managed_contract,
        t.instance_id AS resolved_instance
    FROM ser
    LEFT JOIN EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_INSTANCE_DETAIL ib 
        ON (ser.serial_number = NVL(ib.serial_number, ib.dup_serial_number) AND ib.EDWSF_SOURCE_DELETED_FLAG = 'N')
    LEFT JOIN IDENTIFIER(:tags_tbl) t 
        ON (t.instance_id = ib.instance_id AND t.tag_id = 1411 AND t.is_deleted = 'F')
    LEFT JOIN EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_SITE_GU_DENORM isite ON
    (
        ib.install_at_site_use_id = isite.site_use_id
        AND NVL(isite.EDWSF_SOURCE_DELETED_FLAG, 'N') = 'N'
        AND isite.site_use_code = 'SHIP_TO'
    )
    LEFT JOIN EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_SAIB_ITEMS item ON
    (
        item.INVENTORY_ITEM_ID = ib.INVENTORY_ITEM_ID
        AND NVL(item.EDWSF_SOURCE_DELETED_FLAG, 'N') = 'N'
    )
    LEFT JOIN EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_CVDPRDLINE_DETAIL cvd_line ON
    (
        ib.INSTANCE_ID = cvd_line.INSTANCE_ID
        AND NVL(cvd_line.EDWSF_SOURCE_DELETED_FLAG, 'N') = 'N'
    )
    LEFT JOIN EDW_SERVICE_ETL_DB.SS.CSF_XXCCS_DS_SAHDR_CORE hdr_core ON
    (
        cvd_line.contract_id = hdr_core.contract_id AND cvd_line.service_line_id = hdr_core.service_line_id
        AND NVL(hdr_core.EDWSF_SOURCE_DELETED_FLAG, 'N') = 'N'
    );

    -- ranked table to use all over
    CREATE OR REPLACE TABLE IDENTIFIER(:resolved_ranked_temp_tbl) AS
    WITH
        pre_decisioned AS
        (
            SELECT DISTINCT r.instance_id, r.requested_serial FROM IDENTIFIER(:resolved_temp_tbl) r
            JOIN IDENTIFIER(:tags_tbl) t ON (t.instance_id = r.instance_id)
            AND t.tag_id = 1411 AND t.is_deleted = 'F'
        ),
        dups AS (
            SELECT requested_serial AS duplicated_serial, COUNT(0) AS dup_cnt
            FROM IDENTIFIER(:resolved_temp_tbl)
            GROUP BY requested_serial
            HAVING COUNT(0) > 1
        ),
        resolved_dups AS ( -- previously decisioned serials we need the instance on these
            SELECT duplicated_serial, instance_id
            FROM pre_decisioned p JOIN dups ON (p.requested_serial = dups.duplicated_serial)
        ), -- select * from resolved_dups  -- these are the correct answers for a given serial
        rank AS
        (
            SELECT 
                instance_id, serial_number,
                CASE WHEN resolved_instance = instance_id THEN 1.25 ELSE 0 END +
                IFF(NVL(is_guid, 'N') = 'Y', .55, 0) +
                IFF(NVL(is_managed_contract, 'N') = 'Y', .25, 0) +
                CASE WHEN mx_maintenance_so_number > 0 THEN .1 ELSE 0 END +
                IFF(NVL(is_good_status, 'N') = 'Y', .1, -.1) AS score,
                ROW_NUMBER() OVER (PARTITION BY requested_serial ORDER BY score DESC) AS score_rank
            FROM IDENTIFIER(:resolved_temp_tbl)
            WHERE serial_number IS NOT NULL
        ),
        final AS (  -- for requested tagging
            SELECT DISTINCT beginning.*, rank.score, rank.score_rank, resolved_dups.instance_id AS resolved_instance_id, NVL(dups.dup_cnt, 1) AS dup_cnt
            FROM IDENTIFIER(:resolved_temp_tbl) beginning
            JOIN rank ON (beginning.instance_id = rank.instance_id)
            LEFT JOIN pre_decisioned ON (pre_decisioned.instance_id = rank.instance_id)
            LEFT JOIN dups ON (dups.duplicated_serial = rank.serial_number)
            LEFT JOIN resolved_dups ON (rank.serial_number = resolved_dups.duplicated_serial)
            ORDER BY requested_serial, score_rank
        )
    SELECT * FROM final;

    -- now we need to tag the best picks as resolved in ranked above
    LET c2 CURSOR FOR (
        WITH prep AS (
            SELECT 'set-null' AS ddl_action, 284 AS TAGSET_ID, 1411 AS TAG_ID, ? AS userId, ? AS dc_engagement_id,
                ARRAY_AGG(instance_id::BIGINT) AS INSTANCE_IDs
            FROM IDENTIFIER(?) WHERE score_rank = 1 AND dup_cnt > 1
            GROUP BY 1, 2, 3, 4, 5
        )
        SELECT TO_JSON(OBJECT_CONSTRUCT_KEEP_NULL(
            'ddl_action', prep.ddl_action,
            'tagsetId', prep.TAGSET_ID,
            'tagId', prep.TAG_ID,
            'userId', prep.userId,
            'engagementId', prep.dc_engagement_id,
            'instance', prep.INSTANCE_IDs,
            'comment', ?
        )) AS sp_invalue FROM prep
    );

    OPEN c2 USING(:cco, :dc_engagement, :resolved_ranked_temp_tbl, :comments);

    FOR final_record IN c2 DO
        sp_invalue := final_record.sp_invalue;
        CALL TAG_INSTANCES_11(:sp_invalue);
    END FOR;

    CLOSE c2;

    -- really should return true or error and insert these into audit table
    RETURN OBJECT_CONSTRUCT(
        'sp_invalue', :sp_invalue, 
        'dc_engagement', :dc_engagement,
        'request', :request, 
        'cco', :cco,
        'ddl_action', :ddl_action, 
        'comments', :comments,
        'requested_temp_tbl', :requested_temp_tbl, 
        'resolved_temp_tbl', :resolved_temp_tbl, 
        'tags_table', :tags_tbl
    );
END;
$$;
