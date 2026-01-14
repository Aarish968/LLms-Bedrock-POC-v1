from sqlalchemy import (
    Connection,
    Date,
    DateTime,
    Float,
    Integer,
    String,
    TextualSelect,
    text,
)

from common_serial_resolution.models import (
    AuditResolvedCurrentSerialRow,
    AuditResolvedSerialRow,
    TableName,
)


def _make_get_all_resolved_serials_stmt(
    ranked_table: str, tags_table: str
) -> "TextualSelect":
    """Items that are tagged with RESOLVED either now or in the past"""
    stmt = (
        text(
            """
        SELECT DISTINCT R.INSTANCE_ID, R.REQUESTED_SERIAL, T.UPDATE_BY, T.UPDATE_DTM FROM IDENTIFIER(:ranked_table) R
        JOIN IDENTIFIER(:tags_table) T ON (T.INSTANCE_ID = R.INSTANCE_ID)
        AND T.TAG_ID = 1411 AND T.IS_DELETED = 'F'
        """
        )
        .bindparams(ranked_table=ranked_table, tags_table=tags_table)
        .columns(
            instance_id=Integer,
            requested_serial=String,
            update_by=String,
            update_dtm=DateTime,
        )
    )
    return stmt


def _make_get_last_resolved_serials_stmt(ranked_table: str) -> "TextualSelect":
    """Instances that were tagged in this process"""
    stmt = (
        text(
            # language=Snowflake
            """SELECT
    BILL_TO_SITE_USE_ID::NUMBER AS BILL_TO_SITE_USE_ID,
    COVERED_STATUS,
    DUPLICATE_IB_FLAG,
    DUP_CNT,
    INSTALLED_AT_GU_ID,
    INSTANCE_ID::NUMBER AS INSTANCE_ID,
    INSTANCE_STATUS_DESC,
    IS_GOOD_STATUS,
    IS_GUID,
    IS_MANAGED_CONTRACT,
    MAPPED_TO_SERVICE_FLAG,
    MX_MAINTENANCE_SO_NUMBER,
    PARENT_INSTANCE_ID::NUMBER AS PARENT_INSTANCE_ID,
    PRODUCT_LAST_DATE_OF_SUPPORT_LDOS,
    PRODUCT_SO,
    REQUESTED_SERIAL,
    RESOLVED_INSTANCE,
    RESOLVED_INSTANCE_ID::NUMBER AS RESOLVED_INSTANCE_ID,
    SCORE,
    SCORE_RANK,
    SERIAL_NUMBER
    FROM
        IDENTIFIER ( :ranked_table )
    WHERE SCORE_RANK = 1

        """
        )
        .bindparams(ranked_table=ranked_table)
        .columns(
            bill_to_site_use_id=Integer,
            covered_status=String,
            duplicate_ib_flag=String,
            dup_cnt=Integer,
            installed_at_gu_id=Integer,
            instance_id=Integer,
            instance_status_desc=String,
            is_good_status=String,
            is_guid=String,
            is_managed_contract=String,
            mx_maintenance_so_number=String,
            parent_instance_id=Integer,
            product_last_date_of_support_ldos=Date,
            product_so=String,
            requested_serial=String,
            resolved_instance=Integer,
            resolved_instance_id=Integer,
            score=Float,
            score_rank=Integer,
            serial_number=String,
        )
    )

    return stmt


def _make_get_duplicated_resolved_serials_stmt(ranked_table: str) -> "TextualSelect":
    """
    Duplicates that were found and resolved. This is important because we had to
    make a decision on which instance to resolve to.
    """
    stmt = (
        text(
            # language=Snowflake
            """SELECT
    BILL_TO_SITE_USE_ID::NUMBER AS BILL_TO_SITE_USE_ID,
    COVERED_STATUS,
    DUPLICATE_IB_FLAG,
    DUP_CNT,
    INSTALLED_AT_GU_ID,
    INSTANCE_ID::NUMBER AS INSTANCE_ID,
    INSTANCE_STATUS_DESC,
    IS_GOOD_STATUS,
    IS_GUID,
    IS_MANAGED_CONTRACT,
    MAPPED_TO_SERVICE_FLAG,
    MX_MAINTENANCE_SO_NUMBER,
    PARENT_INSTANCE_ID::NUMBER AS PARENT_INSTANCE_ID,
    PRODUCT_LAST_DATE_OF_SUPPORT_LDOS,
    PRODUCT_SO,
    REQUESTED_SERIAL,
    RESOLVED_INSTANCE,
    RESOLVED_INSTANCE_ID::NUMBER AS RESOLVED_INSTANCE_ID,
    SCORE,
    SCORE_RANK,
    SERIAL_NUMBER
    FROM
        IDENTIFIER ( :ranked_table )
    WHERE DUP_CNT > 1

        """
        )
        .bindparams(ranked_table=ranked_table)
        .columns(
            bill_to_site_use_id=Integer,
            covered_status=String,
            duplicate_ib_flag=String,
            dup_cnt=Integer,
            installed_at_gu_id=Integer,
            instance_id=Integer,
            instance_status_desc=String,
            is_good_status=String,
            is_guid=String,
            is_managed_contract=String,
            mx_maintenance_so_number=String,
            parent_instance_id=Integer,
            product_last_date_of_support_ldos=Date,
            product_so=String,
            requested_serial=String,
            resolved_instance=Integer,
            resolved_instance_id=Integer,
            score=Float,
            score_rank=Integer,
            serial_number=String,
        )
    )

    return stmt


def _make_not_found_serials_stmt(resolved_table: str) -> "TextualSelect":
    """If Serial Number is NULL, it did not join to the source table"""
    stmt = (
        text(
            """
    SELECT RES.REQUESTED_SERIAL AS SERIAL_NUMBER
    FROM IDENTIFIER(:resolved_table) RES
    WHERE RES.SERIAL_NUMBER IS NULL
    """
        )
        .bindparams(resolved_table=resolved_table)
        .columns(serial_number=String)
    )
    return stmt


def get_all_resolved_serials(
    ranked_table: TableName, tags_table: TableName, conn: "Connection"
) -> list[AuditResolvedSerialRow]:
    """Serial numbers that have ever been resolved"""
    stmt = _make_get_all_resolved_serials_stmt(
        ranked_table=str(ranked_table), tags_table=str(tags_table)
    )

    results = conn.execute(stmt).mappings().all()

    return [AuditResolvedSerialRow.model_validate(row) for row in results]


def get_last_resolved_serials(
    ranked_table: TableName, conn: "Connection"
) -> list[AuditResolvedCurrentSerialRow]:
    stmt = _make_get_last_resolved_serials_stmt(ranked_table=str(ranked_table))

    results = conn.execute(stmt).mappings().all()

    return [AuditResolvedCurrentSerialRow.model_validate(row) for row in results]


def get_duplicated_resolved_serials(
    ranked_table: TableName, conn: "Connection"
) -> list[AuditResolvedCurrentSerialRow]:
    stmt = _make_get_duplicated_resolved_serials_stmt(ranked_table=str(ranked_table))

    results = conn.execute(stmt).mappings().all()

    return [AuditResolvedCurrentSerialRow.model_validate(row) for row in results]


def get_not_found_serials(resolved_table: TableName, conn: "Connection") -> set[str]:
    stmt = _make_not_found_serials_stmt(resolved_table=str(resolved_table))

    results = conn.execute(stmt).mappings().all()

    sn = {row["serial_number"] for row in results}
    sn.discard(None)
    return sn
