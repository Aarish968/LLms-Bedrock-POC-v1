from typing import Optional

import click
import pyperclip
from snowflake.sqlalchemy.snowdialect import SnowflakeDialect
from sqlalchemy import (
    column as col,
    select,
    func,
    bindparam,
    table,
)
from sqlalchemy.engine import Dialect
from sqlalchemy.orm import aliased
from sqlalchemy.sql import TableClause
from sqlalchemy.sql.expression import and_, desc, case, text

from queries.cvd_line import CVD_LINE_CTE
from queries.engagement_data import TABLE_ENG
from queries.engagement_header import TABLE_HDR
from queries.func import NVL, IFF, NULL, FIX_DATES
from queries.hdr_core import HDR_CORE_CTE
from queries.instance_detail import TABLE_IB
from queries.saib_items import ITEM_CTE
from queries.site_denorm import ISITE_CTE
from queries.temp_table import TempTableFromSelect

_TRANSIENT_TABLE_TEMPLATE = "{catalog}.{schema}.IB_{name}_TMP"


def format_sql(sql: str) -> str:
    """Format Compiled SQL to reflect reality"""
    return sql.replace('"', "")


def build_transient_table(
    catalog: Optional[str], schema: Optional[str], name: Optional[str], dialect: Dialect
) -> tuple[str, str, TableClause]:
    """
    Compile a statement to create a transient table

    Parameters
    ----------
    catalog : Optional[str]
        The catalog to use for the transient table. If None, table_name is left as parameter
    schema : Optional[str]
        The schema to use for the transient table. If None, table_name is left as parameter
    name : Optional[str]
        The name of the transient table. If None, table_name is left as parameter
    dialect : Dialect
        The dialect to use for the statement

    Returns
    -------

    """
    if not all((catalog, schema, name)):
        TRANSIENT_TABLE_NAME = "identifier(:transient_table_name)"
    else:
        TRANSIENT_TABLE_NAME = _TRANSIENT_TABLE_TEMPLATE.format(
            catalog=catalog, schema=schema, name=name
        )

    TABLE_TRANSIENT = table(
        # Engagement ID -> Instance ID
        TRANSIENT_TABLE_NAME,
        # TABLE_IB columns
        col("bill_to_site_use_id"),
        col("covered_status"),
        col("dup_serial_number"),
        col("duplicate_ib_flag"),
        col("install_at_site_use_id"),
        col("instance_status_desc"),
        col("inventory_item_id"),
        col("item_name"),
        col("item_type_flag"),
        col("parent_instance_id"),
        col("po_number"),
        col("quantity"),
        col("serial_number"),
        col("ship_date"),
        col("ship_to_site_use_id"),
        col("so_number"),
        # TABLE_HDR
        col("engagement_number"),
        # TABLE_ENG
        col("collector_host_name"),
        col("collector_matched"),
        col("c3_matched"),
        col("customer_matched"),
        col("instance_id"),
        col("instance_number"),
        col("engagement_id"),
        col("contract_id"),
    )

    # Statement to create a transient table
    IB_TRANSIENT_STMT = TempTableFromSelect(
        TABLE_TRANSIENT,
        select(
            TABLE_IB.c.bill_to_site_use_id.label("bill_to_site_use_id"),
            TABLE_IB.c.covered_status.label("covered_status"),
            TABLE_IB.c.dup_serial_number.label("dup_serial_number"),
            TABLE_IB.c.duplicate_ib_flag.label("duplicate_ib_flag"),
            TABLE_IB.c.install_at_site_use_id.label("install_at_site_use_id"),
            TABLE_IB.c.instance_status_desc.label("instance_status_desc"),
            TABLE_IB.c.inventory_item_id.label("inventory_item_id"),
            TABLE_IB.c.item_name.label("item_name"),
            TABLE_IB.c.item_type_flag.label("item_type_flag"),
            TABLE_IB.c.parent_instance_id.label("parent_instance_id"),
            TABLE_IB.c.po_number.label("po_number"),
            TABLE_IB.c.quantity.label("quantity"),
            TABLE_IB.c.serial_number.label("serial_number"),
            TABLE_IB.c.ship_date.label("ship_date"),
            TABLE_IB.c.ship_to_site_use_id.label("ship_to_site_use_id"),
            TABLE_IB.c.so_number.label("so_number"),
            TABLE_IB.c.instance_number.label("instance_number"),
            # TABLE_ENG columns
            TABLE_ENG.c.collector_host_name.label("collector_host_name"),
            NVL(TABLE_ENG.c.collector_matched, text("'N'")).label("collector_matched"),
            NVL(TABLE_ENG.c.c3_matched, text("'N'")).label("c3_matched"),
            NVL(TABLE_ENG.c.customer_matched, text("'N'")).label("customer_matched"),
            TABLE_ENG.c.instance_id.label("instance_id"),
            TABLE_ENG.c.engagement_id.label("engagement_id"),
            TABLE_ENG.c.contract_id.label("contract_id"),
            # TABLE_HDR columns
            TABLE_HDR.c.engagement_number.label("engagement_number"),
        )
        .select_from(TABLE_ENG)
        .where(TABLE_ENG.c.engagement_id == bindparam("engagement_id", required=True))
        .where(TABLE_ENG.c.operation_code.in_(("I", "U", "N")))
        .join(TABLE_IB, TABLE_IB.c.instance_id == TABLE_ENG.c.instance_id)
        .join(TABLE_HDR, TABLE_HDR.c.engagement_id == TABLE_ENG.c.engagement_id),
    )

    stmt = str(
        IB_TRANSIENT_STMT.compile(
            dialect=dialect, compile_kwargs={"literal_binds": True}
        )
    )
    stmt = format_sql(stmt)
    stmt = stmt.replace("%(engagement_id)s", ":engagement_id").replace(
        "operation_code IN (__[POSTCOMPILE_operation_code_1])",
        "operation_code IN ('I', 'U', 'N')",
    )
    return stmt, TRANSIENT_TABLE_NAME, TABLE_TRANSIENT


def build_query(table_transient: TableClause, dialect: Dialect) -> str:
    """
    Generate the query to be executed, utilizing the transient table

    Parameters
    ----------
    table_transient : TableClause
        TableClause to use for the transient table. Defined in build_transient_table
    dialect : Dialect
        The dialect to use for the statement

    Returns
    -------

    """

    IB = aliased(table_transient, name="IB")
    IB_PRNT = IB.alias("IB_PRNT")

    query = (
        select(
            # IB (Transient)
            IB.c.instance_number.label("instance_number"),
            NVL(IB.c.serial_number, IB.c.dup_serial_number).label("serial_number"),
            NVL(IB.c.duplicate_ib_flag, "N").label("duplicate_ib_flag"),
            IB.c.instance_status_desc.label("install_base_status"),
            IB.c.item_name.label("device_name"),
            IB.c.so_number.label("product_so"),
            IB.c.po_number.label("product_po"),
            FIX_DATES(IB.c.ship_date).label("ship_date_header"),
            # Joined from BV_MCE_AM_ENGAGEMENT_DATA in transient table
            IB.c.collector_host_name.label("collector_host_name"),
            IB.c.collector_matched.label("collector_matched"),
            IB.c.c3_matched.label("c3_matched"),
            IB.c.customer_matched.label("customer_matched"),
            IB.c.item_type_flag.label("item_type_flag"),
            IB.c.covered_status.label("coverage_status"),
            # IB_PRNT
            IB_PRNT.c.instance_number.label("parent_instance_number"),
            NVL(IB_PRNT.c.serial_number, IB_PRNT.c.dup_serial_number).label(
                "parent_serial_number"
            ),
            # ITEM_CTE
            FIX_DATES(ITEM_CTE.c.last_date_of_support).label(
                "product_last_date_of_support_ldos"
            ),
            ITEM_CTE.c.ib_product_type.label("product_type"),
            ITEM_CTE.c.product_family_mfg_descr.label("product_family_mfg_descr"),
            ITEM_CTE.c.product_family_description.label("product_family_description"),
            ITEM_CTE.c.description.label("product_description"),
            ITEM_CTE.c.product_family.label("product_family"),
            ITEM_CTE.c.business_entity_name_top.label("architecture"),
            ITEM_CTE.c.business_entity_desc_top.label("architecture_d"),
            ITEM_CTE.c.sub_business_entity_desc_top.label("sub_architecture_d"),
            ITEM_CTE.c.sub_business_entity_name_top.label("sub_architecture"),
            ITEM_CTE.c.product_list_price.label("product_list_price"),
            ITEM_CTE.c.product_list_price_gpl_us.label("global_product_list_price"),
            ITEM_CTE.c.serviceable_product_flag.label("serviceable_product_flag"),
            ITEM_CTE.c.service_list_price.label("service_list_price_raw"),
            # CVD_LINE_CTE
            NVL(CVD_LINE_CTE.c.dnr_flag, text("'N'")).label("dnr_flag"),
            CVD_LINE_CTE.c.maintenance_so_number.label("maintenance_so_number"),
            CVD_LINE_CTE.c.maintenance_po_number.label("maintenance_po_number"),
            CVD_LINE_CTE.c.price_negotiated.label("price_negotiated"),
            CVD_LINE_CTE.c.line_number.label("product_coverage_line_number"),
            FIX_DATES(CVD_LINE_CTE.c.start_date).label("product_coverage_start_date"),
            FIX_DATES(CVD_LINE_CTE.c.end_date).label("product_coverage_end_date"),
            FIX_DATES(CVD_LINE_CTE.c.date_terminated).label(
                "product_coverage_date_terminated"
            ),
            CVD_LINE_CTE.c.sts_code.label("sts_code"),
            NVL(CVD_LINE_CTE.c.usd_price_unit, CVD_LINE_CTE.c.price_unit).label(
                "usd_prorated_list_price"
            ),
            (
                NVL(CVD_LINE_CTE.c.usd_price_unit, CVD_LINE_CTE.c.price_unit)
                * IB.c.quantity
            ).label("usd_extended_list_price"),
            IB.c.quantity.label("quantity"),
            # ISITE_CTE
            ISITE_CTE.c.site_use_id.label("installed_at_site_id"),
            ISITE_CTE.c.party_name.label("installed_at_customer_name"),
            ISITE_CTE.c.address.label("installed_at_address_lines"),
            ISITE_CTE.c.city.label("installed_at_city"),
            ISITE_CTE.c.country_name.label("installed_at_country"),
            ISITE_CTE.c.postal_code.label("installed_at_postal_code"),
            ISITE_CTE.c.state.label("installed_at_state_province"),
            ISITE_CTE.c.gu_id.label("installed_at_gu_id"),
            ISITE_CTE.c.gu_name.label("installed_at_gu_name"),
            IFF(ITEM_CTE.c.mapped_to_service_flag == "YES WITH SPM", "T", "F").label(
                "mapped_to_service_flag"
            ),
            # HDR_CORE_CTE
            HDR_CORE_CTE.c.contract_number.label("contract_number"),
            HDR_CORE_CTE.c.bill_to_customer_name.label(
                "contract_bill_to_customer_name"
            ),
            HDR_CORE_CTE.c.billto_cr_party_name.label("bill_to_party_name"),
            HDR_CORE_CTE.c.billto_gu_name.label("contract_bill_to_customer_gu_name"),
            HDR_CORE_CTE.c.bill_to_country.label("contract_bill_to_country"),
            HDR_CORE_CTE.c.service_line_name.label("service_level"),
            HDR_CORE_CTE.c.service_line_sts_code.label("service_level_status"),
            # Multi Table Calculated Fields
            case(
                (CVD_LINE_CTE.c.sts_code.is_not(NULL), CVD_LINE_CTE.c.sts_code),
                (
                    CVD_LINE_CTE.c.sts_code.is_(NULL),
                    case(
                        (IB.c.covered_status == "A", "ACTIVE"),
                        (IB.c.covered_status == "I", "EXPIRED"),
                        (IB.c.covered_status == "N", "NEVER COVERED"),
                    ),
                ),
                else_="NEVER COVERED",
            ).label("product_coverage_status"),
        )
        .select_from(IB)
        .join(
            IB_PRNT,
            # When IB has multiple contracts, this causes duplicate rows
            and_(
                (IB.c.parent_instance_id == IB_PRNT.c.instance_id),
                (IB.c.contract_id == IB_PRNT.c.contract_id),
            ),
            isouter=True,
        )
        .join(
            ISITE_CTE,
            IB.c.install_at_site_use_id == ISITE_CTE.c.site_use_id,
            isouter=True,
        )
        .join(
            CVD_LINE_CTE,
            and_(
                (IB.c.instance_id == CVD_LINE_CTE.c.instance_id),
                (IB.c.contract_id == CVD_LINE_CTE.c.contract_id),
            ),
            isouter=True,
        )
        .join(
            HDR_CORE_CTE,
            and_(
                CVD_LINE_CTE.c.contract_id == HDR_CORE_CTE.c.contract_id,
                CVD_LINE_CTE.c.service_line_id == HDR_CORE_CTE.c.service_line_id,
            ),
            isouter=True,
        )
        .join(
            ITEM_CTE,
            IB.c.inventory_item_id == ITEM_CTE.c.inventory_item_id,
            isouter=True,
        )
    )

    # engine.execute(IB_TRANSIENT_STMT, engagement_id=39589)  # 22k instances

    stmt = (
        str(query.compile(dialect=dialect, compile_kwargs={"literal_binds": True}))
        .replace('"', "")
        .replace("%(engagement_id)s", ":engagement_id")
    )
    stmt = format_sql(stmt)
    return stmt


@click.command()
@click.option(
    "-tn",
    "--transient_table_name",
    help="Transient Table Name",
    type=str,
    required=False,
)
@click.option(
    "-c",
    "--catalog",
    help="Catalog, such as CPS_DB to use for transient table creation",
    type=str,
    required=False,
)
@click.option(
    "-s",
    "--schema",
    type=str,
    required=False,
    help="Schema Name for transient table creation",
)
@click.option(
    "-t",
    "--target",
    type=click.Choice(choices=["transient", "query"]),
    default="transient",
    help="Target Query to Build",
)
@click.option("-e", "--engagement_id", help="Engagement ID", required=False, type=int)
@click.option("-o", "--output", help="Output File", type=click.File("w"), default="-")
@click.option(
    "-cp",
    "--clipboard",
    is_flag=True,
    help="Copy to Clipboard",
    default=False,
)
def build(
    transient_table_name: Optional[str],
    catalog: Optional[str],
    schema: Optional[str],
    target,
    engagement_id: Optional[int],
    output,
    clipboard,
):
    """
    Generate SQL Queries for MCE Macro

    Parameters
    ----------
    transient_table_name: Optional[str]
        Transient Table Name.
    catalog : Optional[str]
        Transient Table Catalog.
    schema : Optional[str]
        Transient Table Schema.
    target : str
        Target Query to Build. Options are "transient" or "query". Default is "transient"
    engagement_id : Optional[int]
        Engagement ID. If not None, will generate the query with the engagement ID. Otherwise, will leave it templated.
    output : click.File
        Output File. Default is stdout
    clipboard : bool
        Copy to Clipboard. Default is False

    Notes
    -----
    The transient table name will be parameterized if not all of transient_table_name, catalog, and schema are provided.
    """

    dialect = SnowflakeDialect()
    stmt, transient_table_name, transient_table = build_transient_table(
        catalog=catalog, schema=schema, name=transient_table_name, dialect=dialect
    )
    if engagement_id:
        stmt = stmt.replace(":engagement_id", str(engagement_id))
    if target == "transient":
        output.write(stmt)
        if clipboard:
            pyperclip.copy(stmt)
        return
    query_stmt = build_query(table_transient=transient_table, dialect=dialect)
    if engagement_id:
        query_stmt = query_stmt.format(engagement_id=str(engagement_id))
    output.write(query_stmt)
    if clipboard:
        pyperclip.copy(query_stmt)


if __name__ == "__main__":
    build()
