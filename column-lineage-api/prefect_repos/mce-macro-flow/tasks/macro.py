from __future__ import annotations

import re
from datetime import datetime
from functools import partial
from pathlib import Path
from typing import Optional

import pandas as pd
from dateutil.relativedelta import relativedelta
from pandas import Timestamp

from common.repo import EGridMap


def replicate_macro(
    df: pd.DataFrame, customer_name: Optional[str], e_grid_path: str | Path
) -> pd.DataFrame:
    """
    Replicate the Excel Macro
    Parameters
    ----------
    df : pd.DataFrame
        DataFrame resulting from the SQL query
    customer_name: Optional[str]
        Customer name to include, optional
    e_grid_path: str | Path

    """

    id2desc = {
        "1.1": "Cisco + Collector",
        "1.2": "Cisco + Customer",
        "1.3": "Fully Aligned",
        "2.0": "Collector Only",
        "2.1": "Collector + Customer",
        "3.0": "Customer Only",
        "4.0": "Cisco Only",
    }

    RE_ZONE_NUMBER = re.compile(r"ZONE\s*(\d+\.?\d*)", re.IGNORECASE)

    def add_customer_name() -> str:
        """
        Populates values for 'Customer Name', Column A
        """
        return customer_name if customer_name else ""

    def zone_format(s: str) -> str:
        """
        Populates values for 'Zone Assignment', Column F
        Format the zone column to match the expected format.

        Examples
        --------
        "ZONE1.0" -> "Zone 1.0"
        "ZONE 1.0" -> "Zone 1.0"

        """

        # Get the zone number

        zone_match = RE_ZONE_NUMBER.search(s)
        if not zone_match:
            return s
        zone_number = zone_match.group(1)
        if not zone_number:
            return s
        return f"Zone {zone_number}"

    def renew_flag_format(s: str) -> str:
        """
        References column 'Renewable', Column W

        This is essentially flipping boolean values of Y and N
        """
        if s.lower() == "y":
            return "N"
        elif s.lower() == "n":
            return "Y"
        else:
            return s

    def zone_desc_format(s: str) -> str:
        """
        Populates values for 'Zone Description', Column G
        References the column 'Zone Assignment', Column F
        """
        zone_id = RE_ZONE_NUMBER.search(s).group(1)
        if not zone_id:
            return ""

        desc = id2desc.get(zone_id)
        if not desc:
            return ""
        return desc

    def ship_data_flag(d: datetime | Timestamp):
        """
        Populates values for column 'YES' <sic>, Column AD
        References column 'Best AvailableShipDate' <sic>, Column AC

        If the date, d is greater than today - 5 years, then return 'YES' , else return 'NO'
        """
        if d > (datetime.now() - relativedelta(years=5)).date():
            return "YES"
        else:
            return "NO"

    def coverage_status_format(s: str) -> str:
        """
        Populates values for 'Coverage Status', Column AR
        References the column 'Covered Line Status', Column AQ


        Notes
        -----
        Excel formula:
            IF(OR(AQ2="ACTIVE",AQ2="SIGNED",AQ2="OVERDUE")=TRUE,"COVERED","UNCOVERED")
        """
        if s in {"ACTIVE", "SIGNED", "OVERDUE"}:
            return "COVERED"
        else:
            return "UNCOVERED"

    def expiration_range_format(d: datetime | Timestamp) -> str:
        """
        Populates values for 'Expiration Range', Column BB
        References the column 'Covered Line EndDate', Column BA

        Notes
        -----
        Excel formula:
            =IF(BA2="","Never Covered",IF(BA2<TODAY(),"Expired",IF(BA2<TODAY()+30,"Expiration < 1 Mo",IF(BA2<TODAY()+182,"1 Mo < Expiration < 6 Mos",
            IF(BA2<TODAY()+365,"6 Mos < Expiration < 12 Mos", "Expiration > 12 Mos")))))
        """

        today = datetime.now().date()

        if pd.isna(d) or d == "" or d is None:
            return "Never Covered"
        elif d < today:
            return "Expired"
        elif d < today + relativedelta(days=30):
            # Following Excel formula logic that 1 month is 30 days
            # TODO: Confirm this is desired behavior
            return "Expiration < 1 Mo"
        elif d < today + relativedelta(days=182):
            # Following Excel formula logic that 6 months is 182 days
            # TODO: Confirm this is desired behavior
            return "1 Mo < Expiration < 6 Mos"
        elif d < today + relativedelta(days=365):
            # Following Excel formula logic that 12 months is 365 days
            # TODO: Confirm this is desired behavior
            return "6 Mos < Expiration < 12 Mos"
        else:
            return "Expiration > 12 Mos"

    def ldos_category(d: datetime | Timestamp) -> str:
        """
        Populates values for 'LDOS Category', Column BE
        References column 'Last DateofSupport', Column BD, sic

        Notes
        -----
        =IF(BD2="","Not Announced",
        IF(BD2<TODAY(),"Past LDoS",
        IF(BD2<TODAY()+365,"LDoS < 12 Mos",
        IF(BD2<TODAY()+730,
        "12 Mos < LDoS < 24 Mos",
        "LDoS > 24 Mos"))))
        """

        today = datetime.now().date()

        if pd.isna(d) or d == "" or d is None:
            return "Not Announced"
        elif d < today:
            return "Past LDoS"
        elif d < today + relativedelta(days=365):
            # Following Excel formula logic that 12 months is 365 days
            # TODO: Confirm this is desired behavior (note year logic is different than expiration range)
            return "LDoS < 12 Mos"
        elif d < today + relativedelta(days=730):
            # Following Excel formula logic that 24 months is 730 days
            # TODO: Confirm this is desired behavior (note year logic is different than expiration range)
            return "12 Mos < LDoS < 24 Mos"
        else:
            return "LDoS > 24 Mos"

    def end_of_sale_format(d: datetime | Timestamp) -> datetime | None:
        """
        Populates values for 'End of Sale Date', Column BL
        References column 'Last DateofSupport', Column BD, sic

        Notes
        -----
        Excel formula:
            =IF(BD2="","",DATE(YEAR(BD2)-5,MONTH(BD2),DAY(BD2)))
        """
        if pd.isna(d) or d == "" or d is None:
            return None
        # TODO: The Excel formula is a naive date, and doesn't account for leap years
        else:
            return d - relativedelta(years=5)

    def service_level_format(s: str, grid: EGridMap) -> str:
        """
        Populates values for 'Service Level Description', Column AW
        References column 'Service Level', Column AV

        Notes
        -----
        Excel formula:
            =IFERROR(VLOOKUP(AV2,'EGrid Report'!D:E,2,0),"")
        """
        if pd.isna(s):
            return ""
        if s not in grid:
            return ""
        return grid.__root__[s].description

    def service_level_category_format(s: str, grid: EGridMap) -> str:
        """
        Populates values for 'Service Level Category', Column AX
        References column 'Service Level', Column AV

        Notes
        -----
        Excel formula:
            =IFERROR(VLOOKUP(AV2,'EGrid Report'!D:E,3,0),"")

        We aren't in Excel so we use a dictionary lookup instead of VLOOKUP
        """
        if pd.isna(s):
            return ""
        if s not in grid:
            return ""
        return grid.__root__[s].category

    def rename_columns(c: str):
        """
        Rename columns to match the expected format of the output file. Where possible, the column names
        are simply converted from snake_case to Title Case. However, some column names require special handling.
        For these, they are provided in the dictionary below
        """
        col_renames = {
            "contract_bill_to_country": "Contract Bill-To Country",
            "collector_host_name": "Host Name",
            "duplicate_ib_flag": "Duplicate Record",
            "device_name": "Product ID",
            "product_type": "Item Type",
            "architecture_d": "Architecture Group",
            "sub_architecture_d": "Architecture Sub Group",
            "install_base_status": "Installed Base Status",
            "quantity": "Product Quantity",
            "product_list_price": "Hardware List Price",
            "product_so": "Hardware SO Number",
            "product_po": "Hardware PO Number",
            "ship_date_header": "Best AvailableShipDate",
            "installed_at_address_lines": "Installed Site Address 1",
            "installed_at_city": "Installed Site City",
            "installed_at_country": "Installed Site Country",
            "installed_at_postal_code": "Installed Site Postal Code",
            "installed_at_gu_id": "GU ID",
            "installed_at_gu_name": "GU Name",
            "installed_at_site_id": "Installed-At SITE ID",
            "product_last_date_of_support_ldos": "Last DateofSupport",
            "product_coverage_status": "Covered Line Status",
            "serviceable_product_flag": "Serviceable",
            "config_type": "Product Relationship",
            "contract_bill_to_customer_name": "Contract Bill-to Name",
            "product_coverage_start_date": "Covered Line StartDate",
            "product_coverage_end_date": "Covered Line EndDate",
            "product_coverage_date_terminated": "Termination Date",
            "dnr_flag": "Renewable",
            "zone_id": "Zone Assignment",
            "ldos_category": "LDoS Category",
            "yes": "YES",
            "maintenance_po_number": "Maintenance PO Number",
            "maintenance_so_number": "Maintenance SO Number",
        }
        if c in col_renames:
            return col_renames[c]
        # Convert snake_case to Title Case
        return c.title().replace("_", " ")

    def order_and_fill_columns(frame: pd.DataFrame) -> pd.DataFrame:
        """
        Finally, we reorder the columns to match the expected output file. This requires reordering the columns
        and filling in any missing columns with empty values

        Returns
        -------

        """

        cols = [
            "Customer Name",
            "AssetID",
            "Unique Record",
            "Duplicate Record",
            "Duplicate Coverage",
            "Zone Assignment",
            "Zone Description",
            "Serial Number",
            "Instance Number",
            "Parent Serial Number",
            "Parent Instance Number",
            "Product Relationship",
            "Product ID",
            "Product Description",
            "Product Quantity",
            "Product Family",
            "Architecture Group",
            "Architecture Sub Group",
            "Item Type",
            "Network",
            "Installed Base Status",
            "Serviceable",
            "Renewable",
            "Host Name",
            "Hardware List Price",
            "Service$",
            "Hardware Bill to",
            "Ship-To Customer Name",
            "Best AvailableShipDate",
            "YES",
            "GU ID",
            "GU Name",
            "Region",
            "Business Unit",
            "Installed-At SITE ID",
            "Installed At Customer Name",
            "Installed Site Address 1",
            "Installed Site City",
            "Installed Site State",
            "Installed Site Province",
            "Installed Site Postal Code",
            "Installed Site Country",
            "Covered Line Status",
            "Coverage Status",
            "Contract Number",
            "Contract Bill-to Name",
            "Contract Bill-To Country",
            "Service Level",
            "Service Level Description",
            "Service Level Category",
            "Product Coverage Line Number",
            "Covered Line StartDate",
            "Covered Line EndDate",
            "Expiration Range",
            "Termination Date",
            "Last DateofSupport",
            "LDoS Category",
            "Warranty Type",
            "Warranty End Date",
            "Hardware PO Number",
            "Hardware SO Number",
            "Maintenance PO Number",
            "Maintenance SO Number",
            "End Of Sale Date",
            "End Of Routine Failure Analysis Date",
            "End of New Service Attachment Date",
            "End of Service Contract Renewal",
            "End Of Sig Releases Date",
            "End Of Security Support Date",
            "End Of Software Availability Date",
            "End Of Software License Availability Date",
            "End Of Software Date",
        ]

        cols_exist = set(frame.columns)
        cols_missing = list(set(cols) - cols_exist)

        def empty_col_filler(n_val: int, *args, **kwargs):
            return [pd.NA for _ in range(n_val)]

        filler = partial(empty_col_filler, len(cols_missing))

        if len(cols_missing) > 0:
            frame[cols_missing] = frame.apply(filler, axis=1, result_type="expand")

        frame = frame[cols]

        cols_dropped = list(set(cols_exist) - set(cols))
        if len(cols_dropped) > 0:
            print(f"Columns dropped: {cols_dropped}")

        return frame

    e_grid = EGridMap.parse_file(e_grid_path)
    df["customer_name"] = add_customer_name()
    df["zone_id"] = df["zone_id"].apply(zone_format)
    df["dnr_flag"] = df["dnr_flag"].fillna("").apply(renew_flag_format)
    df["zone_description"] = df["zone_id"].apply(zone_desc_format)

    df["yes"] = df["ship_date_header"].apply(ship_data_flag)

    df["coverage_status"] = df["sts_code"].apply(coverage_status_format)
    df["expiration_range"] = df["product_coverage_end_date"].apply(
        expiration_range_format
    )
    df["ldos_category"] = df["product_last_date_of_support_ldos"].apply(ldos_category)
    df["end_of_sale_date"] = df["product_last_date_of_support_ldos"].apply(
        end_of_sale_format
    )
    df["service_level_description"] = df["service_level"].apply(
        service_level_format, grid=e_grid
    )
    df["service_level_category"] = df["service_level"].apply(
        service_level_category_format, grid=e_grid
    )
    df["installed_site_state"] = df["installed_at_state_province"]
    # Copy this column to Province
    df["installed_site_province"] = df["installed_site_state"]
    df["duplicate_ib_flag"] = df["duplicate_ib_flag"].replace("N", "ORIGINAL")
    df.drop(columns=["installed_at_state_province"], inplace=True)
    df = df.rename(columns=rename_columns)
    df = order_and_fill_columns(df)

    return df
