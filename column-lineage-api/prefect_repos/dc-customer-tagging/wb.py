from __future__ import annotations
from common import aws_sec
from io import BytesIO
from pathlib import Path
from typing import Callable
import boto3
import json
import awswrangler as wr
import pandas as pd
import prefect
from pandas import ExcelWriter
from prefect import task
from xlsxwriter.format import Format
from xlsxwriter.utility import xl_rowcol_to_cell, xl_range
from xlsxwriter.worksheet import Worksheet
import numpy as np
from common.config import FlowParams, RunSettings



def populate_summary_sheet(
    writer: "ExcelWriter",
    n_multi: int,
    n_single: int,
    n_unscoped: int,
    n_unknown: int,
    run_settings: RunSettings,
) -> bool:
    """

    Populate the summary sheet to the workbook. This provides a short summary of Serial Numbers
    and if there are any to be resolved.

    Parameters
    ----------
    run_settings
    writer : pd.ExcelWriter
    n_multi : int
        Number of Serial Numbers that have multiple Instance IDs
    n_single : int
        Number of Serial Numbers that have a single Instance ID
    n_unscoped : int
        Number of Serial Numbers that are not in scope, and appear in the "Not in Scope" sheet
    n_unknown : int
        Number of Serial Numbers that are unknown, and appear in the "Unknown" sheet
    Notes
    -----
    The message on the summary sheet will be generated from the following logic:
        - State: If n_multi > 0 || n_single > 0, then "State: Conflict"
        - State: If n_multi == 0 && n_single == 0 && n_unscoped > 0 || n_unknown > 0, then "State: Out of Scope Found"
        - State: If n_multi == 0 && n_single == 0 && n_unscoped == 0 && n_unknown == 0 then "State: Resolved"
        - Sheet: If State is not "Conflict", then create "InstanceID - Tag mapping" sheet

    Returns
    -------
    bool
        True if is_conflict, False otherwise. This is used to determine if the "InstanceID - Tag mapping" sheet
        should be created.
    """

    Colors = run_settings.OutputSheetFormatting

    def sheet_to_url(sn: str) -> str:
        return f"internal:'{sn}'!A1" if " " in sn else f"internal:{sn}!A1"

    workbook = writer.book



    text_format = {"font_color": "black", "border": 1, "text_wrap": True}
    link_format = {"font_color": "blue", "border": 1, "underline": 1}
    bg_ok = {"bg_color": Colors.green}
    bg_caution = {"bg_color": Colors.yellow}
    bg_warn = {"bg_color": Colors.red}
    bg_gray = {"bg_color": Colors.gray}

    summary_cell = workbook.add_format(link_format)
    status_cell = workbook.add_format(text_format)
    value_cell_ok = workbook.add_format({**text_format, **bg_ok})
    value_cell_warn = workbook.add_format({**text_format, **bg_warn})
    value_cell_caution = workbook.add_format({**text_format, **bg_caution})
    summary_break = workbook.add_format({**text_format, **bg_gray})

    def red_if_any(value: int):
        return value_cell_warn if value > 0 else value_cell_ok

    def yellow_if_any(value: int):
        return value_cell_caution if value > 0 else value_cell_ok

    def write_summary_row(
        row: int,
        sheet_url,
        msg: str,
        value: int,
        value_formatter: Callable[[int], "Format"],
    ):
        summary_sheet.write_url(
            row=row, col=0, url=sheet_url, string=msg, cell_format=summary_cell
        )
        value_format = value_formatter(value)
        summary_sheet.write_number(
            row=row, col=1, number=value, cell_format=value_format
        )

    SheetState = run_settings.OutputSheetStates


    conflict_level = SheetState.ok
    if n_multi > 0:
        conflict_level |= SheetState.error
    if n_single > 0:
        conflict_level |= SheetState.error
    if n_unscoped > 0:
        conflict_level |= SheetState.caution
    if n_unknown > 0:
        conflict_level |= SheetState.caution

    is_conflict = bool(conflict_level & SheetState.error)

    OutputNames = run_settings.OutputSheetNames

    summary_sheet = workbook.get_worksheet_by_name(OutputNames.summary)
    summary_sheet.set_column(0, 0, 60)
    summary_sheet.set_column(1, 1, 30)
    row_idx = 0
    write_summary_row(
        row_idx,
        sheet_to_url(OutputNames.single),
        "Serial Numbers with Single Instance ID",
        n_single,
        red_if_any,
    )
    row_idx += 1
    write_summary_row(
        row_idx,
        sheet_to_url(OutputNames.multi),
        "Serial Numbers with Multiple Instance ID",
        n_multi,
        red_if_any,
    )
    row_idx += 1
    if n_unscoped > 0:
        write_summary_row(
            row_idx,
            sheet_to_url(OutputNames.unscoped),
            "Serial Numbers not in Scope",
            n_unscoped,
            yellow_if_any,
        )
        row_idx += 1
    if n_unknown > 0:
        write_summary_row(
            row_idx,
            sheet_to_url(OutputNames.not_found),
            "Unknown Serial Numbers",
            n_unknown,
            yellow_if_any,
        )
        row_idx += 1

    # Gray blank row
    for col in range(2):
        summary_sheet.write_string(row_idx, col, "", cell_format=summary_break)
    row_idx += 1

    # Final Status
    summary_sheet.write_string(row_idx, 0, "Status", cell_format=status_cell)
    if (conflict_level & SheetState.error) > 0:
        msg = "Conflicts detected - Resolve before tagging."
        summary_sheet.write_string(row_idx, 1, msg, cell_format=value_cell_warn)
        row_idx += 1
    elif (conflict_level & SheetState.caution) > 0:
        msg = "Review Serial Numbers not in Scope."
        summary_sheet.write_string(row_idx, 1, msg, cell_format=value_cell_caution)
        row_idx += 1
    else:
        msg = "Ready To Tag."
        tag_url = sheet_to_url(OutputNames.upload)
        summary_sheet.write_url(
            row_idx, 1, url=tag_url, string=msg, cell_format=value_cell_ok
        )
        row_idx += 1

    return is_conflict


def format_workbook(
    writer: "ExcelWriter", df_multi: pd.DataFrame, df_single: pd.DataFrame, run_settings
):
    """
    Apply formatting to the workbook.

    Requires that pd.ExcelWriter with engine="xlsxwriter" was used to create the workbook.


    Parameters
    ----------
    run_settings
    writer : pd.ExcelWriter
    df_single: pd.DataFrame
        The dataframe that was written to the workbook
    df_multi: pd.DataFrame
        The dataframe that was written to the workbook
    df_single: pd.DataFrame
        The dataframe that was written to the workbook
    run_settings: RunSettings

    Notes
    -----
    The following formatting is applied:
        - Mimic look of table (All Sheets)
        - Within Serial Number groups, alternate row colors (Multi,Single)
        - Color scale score (Multi)
    """

    def try_autofit(sheet: "Worksheet"):
        if hasattr(sheet, "autofit"):
            sheet.autofit()

    Colors = run_settings.OutputSheetFormatting
    SheetNames = run_settings.OutputSheetNames
    Names = run_settings.CommonNames
    workbook = writer.book

    fmt_header = workbook.add_format(
        {
            "bold": True,
            "bg_color": Colors.header,
            "font_color": "white",
            "right": 1,
            "right_color": "white",
            "bottom": 3,
            "bottom_color": "white",
            "left": 1,
            "left_color": "white",
        }
    )
    fmt_row = workbook.add_format(
        {"bg_color": Colors.row, "border": 1, "border_color": "white"}
    )
    fmt_row_alt = workbook.add_format(
        {"bg_color": Colors.row_alt, "border": 1, "border_color": "white"}
    )
    # Draws a thick black line at the bottom of the group
    fmt_grp_break = workbook.add_format({"bottom_color": "black", "bottom": 3})

    # Multi Sheet
    # Excel-based, alternating row colors based on serial_number
    sheet_multi = writer.sheets["Multi"]
    df_cols = df_multi.columns.tolist()
    col_serial = df_cols.index(run_settings.CommonNames.serial)
    col_score = df_cols.index(run_settings.CommonNames.score)

    # We add two columns, a "resolved" column, and a hidden "group" column
    df_cols.append(run_settings.CommonNames.resolved)
    df_cols.append("group")
    col_group = len(df_cols) - 1

    sheet_multi.write(1, col_group, 1)  # Starts the group counter at 1
    # Write the formula to increment the group counter by 1 every time the serial_number changes
    for row in range(2, len(df_multi) + 1):
        row_serial, row_prev_serial = xl_rowcol_to_cell(
            row, col_serial
        ), xl_rowcol_to_cell(row - 1, col_serial)
        row_prev_group = xl_rowcol_to_cell(row - 1, col_group)
        sheet_multi.write_formula(
            row,
            col_group,
            f"=IF({row_serial}={row_prev_serial}, {row_prev_group}, {row_prev_group}+1)",
        )

    sheet_multi.conditional_format(
        xl_range(1, col_score, len(df_multi), col_score),
        {
            "type": "3_color_scale",
            "min_color": Colors.red,
            "mid_color": Colors.yellow,
            "max_color": Colors.green,
            "min_type": "num",
            "mid_type": "num",
            "max_type": "num",
            "min_value": 0.0,
            "mid_value": 0.5,
            "max_value": 1.0,
        },
    )
    # Conditional formatting based on the score, which has to go before the row coloring, to work

    # Conditional formatting to alternate row colors based on the group counter
    range_group = xl_range(1, 0, len(df_multi), len(df_cols) - 1)
    fmt_form_row = f"ISODD({xl_rowcol_to_cell(1, col_group, col_abs=True)})"
    fmt_form_row_alt = f"ISEVEN({xl_rowcol_to_cell(1, col_group, col_abs=True)})"
    fmt_form_row_brk = (
        f"{xl_rowcol_to_cell(0, col_group, col_abs=True)}"
        f"={xl_rowcol_to_cell(1, col_group, col_abs=True)}"
    )
    sheet_multi.conditional_format(
        range_group, {"type": "formula", "criteria": fmt_form_row, "format": fmt_row}
    )
    sheet_multi.conditional_format(
        range_group,
        {"type": "formula", "criteria": fmt_form_row_alt, "format": fmt_row_alt},
    )
    sheet_multi.conditional_format(
        range_group,
        {"type": "formula", "criteria": fmt_form_row_brk, "format": fmt_grp_break},
    )

    sheet_multi.add_table(
        0,
        0,
        last_row=max(len(df_multi), 1),
        last_col=len(df_cols) - 1,
        options={
            "header_row": True,
            "name": SheetNames.multi,
            "autofilter": False,
            "columns": [
                {"header": col, "header_format": fmt_header} for col in df_cols
            ],
        },
    )
    # Hide the group counter column
    sheet_multi.set_column(col_group, col_group, None, None, {"hidden": True})

    # Single Sheet - Just a Simple Table
    sheet_single = writer.sheets[SheetNames.single]
    sheet_single.add_table(
        0,
        0,
        last_row=max(len(df_single), 1),
        last_col=len(df_single.columns) - 1,
        options={
            "header_row": True,
            "name": SheetNames.single,
            "autofilter": False,
            "columns": [{"header": col} for col in df_single.columns.tolist()],
        },
    )

    # # Tagging Sheet
    # # Here we just add a formula to pull out rows with a resolved value of TRUE from Multi, and All from Single (if any)
    # # As a user tags the rows in the Multi sheet, the output will update
    # sheet_tagged = writer.sheets[SheetNames.tagging]
    # sheet_tagged.write_formula(
    #     1,
    #     0,
    #     f"=IF(COUNTA(Single[{Names.instance_id}])>0"
    #     f",VSTACK(Single[{Names.instance_id}],"
    #     f"FILTER(Multi[{Names.instance_id}],"
    #     f'Multi[{Names.resolved}],"")),'
    #     f"FILTER(Multi[{Names.instance_id}],"
    #     f'Multi[{Names.resolved}],""))',
    # )
    # # A formula that will populate the 'Resolved' tag id when the adjacent column is not blank
    # for row in range(1, len(df_multi) + 1 + len(df_single) + 1):
    #     sheet_tagged.write_formula(
    #         row=row,
    #         col=1,
    #         formula=f'=IF(LEN({xl_rowcol_to_cell(row, 0)})=0, "", {run_settings.tag_resolved})',
    #     )

    # For all sheets, try to autofit the columns
    for sheet in writer.sheets.values():
        try_autofit(sheet)


@task(log_stdout=True, tags=["snowflake_xsmall", "has_result"],nout = 3)
def package_workbook(
    multi_same_parent_df: pd.DataFrame,
    df_query: pd.DataFrame,
    df_resolved: pd.DataFrame,
    unscoped: set[str],
    unknown: set[str],
    flow_params: FlowParams,
    run_settings: RunSettings,
) -> str:
    """
    Create the Workbook for the user to download or the programmatic invocation to use.

    Parameters
    ----------
    df_query : pd.DataFrame
        The dataframe containing the query results
    df_resolved : pd.DataFrame
        The dataframe containing the resolved Serial Numbers and Instance IDs
    unscoped : set[str]
        Iterable of strings containing any unscoped Serial Numbers
    unknown : set[str]
        Iterable of strings containing any unknown Serial Numbers
    flow_params: FlowParams
    run_settings: RunSettings

    Create DataFrames for these results:
    - Not Found Results (not a valid serial number)
    - Unscoped Results (not resolved, not in scope)
    - Single Results (not resolved, but no decision needed) EJ : we can tag this
    - Multi Results (not resolved, decision needed) EJ: we chose number 1 from here , or multi if they share a parent id
    - Resolved Results (resolved, fyi only), created above ej : this should be named previously resolved
    """

    print('#########################################')
    print(df_query)
    print('#########################################')
    # print(multi_same_parent_df)
    cols = df_query.columns.tolist()
    """ 
    Formatting requested:
    - Have install_base_status column adjacent to coverage_status column
    """
    cols.pop(cols.index("install_base_status"))
    cols.insert(cols.index("coverage_status") + 1, "install_base_status")
    df_query = df_query[cols]

    #From df, remove any rows that have a resolved serial number
    df_query = df_query.loc[
        ~df_query[run_settings.serial_col_name].isin(
            df_resolved[run_settings.serial_col_name]
        )
    ]

    # df_resolved = df_query.loc[
    #     df_query[run_settings.serial_col_name].isin(
    #         df_resolved[run_settings.serial_col_name]
    #     )
    # ]

    df_single = df_query.loc[df_query["is_multi"] == "N"]
    df_multi = df_query.loc[df_query["is_multi"] == "Y"]

    print("Query Results".center(60, "-"))
    print(f"# Missing: {len(unknown)}".ljust(8))
    # print(f"# Unscoped: {len(unscoped)}".ljust(8))
    print(f"# Single: {df_single.shape[0]}".ljust(8))
    print(f"# Multi: {df_multi.shape[0]}".ljust(8))
    print(f"# Multi_Same_Parent: {multi_same_parent_df.shape[0]}".ljust(8))
    print(f"# Resolved: {df_resolved.shape[0]}".ljust(8))



    del df_query

    df_single.drop(columns=["is_multi"], inplace=True)
    df_multi.drop(columns=["is_multi"], inplace=True)

    df_single.sort_values(
        by=[run_settings.serial_col_name],
        ignore_index=True,
        ascending=[False],
        inplace=True,
    )

    # Apply sorting to the Multi DataFrame for ease of use
    df_multi.sort_values(
        by=[run_settings.serial_col_name, run_settings.score_rank_col_name],
        ignore_index=True,
        ascending=[False, True],
        inplace=True,
    )


    # Apply sorting to the Multi DataFrame for ease of use
    multi_same_parent_df.sort_values(
        by=[run_settings.serial_col_name, run_settings.score_rank_col_name],
        ignore_index=True,
        ascending=[False, True],
        inplace=True,
    )

    # df_unscoped = pd.DataFrame(unscoped, columns=[run_settings.serial_col_name])
    # df_unscoped.sort_values(
    #     by=[run_settings.serial_col_name],
    #     ignore_index=True,
    #     ascending=[False],
    #     inplace=True,
    # )

    df_unknown = pd.DataFrame(unknown, columns=[run_settings.serial_col_name])

    # Empty sheet to include for tagging. We will use formulas to populate this sheet.
    df_tagging = pd.DataFrame(
        columns=[run_settings.instance_id_col_name, run_settings.tag_id_col_name]
    )
    # Empty sheet to reserve sheet name
    df_dummy = pd.DataFrame()

    # {df_unknown} #Not in Cisco
    # {df_unscoped} #Not in GUID
    # {df_single} #nothing to resolve
    # {df_multi[['instance_id', 'score_ranked']]} #multi
    # {df_resolved} #resolved


    fp = BytesIO()
    writer = pd.ExcelWriter(fp, engine="xlsxwriter")
    # df_dummy.to_excel(
    #     writer, sheet_name="Summary", index=False
    # )
    if len(df_unknown) > 0:
        df_unknown.to_excel(
            writer, sheet_name="Missing", index=False
        )
    # if len(df_unscoped) > 0:
    #     df_unscoped.to_excel(
    #         writer, sheet_name="Not in GUID", index=False
    #     )
    df_single.to_excel(
        writer, sheet_name="Single", index=False
    )
    df_multi.to_excel(writer, sheet_name="Multi", index=False)
    multi_same_parent_df.to_excel(writer, sheet_name="Multi with same Parent", index=False)
    df_resolved.to_excel(
        writer, sheet_name="Previously Resolved", index=False
    )
    # df_tagging.to_excel(
    #     writer, sheet_name="Conflict Tagging", index=False
    # )

    # has_conflicts = populate_summary_sheet(
    #     writer,
    #     n_multi=df_multi[run_settings.serial_col_name].nunique(),
    #     n_single=df_single[run_settings.serial_col_name].nunique(),
    #     n_unscoped=df_unscoped[run_settings.serial_col_name].nunique(),
    #     n_unknown=df_unknown[run_settings.serial_col_name].nunique(),
    #     run_settings=run_settings,
    # )
    format_workbook(writer, df_multi, df_single, run_settings)

    # If there are no conflicts, we include Config.SN_UPLOAD sheet
    # These are populated by resolved instance_ids
    # if not has_conflicts:
    #     df_upload = df_resolved[[run_settings.instance_id_col_name]].rename(
    #         columns={run_settings.instance_id_col_name: "Instance_ID"}
    #     )
    #     df_upload["Tag_ID"] = run_settings.CommonNames.tag_id
    #     df_upload.to_excel(
    #         writer, sheet_name="InstanceID - Tag mapping", index=False
    #     )
    #     # Hide the Conflict Tagging sheet
    #     writer.book.get_worksheet_by_name("Conflict Tagging").hide()

    # Ensure the summary sheet is the first sheet
    # workbook = writer.book
    # worksheet = workbook.get_worksheet_by_name("Summary")


    writer.close()
    fp.seek(0)

    df_single['instance_id'] = df_single['instance_id'].astype(np.int64)
    df_multi['instance_id'] = df_multi['instance_id'].astype(np.int64)
    df_resolved['instance_id'] = df_resolved['instance_id'].astype(np.int64)
    multi_same_parent_df['instance_id'] = multi_same_parent_df['instance_id'].astype(np.int64)

    df_single.set_index('instance_id', inplace=True)


    df_multi = df_multi[df_multi['score_rank'] == 1]
    df_multi.set_index('instance_id', inplace=True)

    multi_same_parent_df.set_index('instance_id', inplace=True)


    df_resolved.set_index('instance_id', inplace=True)


    output_json = f"""   
    {dict( single = df_single['serial_number'].to_dict())} ,
    {dict( multi = df_multi['serial_number'].to_dict())} ,
    {dict( multi_same_parent = multi_same_parent_df['serial_number'].to_dict())} ,
    {dict( resolved =df_resolved['serial_number'].to_dict())} ,
    """


    #filter to only rank 1 multis for metrics
    df_multi_rank_1 = df_multi[df_multi['score_rank'] == 1]

    metrics_json = {
    "single" : df_single['serial_number'].to_dict(),
    "multi" : df_multi_rank_1['serial_number'].to_dict(),
    "multi_same_parent" : multi_same_parent_df['serial_number'].to_dict(),
    "resolved" : df_resolved['serial_number'].to_dict(),
    "excel_location" : flow_params.excel_output_uri
    }

    print(metrics_json)




    session = boto3.Session(
        aws_access_key_id=aws_sec.ACCESS_KEY,
        aws_secret_access_key=aws_sec.SECRET_KEY
    )

    s3 = session.resource('s3')
    s3object = s3.Object(flow_params.serial_resolution_s3_bucket,
                         f'{flow_params.sf_env}/json/{flow_params.request_id}.json')
    s3object.put(
        Body=(bytes(json.dumps(output_json).encode('UTF-8'))))

    print(f"Writing result Excel File to {flow_params.excel_output_uri}")
    wr.s3.upload(fp, flow_params.excel_output_uri)
    # with open("output.xlsx", "wb") as f:
    #     f.write(fp.getbuffer())

    return flow_params.excel_output_uri, flow_params.json_output_uri, metrics_json


@task(log_stdout=True, tags=["snowflake_xsmall", "has_result"],nout = 3)
def package_workbook_for_instance(
    multi_same_parent_df: pd.DataFrame,
    df_query: pd.DataFrame,
    df_resolved: pd.DataFrame,
    unscoped: set[str],
    unknown: set[str],
    flow_params: FlowParams,
    run_settings: RunSettings,
) -> str:
    """
    Create the Workbook for the user to download or the programmatic invocation to use.

    Parameters
    ----------
    df_query : pd.DataFrame
        The dataframe containing the query results
    df_resolved : pd.DataFrame
        The dataframe containing the resolved Serial Numbers and Instance IDs
    unscoped : set[str]
        Iterable of strings containing any unscoped Serial Numbers
    unknown : set[str]
        Iterable of strings containing any unknown Serial Numbers
    flow_params: FlowParams
    run_settings: RunSettings

    Create DataFrames for these results:
    - Not Found Results (not a valid serial number)
    - Unscoped Results (not resolved, not in scope)
    - Single Results (not resolved, but no decision needed) EJ : we can tag this
    - Multi Results (not resolved, decision needed) EJ: we chose number 1 from here , or multi if they share a parent id
    - Resolved Results (resolved, fyi only), created above ej : this should be named previously resolved
    """

    # print('#########################################')
    # print(df_query)
    # print('#########################################')
    # print(multi_same_parent_df)
    # cols = df_query.columns.tolist()
    # """
    # Formatting requested:
    # - Have install_base_status column adjacent to coverage_status column
    # """
    # cols.pop(cols.index("install_base_status"))
    # cols.insert(cols.index("coverage_status") + 1, "install_base_status")
    # df_query = df_query[cols]

    #From df, remove any rows that have a resolved serial number
    # df_query = df_query.loc[
    #     ~df_query[run_settings.serial_col_name].isin(
    #         df_resolved[run_settings.serial_col_name]
    #     )
    # ]

    # df_resolved = df_query.loc[
    #     df_query[run_settings.serial_col_name].isin(
    #         df_resolved[run_settings.serial_col_name]
    #     )
    # ]

    # df_single = df_query.loc[df_query["is_multi"] == "N"]
    # df_multi = df_query.loc[df_query["is_multi"] == "Y"]

    # print("Query Results".center(60, "-"))
    print(f"# Missing: {len(unknown)}".ljust(8))
    # # print(f"# Unscoped: {len(unscoped)}".ljust(8))
    # print(f"# Single: {df_single.shape[0]}".ljust(8))
    # print(f"# Multi: {df_multi.shape[0]}".ljust(8))
    # print(f"# Multi_Same_Parent: {multi_same_parent_df.shape[0]}".ljust(8))
    print(f"# Resolved: {df_resolved.shape[0]}".ljust(8))


    #
    # del df_query

    # df_single.drop(columns=["is_multi"], inplace=True)
    # df_multi.drop(columns=["is_multi"], inplace=True)
    #
    # df_single.sort_values(
    #     by=[run_settings.serial_col_name],
    #     ignore_index=True,
    #     ascending=[False],
    #     inplace=True,
    # )
    #
    # # Apply sorting to the Multi DataFrame for ease of use
    # df_multi.sort_values(
    #     by=[run_settings.serial_col_name, run_settings.score_rank_col_name],
    #     ignore_index=True,
    #     ascending=[False, True],
    #     inplace=True,
    # )


    # Apply sorting to the Multi DataFrame for ease of use
    # multi_same_parent_df.sort_values(
    #     by=[run_settings.serial_col_name, run_settings.score_rank_col_name],
    #     ignore_index=True,
    #     ascending=[False, True],
    #     inplace=True,
    # )

    # df_unscoped = pd.DataFrame(unscoped, columns=[run_settings.serial_col_name])
    # df_unscoped.sort_values(
    #     by=[run_settings.serial_col_name],
    #     ignore_index=True,
    #     ascending=[False],
    #     inplace=True,
    # )

    df_unknown = pd.DataFrame(unknown, columns=['instance_id'])

    # Empty sheet to include for tagging. We will use formulas to populate this sheet.
    df_tagging = pd.DataFrame(
        columns=[run_settings.instance_id_col_name, run_settings.tag_id_col_name]
    )
    # Empty sheet to reserve sheet name
    df_dummy = pd.DataFrame()

    # {df_unknown} #Not in Cisco
    # {df_unscoped} #Not in GUID
    # {df_single} #nothing to resolve
    # {df_multi[['instance_id', 'score_ranked']]} #multi
    # {df_resolved} #resolved


    fp = BytesIO()
    writer = pd.ExcelWriter(fp, engine="xlsxwriter")
    # df_dummy.to_excel(
    #     writer, sheet_name="Summary", index=False
    # )
    if len(df_unknown) > 0:
        df_unknown.to_excel(
            writer, sheet_name="Missing", index=False
        )
    # if len(df_unscoped) > 0:
    #     df_unscoped.to_excel(
    #         writer, sheet_name="Not in GUID", index=False
    #     )
    # df_single.to_excel(
    #     writer, sheet_name="Single", index=False
    # )
    # df_multi.to_excel(writer, sheet_name="Multi", index=False)
    # multi_same_parent_df.to_excel(writer, sheet_name="Multi with same Parent", index=False)
    df_resolved.to_excel(
        writer, sheet_name="Previously Resolved", index=False
    )
    # df_tagging.to_excel(
    #     writer, sheet_name="Conflict Tagging", index=False
    # )

    # has_conflicts = populate_summary_sheet(
    #     writer,
    #     n_multi=df_multi[run_settings.serial_col_name].nunique(),
    #     n_single=df_single[run_settings.serial_col_name].nunique(),
    #     n_unscoped=df_unscoped[run_settings.serial_col_name].nunique(),
    #     n_unknown=df_unknown[run_settings.serial_col_name].nunique(),
    #     run_settings=run_settings,
    # )
    # format_workbook(writer, df_multi, df_single, run_settings)

    # If there are no conflicts, we include Config.SN_UPLOAD sheet
    # These are populated by resolved instance_ids
    # if not has_conflicts:
    #     df_upload = df_resolved[[run_settings.instance_id_col_name]].rename(
    #         columns={run_settings.instance_id_col_name: "Instance_ID"}
    #     )
    #     df_upload["Tag_ID"] = run_settings.CommonNames.tag_id
    #     df_upload.to_excel(
    #         writer, sheet_name="InstanceID - Tag mapping", index=False
    #     )
    #     # Hide the Conflict Tagging sheet
    #     writer.book.get_worksheet_by_name("Conflict Tagging").hide()

    # Ensure the summary sheet is the first sheet
    # workbook = writer.book
    # worksheet = workbook.get_worksheet_by_name("Summary")


    writer.close()
    fp.seek(0)

    # df_single['instance_id'] = df_single['instance_id'].astype(np.int64)
    # df_multi['instance_id'] = df_multi['instance_id'].astype(np.int64)
    df_resolved['instance_id'] = df_resolved['instance_id'].astype(np.int64)
    # multi_same_parent_df['instance_id'] = multi_same_parent_df['instance_id'].astype(np.int64)
    #
    # df_single.set_index('instance_id', inplace=True)
    #
    #
    # df_multi = df_multi[df_multi['score_rank'] == 1]
    # df_multi.set_index('instance_id', inplace=True)

    # multi_same_parent_df.set_index('instance_id', inplace=True)


    # df_resolved.set_index('instance_id', inplace=True)


    output_json = f"""   
    {dict( single = df_unknown['instance_id'].to_dict())} ,
    {dict( resolved =df_resolved['instance_id'].to_dict())} ,
    """


    #filter to only rank 1 multis for metrics
    # df_multi_rank_1 = df_multi[df_multi['score_rank'] == 1]

    metrics_json = {
        "df_unknown": df_unknown['instance_id'].to_dict(),
    "resolved" : df_resolved['instance_id'].to_dict(),
    "excel_location" : flow_params.excel_output_uri
    }

    print(metrics_json)




    session = boto3.Session(
        aws_access_key_id=aws_sec.ACCESS_KEY,
        aws_secret_access_key=aws_sec.SECRET_KEY
    )

    s3 = session.resource('s3')
    s3object = s3.Object(flow_params.serial_resolution_s3_bucket,
                         f'{flow_params.sf_env}/json/{flow_params.request_id}.json')
    s3object.put(
        Body=(bytes(json.dumps(output_json).encode('UTF-8'))))

    print(f"Writing result Excel File to {flow_params.excel_output_uri}")
    wr.s3.upload(fp, flow_params.excel_output_uri)
    # with open("output.xlsx", "wb") as f:
    #     f.write(fp.getbuffer())

    return flow_params.excel_output_uri, flow_params.json_output_uri, metrics_json
