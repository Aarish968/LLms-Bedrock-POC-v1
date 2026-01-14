import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any, Iterable

import pandas as pd
import xlsxwriter
from xlsxwriter.utility import xl_pixel_width

WORKBOOK_DEFAULT_OPTIONS = {
    "default_date_format": "yyyy-mm-dd",
    "strings_to_urls": False,
    "constant_memory": True,
}

if TYPE_CHECKING:
    from xlsxwriter.worksheet import Worksheet

XL_MAX_ROWS = 2**20

logger = logging.getLogger(__name__)


def _get_column_widths(df: pd.DataFrame) -> dict[int, int]:
    """
    In constant memory, we can't use the autofit feature
    So we find the longest string in each column (including the header) and call
    xl_pixel_width

    Returns a zero-indexed dictionary of column widths keyed by the column index
    """
    # Start with the header widths
    cols: dict[int, int] = {
        idx: xl_pixel_width(col) + 4 for idx, col in enumerate(df.columns.tolist())
    }

    for col_idx, header_size in cols.items():
        col_series = df.iloc[:, col_idx]
        max_len_idx = (
            col_series.map(str).str.len().idxmax() if not col_series.empty else None
        )
        max_len_str = str(col_series[max_len_idx]) if max_len_idx is not None else ""
        cols[col_idx] = max(header_size, xl_pixel_width(max_len_str) + 4)
    return cols


def _blank_nan(worksheet: "Worksheet", row: int, col: int, number: Any, format=None):  # noqa: A002, ANN001, ANN202
    """Xlsxwriter write handler to write blank cells for NaNs"""
    if pd.isna(number):
        logger.debug("Writing blank cell at row %d, col %d for NaN value", row, col)
        return worksheet.write_blank(row, col, None, format)
    else:
        return None


def _enforce_max_rows(df: pd.DataFrame) -> pd.DataFrame:
    """
    Enforce the maximum number of rows for Excel files.

    If the DataFrame exceeds XL_MAX_ROWS, it will be truncated to the first XL_MAX_ROWS rows.
    """
    n_rows = len(df)
    if n_rows > XL_MAX_ROWS:
        logger.warning(
            "DataFrame has %d rows, which exceeds the maximum of %d rows for Excel. "
            "Truncating to the first %d rows.",
            n_rows,
            XL_MAX_ROWS,
            XL_MAX_ROWS,
        )
        df = df.iloc[:XL_MAX_ROWS]
    return df


def _create_handler_worksheet(
    workbook: xlsxwriter.Workbook, sheet_name: str
) -> "Worksheet":
    """
    Create a worksheet with a custom write handler for NaN values.

    This function is used to ensure that NaN values are written as blank cells in the Excel file.
    """
    ws = workbook.add_worksheet(name=sheet_name)
    ws.add_write_handler(float, _blank_nan)
    ws.add_write_handler(type(pd.NA), _blank_nan)
    ws.add_write_handler(type(pd.NaT), _blank_nan)
    return ws


def write_to_excel(
    df: pd.DataFrame,
    output: str | Path,
    sheet_name: str = "Sheet1",
) -> Path:
    """
    Writing a DataFrame to an Excel file, can be resource-intensive and may cause OOM errors

    This function uses the xlsxwriter library, along with constant memory mode, to write the DataFrame to an Excel file.

    The result is greatly improved performance, but with the loss of some features, such as:
    - No autofit for columns (we set the width to the max length of the string in the column)
    - No tables

    """

    df = _enforce_max_rows(df)
    n_rows = len(df)

    logger.info("Writing %d rows to Excel", n_rows)

    output_stream = Path(output)

    with xlsxwriter.Workbook(
        str(output_stream), options=WORKBOOK_DEFAULT_OPTIONS
    ) as workbook:
        ws = _create_handler_worksheet(workbook=workbook, sheet_name=sheet_name)
        # Write the header
        ws.write_row(0, 0, df.columns.tolist())
        # Write the data
        for row_num, row in enumerate(df.itertuples(index=False), start=1):
            ws.write_row(row_num, 0, row)

        # Set column widths
        col_widths = _get_column_widths(df)
        for col_num, width in enumerate(col_widths.values()):
            ws.set_column_pixels(col_num, col_num, width)

    return output_stream


TSheetParam = tuple[str, pd.DataFrame]


def write_to_excel_workbook(
    dfs: Iterable[TSheetParam],
    output: str | Path,
) -> Path:
    """
    Similar to `write_to_excel`, but writes multiple DataFrames to a single Excel file with multiple sheets.

    Parameters:
        dfs: An iterable of tuples, where each tuple contains a sheet name and a DataFrame. Such as
              `("Sheet1", df1), ("Sheet2", df2), ...`
        output: The path to the output Excel file.
    """
    output_stream = Path(output)

    with xlsxwriter.Workbook(
        str(output_stream), options=WORKBOOK_DEFAULT_OPTIONS
    ) as workbook:
        for sheet_name, df in dfs:
            df_sized = _enforce_max_rows(df=df)
            n_rows = len(df_sized)
            logger.info("Processing sheet '%s' with %d rows", sheet_name, n_rows)

            ws = _create_handler_worksheet(workbook=workbook, sheet_name=sheet_name)
            # Write the header
            ws.write_row(0, 0, df_sized.columns.tolist())
            # Write the data
            for row_num, row in enumerate(df_sized.itertuples(index=False), start=1):
                ws.write_row(row_num, 0, row)

            # Set column widths
            col_widths = _get_column_widths(df_sized)
            for col_num, width in enumerate(col_widths.values()):
                ws.set_column_pixels(col_num, col_num, width)

    logger.info("Excel file written to %s", output_stream)

    return output_stream
