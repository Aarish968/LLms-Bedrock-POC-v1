from __future__ import annotations

import os
import shutil
import tempfile
import zipfile
from pathlib import Path
from typing import Optional

import boto3
import numpy as np
import pandas as pd
import xlsxwriter
from boto3.s3.transfer import TransferConfig
from prefect import Task
from prefect.engine.signals import FAIL
from prefect.utilities.tasks import defaults_from_attrs

from src.common_tasks.utils import UploadProgressCallback, parse_s3_uri


class DataFrameUploadTask(Task):
    _tmp_files: list[str]
    _tmp_dirs: list[str]

    """
    Task that handles persisting a DataFrame to S3 or a local file.
    It provides a consistent interface, and implements several optimizations to reduce memory usage as well as
    handling Excel files that are larger than the maximum number of rows that Excel supports.
    """

    def __init__(
        self,
        xl_max_rows: int = 2**20,
        date_format: Optional[str] = "mm/dd/yyyy",
        output_uri: Optional[str] = None,
        file_path: Optional[str] = None,
        force_constant_memory: bool = False,
    ):
        """

        Parameters
        ----------
        xl_max_rows :
            The maximum number of rows to write to an Excel file. If the DataFrame has more rows than this, then the
            file will be split into multiple files and stored in a zip archive.
        date_format: Optional[str]
            The date format to use when writing dates to Excel. If not provided, the default format will be used.
        output_uri: Optional[str]
            If this is set, then subsequent tasks will be passed this S3 URI by default.
        file_path: Optional[str]
            If this is set, then subsequent tasks will be passed this local file path by default.
        """
        super().__init__()
        self.xl_max_rows = xl_max_rows
        self.date_format = date_format
        self.output_uri = output_uri
        self.file_path = file_path
        self.force_constant_memory = force_constant_memory
        self._tmp_files = []
        self._tmp_dirs = []
        self.log_stdout = True

    def _get_tmp_file(self) -> str:
        """Get a temporary file path, that will be the Task's responsibility to clean up"""
        temp_file = tempfile.NamedTemporaryFile(delete=False)
        self._tmp_files.append(temp_file.name)
        return temp_file.name

    def _get_tmp_dir(self) -> str:
        """Get a temporary directory, that will be the Task's responsibility to clean up"""
        temp_dir: str = tempfile.mkdtemp()
        self._tmp_dirs.append(temp_dir)
        return temp_dir

    def _clean_up(self):
        """Clean up any temporary files or directories that were created"""
        for temp_file in self._tmp_files:
            try:
                os.unlink(temp_file)
            except Exception:
                ...
        for temp_dir in self._tmp_dirs:
            try:
                shutil.rmtree(temp_dir, ignore_errors=True)
            except Exception:
                ...

    # noinspection PyMethodOverriding
    @defaults_from_attrs(
        "xl_max_rows", "date_format", "output_uri", "file_path", "force_constant_memory"
    )
    def run(
        self,
        df: pd.DataFrame,
        output_uri: Optional[str] = None,
        file_path: Optional[str] = None,
        boto3_session: Optional[boto3.Session] = None,
        xl_max_rows: Optional[int] = None,
        date_format: Optional[str] = None,
        force_constant_memory: bool = False,
    ):
        """
        Parameters
        ----------
        df : pd.DataFrame
        output_uri : Optional[str]
            If this is passed explicitly, then the value will override the default value set in the Task.
            If not None, then the DataFrame will be uploaded to this S3 URI.
        file_path : Optional[str]
            If this is passed explicitly, then the value will override the default value set in the Task.
            If not None, then the DataFrame will be saved to this local file path.
        boto3_session : Optional[boto3.Session]
            If this is set, then the file will be uploaded to S3 using this session. Otherwise, a session will be
            created using the default credentials.
        xl_max_rows : Optional[int]
            The maximum number of rows to write to an Excel file. If the DataFrame has more rows than this, then the
            file will be split into multiple files and stored in a zip archive.
        date_format: Optional[str]
            The date format to use when writing dates to Excel. Explicitly setting this will override the default
            format of the Task ('mm/dd/yyyy').
        force_constant_memory: bool
            If True, then the DataFrame will be written to an Excel file using constant memory, regardless of the
            number of rows. This overrides the heuristic that checks the number of rows in the DataFrame.

        Notes
        -----
        At least one of output_uri or file_path must be set. If both are set, then the file will be saved locally and
        uploaded to S3.


        Examples
        --------
        ```python
            from prefect import Flow
            from common_tasks.dataframes import DataFrameUploadTask
            upload_task = DataFrameUploadTask()
            with Flow("test") as flow:
                upload_task(df=df, output_uri="s3://my-bucket/my-file.xlsx")

        ```
        """
        if not any((output_uri, file_path)):
            raise FAIL("At least one of output_uri or file_path must be set")

        from prefect import context

        logger = context.get("logger")

        def blank_nan(worksheet, row, col, number, format=None):
            """Xlsxwriter write handler to write blank cells for NaNs"""
            if pd.isna(number):
                return worksheet.write_blank(row, col, None, format)
            else:
                return None

        def constant_memory_write(output: str | Path, frame: pd.DataFrame):
            """
            Write to Excel using constant memory and XlsxWriter. This flushes per row rather than storing the entire
            Worksheet in memory.
            """
            wb_options = dict(constant_memory=True)
            if date_format:
                wb_options["default_date_format"] = date_format.lower()

            with xlsxwriter.Workbook(
                output,
                options=wb_options,
            ) as workbook:
                ws = workbook.add_worksheet()
                ws.add_write_handler(float, blank_nan)
                # Write the header
                ws.write_row(0, 0, frame.columns.tolist())
                for row_num, row in enumerate(frame.itertuples(index=False), start=1):
                    ws.write_row(row_num, 0, [val for val in row])
            return output

        def output_as_archive(frame: pd.DataFrame) -> str:
            """
            We write several Excel files to a temporary directory and use some additional tricks to
            reduce the memory footprint of the task.

            Then, we create a zip archive of the files and store it in a temporary file.
            """
            sheet_dump_dir = Path(self._get_tmp_dir())
            sheet_files = []
            for i, chunk in enumerate(
                np.array_split(frame, len(frame) // self.xl_max_rows + 1), start=1
            ):
                print(f"{self.__class__.__name__} : Writing sheet {i}...")
                sheet_fp = sheet_dump_dir / f"Sheet_{i:06d}.xlsx"
                constant_memory_write(sheet_fp, chunk)
                sheet_files.append(sheet_fp)

            # Create the archive on disk
            # archive_fp = Path(sheet_dump_dir) / "archive.zip"
            archive_fp = self._get_tmp_file()
            with zipfile.ZipFile(archive_fp, "w", compression=zipfile.ZIP_STORED) as zf:
                for sheet_fp in sheet_files:
                    zf.write(sheet_fp, sheet_fp.name)

            return archive_fp

        def output_as_file(frame: pd.DataFrame) -> str:
            """
            Write the DataFrame to a temporary file and return the path
            """
            tmp_file = self._get_tmp_file()
            if self.force_constant_memory:
                logger.info(
                    f"{self.__class__.__name__}: Writing DataFrame to Excel using constant memory"
                )
                sheet_fp = constant_memory_write(tmp_file, frame)
                return sheet_fp
            else:
                writer_kwargs = dict(engine="xlsxwriter")
                if date_format:
                    writer_kwargs["date_format"] = date_format.upper()

                logger.info(
                    f"{self.__class__.__name__}: Writing DataFrame to {tmp_file=}"
                )
                with pd.ExcelWriter(tmp_file, **writer_kwargs) as writer:
                    frame.to_excel(writer, index=False)
                return tmp_file

        if len(df) >= self.xl_max_rows:
            logger.info(
                f"{self.__class__.__name__}: Writing DataFrame of Size : {len(df)} to Archive"
            )
            df_tmp = output_as_archive(df)
            # Ensure our output URI has the .zip extension
            if output_uri:
                output_uri_stem = output_uri.rsplit(".", 1)[0]
                output_uri = f"{output_uri_stem}.zip"
        else:
            logger.info(
                f"{self.__class__.__name__}: Writing DataFrame of Size : {len(df)} to File"
            )
            df_tmp = output_as_file(df)

        logger.info(f"{self.__class__.__name__}: DataFrame Written to {df_tmp}")

        try:
            if output_uri:
                logger.info(
                    f"{self.__class__.__name__}: Uploading DataFrame to {output_uri}"
                )
                s3 = boto3_session.client("s3") if boto3_session else boto3.client("s3")
                bucket_name, key = parse_s3_uri(output_uri)
                s3.upload_file(
                    df_tmp,
                    bucket_name,
                    key,
                    Callback=UploadProgressCallback(df_tmp, logger=logger),
                    Config=TransferConfig(use_threads=False),
                )
            if file_path:
                logger.info(
                    f"{self.__class__.__name__}: Writing DataFrame to {file_path}..."
                )
                Path(file_path).parent.mkdir(parents=True, exist_ok=True)
                shutil.copy(df_tmp, file_path)
        finally:
            self._clean_up()
