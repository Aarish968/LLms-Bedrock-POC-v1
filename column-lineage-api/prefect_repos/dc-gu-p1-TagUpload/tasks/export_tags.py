from __future__ import annotations

import tempfile
from io import BytesIO
from pathlib import Path
from typing import Optional, Union

import boto3
import pandas as pd
from pydantic import parse_obj_as
from sqlalchemy import create_engine, text, bindparam, Integer, column, String

from common import sec
from common.config import RunSettings, SheetSettings
from common.models import GetTagRow, LocalFileUri, S3FileUri, FileUri, WorkbookSheets


def get_engagement_tags(dc_engagement_id: int, run_settings: RunSettings) -> list[GetTagRow]:
    """Get the tags available for the engagement"""

    engine = create_engine(
        sec.get_sf_pw(
            sec.check_env(run_settings.db_env),
            run_settings.get_warehouse(),  # Warehouse
            run_settings.schema_api,  # Schema
        ),
        connect_args={"log_max_query_length": 10_000},
    )

    get_tags_query = (
        text(
            """
        SELECT t.TAG_NAME AS TAG_NAME, t.TAG_ID AS TAG_ID,
         ts.TAGSET_ID AS TAGSET_ID, ts.TAGSET_NAME AS TAGSET_NAME,
        ts.TAGSET_TYPE AS TAGSET_TYPE, ts.SCOPE AS TAGSET_SCOPE
        FROM IDENTIFIER(:dc_tags) t 
        JOIN IDENTIFIER(:dc_tagset) ts ON (ts.TAGSET_ID = t.TAGSET_ID)
        WHERE (
        ts.DC_ENGAGEMENT_ID = :dc_engagement_id
        OR
        ts.SCOPE = 'Global'
        ) AND (
            NVL(t.IS_DELETED, 'F') = 'F'
            AND NVL(ts.IS_DELETED, 'F') = 'F'
        )
        """
        )
        .bindparams(
            bindparam("dc_tags", run_settings.dc_tags),
            bindparam("dc_tagset", run_settings.dc_tagset),
            bindparam("dc_engagement_id", dc_engagement_id, type_=Integer),
        )
        .columns(
            column("tag_name", String),
            column("tag_id", Integer),
            column("tagset_id", Integer),
            column("tagset_name", String),
            column("tagset_type", String),
            column("tagset_scope", String),
        )
    )

    with engine.connect() as conn:
        tags = conn.execute(get_tags_query).all()

    tags_parsed = parse_obj_as(list[GetTagRow], tags) if tags else []
    return tags_parsed


def make_tag_workbook(tags: list[GetTagRow], dc_engagement_id: int, sheet_settings: SheetSettings) -> WorkbookSheets:
    """
    Makes the tag workbook
    Parameters
    ----------
    sheet_settings
    tags : list[GetTagRow]
    dc_engagement_id : int

    Returns
    -------
    TagWorkbookExport
    """
    # Tag Sets - Tags
    df_tags = pd.DataFrame((t.dict() for t in tags))
    df_tags = df_tags[list(sheet_settings.ws_tagset.column_dtypes.keys())].convert_dtypes()

    # Placeholder for InstanceID - Tag
    df_instance_tag = pd.DataFrame(columns=list(sheet_settings.ws_tagset_mappings.column_dtypes.keys()))

    # EngagementID
    df_engagement_id = pd.DataFrame(columns=list(sheet_settings.ws_engagement_id.column_dtypes.keys()),
                                    data=[dc_engagement_id])

    # Hidden sheet with info
    df_info_sheet = pd.DataFrame(columns=list(sheet_settings.ws_info.column_dtypes.keys()),
                                 data=[sheet_settings.spreadsheet_upload_type])

    return WorkbookSheets(
        tagsets=df_tags, tag_mapping=df_instance_tag, engagement_id=df_engagement_id, info_sheet=df_info_sheet
    )


def tag_export(
        dc_engagement_id: int, run_settings: RunSettings, export_path: Optional[FileUri]
) -> Union[LocalFileUri, S3FileUri, BytesIO]:
    """
    Task that performs the tag export to Workbook

    Parameters
    ----------

    dc_engagement_id : int
    run_settings : RunSettings
    export_path : LocalFileUri | S3FileUri
        Where to store the workbook

    Returns
    -------

    """

    tags = get_engagement_tags(dc_engagement_id=dc_engagement_id, run_settings=run_settings)

    # Package the workbook

    if export_path is None:
        pd_path = BytesIO()
    elif export_path.file_type == "local":
        pd_path = export_path.uri
    elif export_path.file_type == "s3":
        pd_path = tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False).name
    else:
        raise ValueError(f"Unknown file type {export_path.file_type}")

    sheets: WorkbookSheets = make_tag_workbook(tags=tags, dc_engagement_id=dc_engagement_id,
                                               sheet_settings=run_settings.ss)

    with pd.ExcelWriter(pd_path, engine="xlsxwriter") as writer:
        sheets.tagsets.to_excel(writer, sheet_name=run_settings.ss.ws_tagset.sheet_name, index=False)
        sheets.tag_mapping.to_excel(writer, sheet_name=run_settings.ss.ws_tagset_mappings.sheet_name, index=False)
        sheets.engagement_id.to_excel(writer, sheet_name=run_settings.ss.ws_engagement_id.sheet_name, index=False)
        sheets.info_sheet.to_excel(writer, sheet_name=run_settings.ss.ws_info.sheet_name, index=False)

        for sheet_name in writer.sheets.keys():
            writer.sheets[sheet_name].autofit()

        info_sheet = writer.sheets["info"]
        info_sheet.hide()

    if export_path is None:
        pd_path.seek(0)
        return pd_path
    elif export_path.file_type == "s3":
        # Upload to S3
        s3 = boto3.client("s3")
        s3.upload_file(pd_path, export_path.bucket_name, export_path.key)
        Path(pd_path).unlink()
        return export_path
    else:
        return export_path
