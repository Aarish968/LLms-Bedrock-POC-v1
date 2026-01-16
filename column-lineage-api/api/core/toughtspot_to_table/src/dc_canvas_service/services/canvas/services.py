import logging
from typing import TYPE_CHECKING, Callable, Optional, Union

from dc_canvas_service.common import Env, Settings
from dc_canvas_service.services.liveboard import (
    LiveboardService,
    update_table_for_visualizations,
    update_worksheet_references_in_liveboard,
)
from dc_canvas_service.services.s3 import S3Service
from dc_canvas_service.services.snowflake import (
    SnowflakeService,
    ThoughtSpotObjectRowInsert,
    create_ts_object,
    delete_liveboard,
    delete_ts_object,
    drop_canvas_data_table,
    drop_canvas_live_view,
    get_active_liveboards,
    get_canvas_metadata,
    get_column_types,
    get_liveboard_via_guid,
    get_pending_actions,
    get_thoughtspot_data_tables,
    get_ts_objects,
    mark_success_canvas_id,
    mark_success_request_id,
)
from dc_canvas_service.services.thoughtspot import (
    ThoughtSpotService,
    TSIdentity,
    TSTable,
    TSWorksheet,
)

from .exceptions import (
    CanvasLiveboardCreationError,
    CanvasNotFound,
    CanvasS3LocationNotFound,
    CanvasTableCreationError,
    CanvasTableNotFoundError,
    CanvasTMLCreationError,
    CanvasTSLiveboardPushError,
    CanvasTSTableAlreadyExists,
    CanvasTSWorksheetAlreadyExists,
    CanvasWorksheetCreationError,
)
from .models import CanvasParseActions
from .utils import (
    parse_change_json,
)

if TYPE_CHECKING:
    from sqlalchemy import Engine
    from thoughtspot_tml import Liveboard, Table, Worksheet

    from dc_canvas_service.services.snowflake import LiveboardRow

    from .models import CloneAction, CreateAction, UpdateAction

logger = logging.getLogger(__name__)


class CanvasService:
    def __init__(
        self,
        canvas_id: int,
        engagement_id: int,
        user_cisco_cco_id: str,
        env: Union[Env, str] = Env.dev,
        get_engine: Callable[[], "Engine"] | None = None,
    ):
        self.canvas_id = canvas_id
        self.engagement_id = engagement_id
        self.user_cisco_cco_id = user_cisco_cco_id

        self.settings = Settings(env)
        self.s3 = S3Service(aws_session=self.settings.aws_session)
        self.ts = ThoughtSpotService(settings=self.settings, s3=self.s3)
        self.sf = SnowflakeService(settings=self.settings, get_engine=get_engine)

        self.canvas_name = f"CANVAS_{canvas_id}"
        self.snowflake_table_name = f"{self.canvas_name}_THOUGHT_SPOT"
        self.snowflake_view_name = f"{self.canvas_name}_THOUGHT_SPOT_V"
        self.ts_table_name = self.get_ts_table_name(canvas_id)
        self.ts_worksheet_name = f"ws_canvas-{self.canvas_id}"
        self.ts_connection_name = f"CANVAS-{canvas_id}_CONNECTION"

        self.canvas_metadata = self.fetch_canvas_metadata()
        self.is_deleted = self.canvas_metadata.get("is_deleted") == "T"
        self.ts_connection = self._get_connection()
        self.ts_table = self._get_table()
        self.ts_worksheet = self._get_worksheet()

        logger.info(
            f"Canvas Service instantiated with metadata: {self.canvas_metadata}"
        )

    def fetch_canvas_metadata(self):
        """
        Fetches the metadata for a specific canvas using the provided session.

        Returns:
            The canvas metadata retrieved from the session.
        """

        with self.sf.conn_transaction() as conn:
            canvas_metadata = get_canvas_metadata(conn=conn, canvas_id=self.canvas_id)

        if not canvas_metadata:
            raise CanvasNotFound(self.canvas_id)
        return canvas_metadata

    def _get_table(self) -> Optional["Table"]:
        """
        Retrieves the table data by searching through Snowflake and ThoughtSpot.

        Returns:
            The table data if found; otherwise, None.
        """

        # Search in Snowflake
        with self.sf.conn_transaction() as conn:
            table_objects = get_ts_objects(
                conn=conn, canvas_id=self.canvas_id, object_names="table"
            )
            table_GUIDS = [row.guid for row in table_objects]

        # Search in ThoughtSpot
        if not table_GUIDS:
            table_ts_metadata = self.ts.search_tmls(
                name_patterns=[self.ts_table_name], export_type="LOGICAL_TABLE"
            )
            table_GUIDS = [
                guid
                for guid in table_ts_metadata
                if table_ts_metadata[guid] is not None
            ]

        if not table_GUIDS:
            return None

        ts_table_data = self.ts.get_tmls(metadata_GUIDs=table_GUIDS, tml_type="table")

        valid_tables = []
        for table_GUID in table_GUIDS:
            if ts_table := ts_table_data.get(table_GUID):
                valid_tables.append(ts_table)

        if valid_tables:
            return valid_tables[0]

        return None

    def _get_worksheet(self) -> Optional["Worksheet"]:
        """
        Retrieves the worksheet data by searching through Snowflake and ThoughtSpot.

        Returns:
            The worksheet data if found; otherwise, None.
        """

        # Search in Snowflake
        with self.sf.conn_transaction() as conn:
            worksheet_objects = get_ts_objects(
                conn=conn,
                canvas_id=self.canvas_id,
                object_names="worksheet",
            )
            worksheet_GUIDS = [row.guid for row in worksheet_objects]

        # Search in ThoughtSpot
        if not worksheet_GUIDS:
            worksheet_ts_metadata = self.ts.search_tmls(
                name_patterns=[self.ts_worksheet_name], export_type="LOGICAL_TABLE"
            )
            worksheet_GUIDS = [
                guid
                for guid in worksheet_ts_metadata
                if worksheet_ts_metadata[guid] is not None
            ]

        if not worksheet_GUIDS:
            return None

        ts_worksheet_data = self.ts.get_tmls(
            metadata_GUIDs=worksheet_GUIDS, tml_type="worksheet"
        )

        valid_worksheets = []
        for worksheet_GUID in worksheet_GUIDS:
            if ts_worksheet := ts_worksheet_data.get(worksheet_GUID):
                valid_worksheets.append(ts_worksheet)

        if valid_worksheets:
            return valid_worksheets[0]

        return None

    def _get_connection(self) -> "TSIdentity":
        """
        Retrieves or creates a connection with the specified name.

        Returns:
            TSIdentity: A `TSIdentity` object if an existing connection is found,
            otherwise a newly created connection object.
        """

        connection_guid = self.ts.get_connection_guid(self.ts_connection_name)

        if connection_guid:
            return TSIdentity(name=self.ts_connection_name, fqn=connection_guid)

        return self.ts.create_connection(
            connection_name=self.ts_connection_name, table_name=self.snowflake_view_name
        )

    @staticmethod
    def get_ts_table_name(canvas_id: int) -> str:
        """
        Get Thoughtspot table name for a canvas
        Args
            canvas_id:  Canvas ID
        Returns
            Thoughtspot spot LOGICAL TABLE name (a view name in Snowflake).
        """
        return f"CANVAS_{canvas_id}_THOUGHT_SPOT_V"

    def get_liveboards(self, guid: str | None = None) -> list[LiveboardService]:
        """
        Retrieves the liveboards data by searching through Snowflake and ThoughtSpot.

        :param: guid. (optional) A GUID of the only liveboard to return
        :return The LiveboardService Objects if found; otherwise, None.
        """

        # Search in Snowflake
        with self.sf.conn_transaction() as conn:
            liveboard_data = get_active_liveboards(conn=conn, canvas_id=self.canvas_id)
            liveboard_GUIDS = set(r.guid for r in liveboard_data)
            liveboard_GUID_ID_map = {
                r.guid: {
                    "liveboard_id": r.liveboard_id,
                    "parent_liveboard_id": r.parent_liveboard_id,
                }
                for r in liveboard_data
            }

        # Search in ThoughtSpot
        if not liveboard_GUIDS:
            liveboard_name_pattern = f"%E_{self.engagement_id} - C_{self.canvas_id}%"
            liveboard_ts_metadata = self.ts.search_tmls(
                name_patterns=[liveboard_name_pattern], export_type="LIVEBOARD"
            )
            liveboard_GUIDS = set(
                guid
                for guid in liveboard_ts_metadata
                if liveboard_ts_metadata[guid] is not None
            )

        if not liveboard_GUIDS:
            return None

        if guid:
            liveboard_GUIDS = {guid}

        ts_liveboards = self.ts.get_tmls(
            metadata_GUIDs=list(liveboard_GUIDS), tml_type="liveboard"
        )

        liveboards = []
        for ts_liveboard in ts_liveboards.values():
            if ts_liveboard:
                liveboard_id = parent_liveboard_id = None
                if liveboard_data and ts_liveboard.guid in liveboard_GUID_ID_map:
                    liveboard_id = liveboard_GUID_ID_map.get(ts_liveboard.guid).get(
                        "liveboard_id"
                    )
                    parent_liveboard_id = liveboard_GUID_ID_map.get(
                        ts_liveboard.guid
                    ).get("parent_liveboard_id")

                # Extract original name from existing liveboard (remove E_ and C_ tags)
                import re

                original_name = re.sub(
                    r" - E_\d+ - C_\d+$", "", ts_liveboard.liveboard.name
                )

                liveboard = LiveboardService(
                    ts=self.ts,
                    sf=self.sf,
                    ts_liveboard=ts_liveboard,
                    canvas_id=self.canvas_id,
                    engagement_id=self.engagement_id,
                    liveboard_name=original_name,
                    liveboard_id=liveboard_id,
                    parent_liveboard_id=parent_liveboard_id,
                )
                liveboards.append(liveboard)

        return liveboards

    def get_liveboards_guids(self) -> set:
        """
        Get the existing Canvas Liveboard GUID
        Returns:
            set: Set of Liveboard GUID
        """
        # Search in Snowflake
        with self.sf.conn_transaction() as conn:
            liveboard_data = get_active_liveboards(conn=conn, canvas_id=self.canvas_id)
            liveboard_GUIDS = set(r.guid for r in liveboard_data)

        # Search in ThoughtSpot
        if not liveboard_GUIDS:
            liveboard_name_pattern = f"%E_{self.engagement_id} - C_{self.canvas_id}%"
            liveboard_ts_metadata = self.ts.search_tmls(
                name_patterns=[liveboard_name_pattern], export_type="LIVEBOARD"
            )
            liveboard_GUIDS = set(
                guid
                for guid in liveboard_ts_metadata
                if liveboard_ts_metadata[guid] is not None
            )

        return liveboard_GUIDS

    def get_untracked_objects_guids_from_ts(self) -> dict[str, str]:
        """
        Get liveboard, view, and answer GUIDs directly from ThoughtSpot by searching for
        dependent objects of the canvas table.

        This method queries ThoughtSpot for all objects that depend on the canvas table,
        including liveboards, views, and answers that may not be tracked in the database.

        Returns:
            dict[str, str]: Dictionary mapping GUID to object type ('LIVEBOARD', 'VIEW', or 'ANSWER')
        """
        ts_dep_objects = self.ts.search_table_dependent_objects(
            CanvasService.get_ts_table_name(self.canvas_id)
        )

        # Filter to only LIVEBOARD, VIEW, and ANSWER types
        untracked_objects = {
            guid: obj_type
            for guid, obj_type in ts_dep_objects.items()
            if obj_type in ("LIVEBOARD", "VIEW", "ANSWER")
        }

        if not untracked_objects:
            return untracked_objects

        from collections import Counter

        counts = Counter(untracked_objects.values())
        count_parts = [
            f"{count} {obj_type.lower()}s" for obj_type, count in counts.items()
        ]
        logger.warning(
            f"Found {', '.join(count_parts)} in TS for canvas {self.canvas_id}"
        )
        return untracked_objects

    def _export_tml_to_s3(self, guid: str, ts_object_type: str) -> str | None:
        """
        Export a TML object from ThoughtSpot to S3.

        Args:
            guid: The object GUID to export
            ts_object_type: Type from ThoughtSpot ('LIVEBOARD', 'VIEW', 'ANSWER', etc.)

        Returns:
            str | None: S3 location if successful, None if TML could not be retrieved
        """
        # Convert TS object type to TML type (lowercase)
        tml_type = ts_object_type.lower()

        # Get TML from ThoughtSpot
        tml_dict = self.ts.get_tmls(metadata_GUIDs=[guid], tml_type=tml_type)
        tml_obj = tml_dict.get(guid)

        if not tml_obj:
            logger.warning(f"Could not retrieve TML for {ts_object_type} {guid}")
            return None

        # Extract object name generically using object type
        try:
            obj_attr = ts_object_type.lower()
            obj = getattr(tml_obj, obj_attr)
            obj_name = obj.name
        except (AttributeError, TypeError):
            obj_name = "unknown"

        logger.info(
            f"Exporting untracked {ts_object_type}: '{obj_name}' (GUID: {guid})"
        )

        # Generate S3 location using GUID as identifier
        s3_location = self.s3.make_liveboard_uri(
            bucket=self.settings.s3_bucket,
            env=self.settings.env,
            dest_type="canvas",
            object_id=str(self.canvas_id),
            liveboard_id=guid,  # type: ignore[arg-type]
        )

        # Upload to S3
        bucket, key = tuple(self.s3.parse_uri(s3_location))
        tml_contents = tml_obj.dumps().encode("utf-8")
        self.s3.upload_file(bucket=bucket, key=key, content=tml_contents)

        logger.info(
            f"Exported {ts_object_type} '{obj_name}' (GUID: {guid}) to {s3_location}"
        )
        return s3_location

    def delete_liveboards_via_guid(
        self, liveboard_GUID: str, liveboard_row: "LiveboardRow" = None
    ):
        """
        Deletes liveboard and updates the snowflake table with the details
        Args:
            liveboard_GUID (str): A Liveboard GUID to be deleted.
            liveboard_row (LiveboardRow): Snowflake Liveboard Row
        """

        if not liveboard_row:
            with self.sf.conn_transaction() as conn:
                liveboard_row = get_liveboard_via_guid(conn=conn, guid=liveboard_GUID)

        if liveboard_row:
            s3_location = liveboard_row.location
            bucket, key = tuple(self.s3.parse_uri(s3_location))

            new_s3_location = self.s3.make_liveboard_uri(
                bucket=self.settings.s3_bucket,
                env=self.settings.env,
                dest_type="delete",
                object_id=self.user_cisco_cco_id,
                liveboard_id=liveboard_row.liveboard_id,
            )
            _, new_key = tuple(self.s3.parse_uri(new_s3_location))
            self.s3.move_file(bucket, key, new_key)

            with self.sf.conn_transaction() as conn:
                delete_liveboard(
                    conn=conn,
                    liveboard_id=liveboard_row.liveboard_id,
                    location=new_s3_location,
                    updated_by=self.user_cisco_cco_id,
                )

        self.ts.delete_tmls(metadata_GUIDs=[liveboard_GUID], tml_type="liveboard")

    def get_pending_actions(
        self, request_id: int | None = None
    ) -> list["CanvasParseActions"] | None:
        """
        Retrieves actions associated with the specified canvas and optional request IDs.
        Args:
            request_id (int, optional): A list of request ID to filter the actions.
            If None, retrieves actions for all request IDs associated with the canvas.

        Returns:
            actions: The actions retrieved from the database for the specified canvas and request IDs.
        """

        with self.sf.conn_transaction() as conn:
            actions = get_pending_actions(
                conn=conn, canvas_id=self.canvas_id, request_id=request_id
            )
            if not actions:
                return

        new_actions = []

        for action in actions:
            change_json = action.changes_json
            with self.sf.conn_transaction() as conn:
                parsed_actions = parse_change_json(conn=conn, change_json=change_json)

            new_action = CanvasParseActions(
                **action.model_dump(), parsed_actions=parsed_actions
            )

            new_actions.append(new_action)

        return new_actions

    def mark_success(self, request_id: int) -> None:
        """
        Mark request_id as Success in DC_FILE_MANAGEMENT_RUNS.status
        Mark canvas_id as Success in DC_CANVAS_HDR.CANVAS_STATUS
        :param request_id:
        :return:
        """

        with self.sf.conn_transaction() as conn:
            mark_success_request_id(conn, request_id, updated_by=self.user_cisco_cco_id)
            mark_success_canvas_id(
                conn, self.canvas_id, updated_by=self.user_cisco_cco_id
            )

    def clean_ts(
        self,
        delete_liveboards: bool = True,
        delete_worksheet: bool = True,
        delete_table: bool = True,
    ):
        """
        Performs clean-up | Deletes TS Table & WorkSheet Objects | Soft Deletes Data in Snowflake
        If a canvas is already deleted, also drops Canvas live view and TS connection.
        Note: As this might impact multiple things make sure to use it at cause.
        Args:
            delete_liveboards (bool): Flag to toggle Liveboards delete
            delete_worksheet (bool): Flag to toggle Worksheet delete
            delete_table (bool): Flag to toggle Table delete
        """
        if delete_liveboards:
            if liveboard_GUIDs := self.get_liveboards_guids():
                for guid in liveboard_GUIDs:
                    self.delete_liveboards_via_guid(liveboard_GUID=guid)

            # Check for additional objects in TS that might not be tracked in DB
            untracked_objects = self.get_untracked_objects_guids_from_ts()

            if untracked_objects:
                logger.warning(
                    f"Found {len(untracked_objects)} untracked objects in TS "
                    f"for canvas {self.canvas_id}: {untracked_objects}"
                )

                for guid, obj_type in untracked_objects.items():
                    # Export TML to S3 before deletion
                    self._export_tml_to_s3(guid, obj_type)

                    # Delete from TS
                    self.ts.delete_tmls(
                        metadata_GUIDs=[guid],
                        tml_type=obj_type.lower(),  # type: ignore[arg-type]
                    )

        if delete_worksheet and self.ts_worksheet:
            self.ts.delete_tmls(
                metadata_GUIDs=[self.ts_worksheet.guid], tml_type="worksheet"
            )
            with self.sf.conn_transaction() as conn:
                delete_ts_object(
                    conn=conn,
                    canvas_id=self.canvas_id,
                    object_id=self.ts_worksheet.guid,
                    deleted_by=self.user_cisco_cco_id,
                )
            self.ts_worksheet = None

        if delete_table and self.ts_table:
            self.ts.delete_tmls(metadata_GUIDs=[self.ts_table.guid], tml_type="table")
            with self.sf.conn_transaction() as conn:
                delete_ts_object(
                    conn=conn,
                    canvas_id=self.canvas_id,
                    object_id=self.ts_table.guid,
                    deleted_by=self.user_cisco_cco_id,
                )
            self.ts_table = None

        if self.is_deleted:
            with self.sf.conn_transaction() as conn:
                drop_canvas_live_view(conn, self.canvas_id)
                drop_canvas_data_table(conn, self.canvas_id)

                # Verify both table and view are dropped
                if remaining := get_thoughtspot_data_tables(
                    conn, self.settings.sf_schema.value, self.canvas_id
                ):
                    raise Exception(
                        f"Canvas {self.canvas_id}: Failed to drop objects. Still exist: {remaining}"
                    )

            if str(self.canvas_id) in self.ts_connection_name:
                connection_guid = self.ts.get_connection_guid(self.ts_connection_name)
                if connection_guid:
                    self.ts.ts_session.connection_delete(connection_guid)

    def _create_ts_table(self):
        """
        Creates ThoughtSpot Table
        Returns:
            Table: If no TS Table Exists for the Canvas then Creates the table in TS
            and updates Snowflake Table.

        Raises:
            CanvasTSTableAlreadyExists: If Table already exists
        """

        if self.ts_table:
            msg = "TS Table already exists. Perform clean-up first."
            raise CanvasTSTableAlreadyExists(msg)

        table_data = TSTable(
            name=self.ts_table_name,
            description=f"Table for CANVAS_{self.canvas_id} | ENGAGEMENT_{self.engagement_id}",
            db=self.settings.sf_db,
            db_schema=self.settings.sf_schema,
            db_table=self.snowflake_view_name,
            connection=self.ts_connection,
        )

        ts_base_table = self.ts.create_base_table(table_data=table_data)

        with self.sf.conn_transaction() as conn:
            columns = get_column_types(
                conn=conn,
                schema=self.settings.sf_schema.value,
                canvas_id=self.canvas_id,
            )

        ts_base_table = self.ts.add_columns(
            tml_obj=ts_base_table, columns=columns, replace_all=True
        )

        push_details = self.ts.push_tmls(tml_objs=[ts_base_table])

        valid_responses = push_details.get("valid_responses")
        error_responses = push_details.get("error_responses")

        if error_responses:
            raise CanvasTableCreationError(error_responses)

        ts_table = valid_responses[0]

        with self.sf.conn_transaction() as conn:
            create_ts_object(
                conn=conn,
                ts_object=ThoughtSpotObjectRowInsert(
                    pinboard_name="table",
                    canvas_id=self.canvas_id,
                    dashboard_name=ts_table.table.name,
                    link=f"{self.ts.ts_table_base_link}/{ts_table.guid}",
                    object_uuid=ts_table.guid,
                    created_by=self.user_cisco_cco_id,
                ),
            )

        self.ts_table = ts_table

        self.ts.share_metadata(
            metadata_GUIDs=[self.ts_table.guid],
            metadata_type="LOGICAL_TABLE",
            users=self.canvas_metadata.get("users"),
            share_mode="READ_ONLY",
        )

    def _create_ts_worksheet(self):
        """
        Creates ThoughtSpot Worksheet
        Returns:
            Worksheet: If no TS Worksheet Exists for the Canvas then Creates the worksheet in TS
            and updates Snowflake Table.

        Raises:
            CanvasTableNotFoundError: If TS Table Not Found
            CanvasTSWorksheetAlreadyExists: If Worksheet already exists
        """

        if not self.ts_table:
            msg = "Create TS Table prior to Worksheet"
            raise CanvasTableNotFoundError(msg)

        if self.ts_worksheet:
            msg = "TS Worksheet already exists. Perform clean-up first."
            raise CanvasTSWorksheetAlreadyExists(msg)

        worksheet_data = TSWorksheet(
            name=self.ts_worksheet_name,
            description=f"Worksheet for CANVAS_{self.canvas_id} | ENGAGEMENT_{self.engagement_id}",
            tables=[TSIdentity(name=self.ts_table.name, fqn=self.ts_table.guid)],
        )

        ts_base_worksheet = self.ts.create_base_worksheet(worksheet_data=worksheet_data)

        with self.sf.conn_transaction() as conn:
            columns = get_column_types(
                conn=conn,
                schema=self.settings.sf_schema.value,
                canvas_id=self.canvas_id,
            )

        ts_base_worksheet = self.ts.add_columns(
            tml_obj=ts_base_worksheet,
            columns=columns,
            replace_all=True,
            table_name=self.ts_table.table.name,
        )

        # Add formulas from template
        ts_base_worksheet = self.ts.add_formulas_to_worksheet(ts_base_worksheet)

        push_details = self.ts.push_tmls(tml_objs=[ts_base_worksheet])

        valid_responses = push_details.get("valid_responses")
        error_responses = push_details.get("error_responses")

        if error_responses:
            raise CanvasWorksheetCreationError(error_responses)

        ts_worksheet = valid_responses[0]

        with self.sf.conn_transaction() as conn:
            create_ts_object(
                conn=conn,
                ts_object=ThoughtSpotObjectRowInsert(
                    pinboard_name="worksheet",
                    canvas_id=self.canvas_id,
                    dashboard_name=ts_worksheet.worksheet.name,
                    link=f"{self.ts.ts_table_base_link}/{ts_worksheet.guid}",
                    object_uuid=ts_worksheet.guid,
                    created_by=self.user_cisco_cco_id,
                ),
            )

        self.ts_worksheet = ts_worksheet

        self.ts.share_metadata(
            metadata_GUIDs=[self.ts_worksheet.guid],
            metadata_type="LOGICAL_TABLE",
            users=self.canvas_metadata.get("users"),
            share_mode="MODIFY",
        )

    def create_liveboard_service(self, action: "CreateAction") -> "LiveboardService":
        """
        Create Liveboard Service using the action
        Args:
            action: A Dictionary containing the details for creating Liveboard

        Returns:
            LiveboardService: New Created Liveboard

        Raises:
            CanvasS3LocationNotFound: If Liveboard ID or Location details is not present in SF
        """
        liveboard_name = action.liveboard_name
        liveboard_row = action.liveboard_row

        s3_location = liveboard_row.location
        parent_liveboard_id = action.liveboard_id

        if not s3_location:
            msg = "File Location | Liveboard ID not found in SF table"
            raise CanvasS3LocationNotFound(msg)

        liveboard = self.ts.create_liveboard_from_s3(s3_location=s3_location)

        # Apply E_ and C_ tags for ThoughtSpot
        from .utils import formulate_ts_liveboard_name

        enriched_name = formulate_ts_liveboard_name(
            liveboard_name=liveboard_name,
            engagement_id=self.engagement_id,
            canvas_id=self.canvas_id,
        )
        liveboard.liveboard.name = enriched_name

        table_detail = {
            "id": self.ts_worksheet.worksheet.name,
            "name": self.ts_worksheet.worksheet.name,
            "fqn": self.ts_worksheet.guid,
        }

        liveboard.liveboard.tables = [table_detail]

        liveboard = update_table_for_visualizations(
            table_detail=table_detail, liveboard=liveboard
        )

        # Update worksheet references in parameter_overrides and ordered_chips
        liveboard = update_worksheet_references_in_liveboard(
            liveboard=liveboard,
            new_worksheet_name=self.ts_worksheet_name,
        )

        ts_response = self.ts.push_tmls(tml_objs=[liveboard])
        if not (response := ts_response.get("valid_responses")):
            raise CanvasTSLiveboardPushError(ts_response)

        liveboard = response[0]

        liveboard_service = LiveboardService(
            ts=self.ts,
            sf=self.sf,
            ts_liveboard=liveboard,
            canvas_id=self.canvas_id,
            engagement_id=self.engagement_id,
            liveboard_name=liveboard_name,
            parent_liveboard_id=parent_liveboard_id,
        )

        return liveboard_service

    def create_liveboard(
        self, action: "CreateAction", overwrite: bool = False
    ) -> ("LiveboardService", dict):
        """
        Creates liveboard using specified action data and manages existing ThoughtSpot resources.
        Args:
            action ("CreateAction"): An actions that define how a liveboard should be created.
            overwrite (bool): If True, existing ThoughtSpot resources (tables, worksheets) will be
                cleaned and recreated. Defaults to False.

        Returns:
            tuple: A tuple containing:
                LiveboardService: The successfully created liveboard service instance.
                dict: A dictionaries with details about any errors encountered, containing:
                    - "action": The action that caused the error.
                    - "error": The exception raised during processing.
        """

        if not self.ts_table or not self.ts_worksheet:
            overwrite = True

        if overwrite:
            self.overwrite_thoughtspot()

        users = self.canvas_metadata.get("users")
        liveboard_error = {}
        new_liveboard = None

        try:
            new_liveboard = self.create_liveboard_service(action=action)

            new_liveboard.add_custom_actions()  # Also pushes the TML to TS
            new_liveboard.push_tml_to_s3_and_db(
                user_cisco_cco_id=self.user_cisco_cco_id
            )
            new_liveboard.share_liveboard(users=users, share_mode="MODIFY")

        except Exception as e:
            logger.exception("create_liveboard has failed.")
            liveboard_error.update({"action": action, "error": e})

        return new_liveboard, liveboard_error

    def update_liveboards(
        self, liveboards: list["LiveboardService"], action: "UpdateAction"
    ) -> dict:
        """
        Updates Liveboards display_name or shares Liveboards with Users
        Args:
            liveboards (list[LiveboardService]): Liveboards to be updates
            action (UpdateAction): Update Action dict


        Returns:
            liveboards: Updated Liveboards
        """
        unassigned_users: set = set()

        for liveboard in liveboards:
            match action.action_type:
                case "display_name":
                    liveboard.update_display_name(new_name=action.new_name)
                case "share_content":
                    share_details = liveboard.share_liveboard(
                        share_mode=action.share_mode, users=action.users
                    )
                    unassigned_users.update(share_details.get("unassigned_users"))

            liveboard.push_tml_to_s3_and_db(user_cisco_cco_id=self.user_cisco_cco_id)

        return {"liveboards": liveboards, "unassigned_users": unassigned_users}

    def clone_liveboard(
        self, action: "CloneAction", overwrite: bool = False
    ) -> "Liveboard":
        """
        Clones an existing liveboard with a new name and adds it to the current list of liveboards.
        Args:
            action ("CloneAction"): An actions that define how a liveboard should be cloned.
            overwrite (bool): If True, existing ThoughtSpot resources (tables, worksheets) will be
                cleaned and recreated. Defaults to False.

        Returns:
            new_liveboard: The newly created liveboard object.

        Raises:
            CanvasLiveboardCreationError: If there are any errors during the creation of the new liveboard.
        """
        if not self.ts_table or not self.ts_worksheet:
            overwrite = True

        if overwrite:
            self.overwrite_thoughtspot()

        users = self.canvas_metadata.get("users")

        existing_liveboard_GUID = action.liveboard_row.guid

        liveboards_response = self.ts.get_tmls(
            metadata_GUIDs=[existing_liveboard_GUID],
            tml_type="liveboard",
        )

        if existing_liveboard_GUID not in liveboards_response:
            raise Exception("Liveboard Not Found")

        ts_liveboard = liveboards_response.get(existing_liveboard_GUID)

        ts_liveboard.guid = ""

        # Apply E_ and C_ tags for ThoughtSpot
        from .utils import formulate_ts_liveboard_name

        enriched_name = formulate_ts_liveboard_name(
            liveboard_name=action.liveboard_name,
            engagement_id=self.engagement_id,
            canvas_id=self.canvas_id,
        )
        ts_liveboard.liveboard.name = enriched_name

        table_detail = {
            "id": self.ts_worksheet.worksheet.name,
            "name": self.ts_worksheet.worksheet.name,
            "fqn": self.ts_worksheet.guid,
        }

        ts_liveboard.liveboard.tables = [table_detail]

        ts_liveboard = update_table_for_visualizations(
            table_detail=table_detail, liveboard=ts_liveboard
        )

        # Update worksheet references in parameter_overrides and ordered_chips
        ts_liveboard = update_worksheet_references_in_liveboard(
            liveboard=ts_liveboard,
            new_worksheet_name=self.ts_worksheet_name,
        )

        push_details = self.ts.push_tmls(tml_objs=[ts_liveboard], create_new=True)

        valid_responses = push_details.get("valid_responses")
        error_responses = push_details.get("error_responses")

        if error_responses:
            raise CanvasLiveboardCreationError(error_responses)

        new_liveboard = valid_responses[0]

        new_liveboard_service = LiveboardService(
            ts=self.ts,
            sf=self.sf,
            ts_liveboard=new_liveboard,
            canvas_id=self.canvas_id,
            engagement_id=self.engagement_id,
            liveboard_name=action.liveboard_name,
            parent_liveboard_id=action.liveboard_id,
        )

        new_liveboard_service.push_tml_to_s3_and_db(
            user_cisco_cco_id=self.user_cisco_cco_id
        )
        new_liveboard_service.share_liveboard(users=users, share_mode="MODIFY")

        return new_liveboard

    def refresh_ts_datasource(self) -> None:
        """
        Updates TS DataSource with the Updated View data and Columns
        """

        if not self.ts_table or not self.ts_worksheet:
            msg = "Nothing to refresh. Table and/or Worksheet are not in TS."
            logger.info(msg)
            return

        ts_table = self.ts_table
        ts_worksheet = self.ts_worksheet

        with self.sf.conn_transaction() as conn:
            columns = get_column_types(
                conn=conn,
                schema=self.settings.sf_schema.value,
                canvas_id=self.canvas_id,
            )

        ts_table = self.ts.add_columns(
            tml_obj=ts_table, columns=columns, replace_all=True
        )
        ts_worksheet = self.ts.add_columns(
            tml_obj=ts_worksheet,
            columns=columns,
            replace_all=True,
            table_name=ts_table.table.name,
        )

        # Add formulas from template (same as in _create_ts_worksheet)
        ts_worksheet = self.ts.add_formulas_to_worksheet(ts_worksheet)

        # Push Table & Worksheet
        push_details = self.ts.push_tmls(tml_objs=[ts_table, ts_worksheet])
        error_responses = push_details.get("error_responses")
        if error_responses:
            raise CanvasTMLCreationError(error_responses)

        self.ts_table = ts_table
        self.ts_worksheet = ts_worksheet

    def overwrite_thoughtspot(self):
        """
        Performs TS Clean-up & Creating Table, Worksheet
        """
        logger.info("Performing ThoughtSpot Clean-up")
        self.clean_ts()

        logger.info("Creating ThoughtSpot Table")
        self._create_ts_table()

        logger.info("Creating ThoughtSpot Worksheet")
        self._create_ts_worksheet()
