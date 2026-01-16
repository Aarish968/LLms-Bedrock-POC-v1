import logging
from pathlib import Path
from typing import TYPE_CHECKING, Callable, Union

from botocore.exceptions import ClientError
from retry import retry

from dc_canvas_service.common import Env
from dc_canvas_service.services.canvas import (
    ActionType,
    CanvasParseActions,
    CanvasService,
    CloneAction,
    CopyAction,
    CreateAction,
    DeleteAction,
    MoveAction,
    RenameAction,
)
from dc_canvas_service.services.snowflake import (
    LiveboardRowInsert,
    LiveboardRowUpdate,
    create_liveboard,
    delete_liveboard,
    get_liveboard_nextval,
    update_liveboard,
)

from ..liveboard import (
    add_custom_actions_in_tml,
    update_table_for_visualizations,
    update_worksheet_references_in_liveboard,
)
from .exceptions import (
    CanvasDeletedException,
    CreateActionFailure,
    InvalidAction,
)

if TYPE_CHECKING:
    from sqlalchemy import Engine

    from dc_canvas_service.services.canvas import SRCDestType

logger = logging.getLogger(__name__)


class ActionService:
    def __init__(
        self,
        canvas_id: int,
        engagement_id: int,
        user_cisco_cco_id: str,
        env: Union[Env, str] = Env.dev,
        get_engine: Callable[[], "Engine"] | None = None,
    ):
        """"""
        self.canvas_id = canvas_id
        self.engagement_id = engagement_id
        self.user_cisco_cco_id = user_cisco_cco_id
        self.env = env

        self.canvas_service = CanvasService(
            canvas_id=canvas_id,
            engagement_id=engagement_id,
            user_cisco_cco_id=user_cisco_cco_id,
            env=env,
            get_engine=get_engine,
        )

        logger.info(f"Action Service instantiated | {env=}")

    def get_pending_actions(
        self, request_id: int | None = None
    ) -> list["CanvasParseActions"] | None:
        """
        Get pending actions for a request_id
        :param request_id: request id
        :return: CanvasParseActions object
        """
        pending_actions = self.canvas_service.get_pending_actions(request_id)
        return pending_actions

    def mark_success(self, request_id: int) -> None:
        """
        Mark request_id as Success in DC_FILE_MANAGEMENT_RUNS.status
        Mark canvas_id as Success in DC_CANVAS_HDR.CANVAS_STATUS
        :param request_id:
        :return:
        """
        self.canvas_service.mark_success(request_id)

    def share_canvas(self, users: list[str]) -> set:
        """
        Shares Liveboards, Worksheet, Table with the list of User
        Args:
            users (list[str]): List of CISCO_CCO_IDs to which Liveboards needs to be shared

        Returns:
            set: Unassigned Users as the User has not logged into TS
        """
        logger.info(f"Sharing canvas with {users=}")

        ts_table_guid = (
            self.canvas_service.ts_table.guid if self.canvas_service.ts_table else None
        )
        ts_worksheet_guid = (
            self.canvas_service.ts_worksheet.guid
            if self.canvas_service.ts_worksheet
            else None
        )
        ts_liveboard_guids = self.canvas_service.get_liveboards_guids()

        unassigned_users = set()

        # Shares Table with Read Only Access
        if ts_table_guid:
            table_share_detail = self.canvas_service.ts.share_metadata(
                metadata_GUIDs=[ts_table_guid],
                metadata_type="LOGICAL_TABLE",
                users=users,
                share_mode="READ_ONLY",
            )
            unassigned_users.update(table_share_detail.get("unassigned_users"))
            logger.info(f"Table Shared GUID:{ts_table_guid}")

        # Shares Worksheet with Modify Access
        if ts_worksheet_guid:
            worksheet_share_detail = self.canvas_service.ts.share_metadata(
                metadata_GUIDs=[ts_worksheet_guid],
                metadata_type="LOGICAL_TABLE",
                users=users,
                share_mode="MODIFY",
            )
            unassigned_users.update(worksheet_share_detail.get("unassigned_users"))
            logger.info(f"Worksheet Shared GUID:{ts_worksheet_guid}")

        # Shares Liveboards with Modify Access
        if ts_liveboard_guids:
            liveboards_share_detail = self.canvas_service.ts.share_metadata(
                metadata_GUIDs=list(ts_liveboard_guids),
                metadata_type="LIVEBOARD",
                users=users,
                share_mode="MODIFY",
            )
            unassigned_users.update(liveboards_share_detail.get("unassigned_users"))
            logger.info(f"Liveboards Shared GUIDs:{ts_liveboard_guids}")

        if unassigned_users:
            logger.warning(
                f"Unable to share Canvas with the following users: {unassigned_users}"
            )

        return unassigned_users

    @retry(tries=3, backoff=5, jitter=(2, 10))
    def _handle_action(self, action: "ActionType") -> LiveboardRowInsert | None:
        """
        Handle action.
        :param action: Action
        :return:
        """
        logger.info(action)
        if self.canvas_service.is_deleted and action.__class__.__name__ not in [
            "DeleteAction"
        ]:
            msg = f"The action cannot be performed because the canvas has been deleted | {action=}"
            raise CanvasDeletedException(msg)

        match action.__class__.__name__:
            case "CreateAction":
                return self._handle_create_action(action)
            case "CloneAction":
                return self._handle_clone_action(action)
            case "CopyAction":
                return self._handle_copy_action(action)
            case "RenameAction":
                return self._handle_rename_action(action)
            case "DeleteAction":
                return self._handle_delete_action(action)
            case "MoveAction":
                return self._handle_move_action(action)
            case _:
                raise InvalidAction(action)

    def _get_liveboard_type_and_value(self, dest_type: "SRCDestType") -> dict:
        """
        Fetches the Liveboard Type and Value to be utilized for S3 & Snowflake inserts
        Args:
            dest_type (SRCDestType): The destination type of the Liveboard TML

        Returns:
            dict: Containing the liveboard_type & liveboard_type_value values.
        """

        match dest_type:
            case "custom_eng" | "engagement":
                liveboard_type = "engagement"
                liveboard_type_value = f"eng_{self.engagement_id}"
            case "custom_user":
                liveboard_type = "user"
                liveboard_type_value = self.user_cisco_cco_id
            case "delete":
                liveboard_type = "delete"
                liveboard_type_value = self.user_cisco_cco_id
            case _:
                liveboard_type = "canvas"
                liveboard_type_value = self.canvas_id

        return {
            "liveboard_type": liveboard_type,
            "liveboard_type_value": liveboard_type_value,
        }

    def _handle_create_action(self, action: "CreateAction") -> None:
        """
        Performs new liveboard creation in ThoughtSpot for the Canvas
        Args:
            action (CreateAction): The Action Model containing details for Creating Liveboard
        """
        _, liveboard_error = self.canvas_service.create_liveboard(action=action)
        if liveboard_error:
            raise CreateActionFailure(liveboard_error)

    def _handle_clone_action(self, action: "CloneAction") -> None:
        """
        Performs cloning of liveboard present in ThoughtSpot for the Canvas
        Args:
            action (CloneAction): The Action Model containing details for Cloning Liveboard
        """
        self.canvas_service.clone_liveboard(action=action)

    def _handle_copy_action(self, action: "CopyAction") -> "LiveboardRowInsert":
        """
        Perform copying of a Liveboard TML present in TS or S3 using the Canvas Details
        Args:
            action (CopyAction): The Action Model containing details for Copying Liveboard
        """
        dest_type = action.dest_type

        liveboard_row = action.liveboard_row
        s3_location = liveboard_row.location
        ts_guid = liveboard_row.guid
        liveboard = None

        if not self.canvas_service.ts_table or not self.canvas_service.ts_worksheet:
            self.canvas_service.overwrite_thoughtspot()

        # prioritize getting liveboard TML from TS rather than S3
        if ts_guid:
            logger.info(f"Creating Liveboard TML Object from TS guid={ts_guid}")
            liveboards_response = self.canvas_service.ts.get_tmls(
                metadata_GUIDs=[ts_guid],
                tml_type="liveboard",
            )
            liveboard = liveboards_response.get(ts_guid)

        if liveboard:
            liveboard.guid = ""
        else:
            logger.info(f"Creating Liveboard TML Object from {s3_location}")
            liveboard = self.canvas_service.ts.create_liveboard_from_s3(
                s3_location=s3_location
            )

        logger.info("Setting the liveboard name")
        liveboard.liveboard.name = action.liveboard_name

        table_detail = {
            "id": self.canvas_service.ts_worksheet.worksheet.name,
            "name": self.canvas_service.ts_worksheet.worksheet.name,
            "fqn": self.canvas_service.ts_worksheet.guid,
        }

        logger.info(
            f"Updating Worksheet details in Liveboard & Visualizations {table_detail}"
        )
        liveboard.liveboard.tables = [table_detail]
        liveboard = update_table_for_visualizations(
            liveboard=liveboard, table_detail=table_detail
        )

        # Update worksheet references in parameter_overrides and ordered_chips
        liveboard = update_worksheet_references_in_liveboard(
            liveboard=liveboard,
            new_worksheet_name=self.canvas_service.ts_worksheet_name,
        )

        liveboard, updated_guids = add_custom_actions_in_tml(liveboard)

        logger.info("Fetching the new Liveboard ID from Snowflake")
        with self.canvas_service.sf.conn_transaction() as conn:
            new_liveboard_id = get_liveboard_nextval(conn=conn)

        logger.info(
            f"Fetching the Liveboard Type & Value for liveboard_id: {new_liveboard_id} using destination type {dest_type}"
        )
        liveboard_type_details = self._get_liveboard_type_and_value(dest_type=dest_type)

        liveboard_type = liveboard_type_details.get("liveboard_type")
        liveboard_type_value = liveboard_type_details.get("liveboard_type_value")

        logger.info(
            f"Creating S3 URI for Liveboard Upload with {liveboard_type_details}"
        )
        new_s3_location = self.canvas_service.s3.make_liveboard_uri(
            bucket=self.canvas_service.settings.s3_bucket,
            env=self.env,
            dest_type=liveboard_type,
            object_id=liveboard_type_value,
            liveboard_id=new_liveboard_id,
        )

        logger.info(f"New Liveboard location: {new_s3_location}")
        bucket, key = tuple(self.canvas_service.s3.parse_uri(new_s3_location))

        tml_contents = liveboard.dumps().encode("utf-8")
        self.canvas_service.s3.upload_file(bucket=bucket, key=key, content=tml_contents)

        logger.info("Inserting the Liveboard details into Snowflake")
        new_liveboard_row = LiveboardRowInsert(
            parent_liveboard_id=action.liveboard_id,
            liveboard_id=new_liveboard_id,
            display_name=liveboard.liveboard.name,
            liveboard_type=liveboard_type,
            location=new_s3_location,
            created_by=self.user_cisco_cco_id,
            liveboard_type_value=liveboard_type_value,
            liveboard_name=Path(new_s3_location).name,
        )

        with self.canvas_service.sf.conn_transaction() as conn:
            create_liveboard(conn=conn, liveboard=new_liveboard_row)

        return new_liveboard_row

    def _handle_rename_action(self, action: "RenameAction") -> None:
        """
        Handle rename action
        :param action: RenameAction object
        :return: None
        """

        liveboard_guid = action.liveboard_row.guid
        liveboard_type = action.liveboard_row.liveboard_type

        if liveboard_guid and liveboard_type in ["canvas", "currently_in_ts"]:
            liveboard_services = self.canvas_service.get_liveboards(
                guid=action.liveboard_row.guid
            )
            ls = liveboard_services[0]
            ls.update_display_name(action.liveboard_name)
            ls.push_tml_to_s3_and_db(self.user_cisco_cco_id)

        else:
            row = LiveboardRowUpdate(
                liveboard_id=action.liveboard_id,
                display_name=action.liveboard_name,
                updated_by=self.user_cisco_cco_id,
            )

            with self.canvas_service.sf.conn_transaction() as conn:
                update_liveboard(conn, row)

    def _handle_delete_action(self, action: "DeleteAction") -> None:
        """
        Handle delete action
        :param action: DeleteAction object
        :return:
        """

        if (liveboard_row := action.liveboard_row) is None:
            logger.warning("Liveboard does not exist.")
            return

        guid = liveboard_row.guid

        self.canvas_service.delete_liveboards_via_guid(
            liveboard_GUID=guid, liveboard_row=liveboard_row
        )

    def _handle_move_action(self, action: "MoveAction") -> None:
        """
        Performs a complete move of a S3 TMl file to 'delete' folder
        Args:
            action (MoveAction): The Action Model containing details for Moving Liveboard
        """
        liveboard_row = action.liveboard_row
        s3_location = liveboard_row.location
        bucket, key = tuple(self.canvas_service.s3.parse_uri(s3_location))

        new_s3_location = self.canvas_service.s3.make_liveboard_uri(
            bucket=self.canvas_service.settings.s3_bucket,
            env=self.env,
            dest_type="delete",
            object_id=self.user_cisco_cco_id,
            liveboard_id=liveboard_row.liveboard_id,
        )
        _, new_key = tuple(self.canvas_service.s3.parse_uri(new_s3_location))
        try:
            self.canvas_service.s3.move_file(bucket, key, new_key)
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") == "404":
                logger.warning(e)
            else:
                raise

        with self.canvas_service.sf.conn_transaction() as conn:
            delete_liveboard(
                conn=conn,
                liveboard_id=action.liveboard_id,
                location=new_s3_location,
                updated_by=self.user_cisco_cco_id,
            )

    def handle_request(self, request_id: int) -> None:
        """
        Fetch pending actions for a given request_id and handle them.
        :param request_id: Request ID
        :return:
        """
        actions = self.get_pending_actions(request_id)
        if not actions:
            logger.info("No pending actions found.")
        else:
            parsed_actions = actions[0].parsed_actions
            for action_type in parsed_actions.model_fields_set:
                actions = getattr(parsed_actions, action_type)
                list(map(self._handle_action, actions))

        self.mark_success(request_id)
