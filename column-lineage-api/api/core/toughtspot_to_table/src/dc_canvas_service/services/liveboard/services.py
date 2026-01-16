from pathlib import Path
from typing import TYPE_CHECKING

from dc_canvas_service.services.liveboard.exceptions import LiveboardImportException
from dc_canvas_service.services.liveboard.utils import add_custom_actions_in_tml
from dc_canvas_service.services.snowflake import (
    LiveboardRowInsert,
    LiveboardRowUpdate,
    LiveboardType,
    create_liveboard,
    delete_liveboard_guid,
    get_liveboard_nextval,
    update_liveboard,
)

if TYPE_CHECKING:
    from thoughtspot_tml import Liveboard

    from dc_canvas_service.services.snowflake import SnowflakeService
    from dc_canvas_service.services.thoughtspot import (
        ThoughtSpotService,
        TSShareModeType,
    )

import logging

logger = logging.getLogger(__name__)


class LiveboardService:
    def __init__(
        self,
        ts: "ThoughtSpotService",
        sf: "SnowflakeService",
        ts_liveboard: "Liveboard",
        canvas_id: int,
        engagement_id: int,
        liveboard_name: str,
        parent_liveboard_id: int | None = None,
        liveboard_id: int | None = None,
    ):
        self.ts = ts
        self.sf = sf

        self.ts_liveboard = ts_liveboard
        self.canvas_id = canvas_id
        self.engagement_id = engagement_id
        self.liveboard_name = liveboard_name
        self.parent_liveboard_id = parent_liveboard_id
        self.liveboard_id = liveboard_id

        self.users = self._get_users()

    def _get_users(self) -> dict:
        """
        Retrieves the users associated with a specific liveboard.

        Returns:
            A dict of users associated with the liveboard GUID.
            {"CISCO_CCO_ID": {"user_GUID": GUID, "cisco_cco_id": str, "permission": TSShareModeType}}
        """
        users_response = self.ts.search_metadata_users(
            metadata_GUIDs=[self.ts_liveboard.guid], metadata_type="LIVEBOARD"
        )

        return users_response.get(self.ts_liveboard.guid)

    def delete_liveboard(self, user_cisco_cco_id: str) -> None:
        """
        Deletes a liveboard and its associated metadata.
        Args:
           user_cisco_cco_id (str): The Cisco CCO ID of the user requesting the deletion, used for audit purposes.

        Operations:
           Deletes the TML metadata for the liveboard using its GUID.
           Updates the session to delete the liveboard entry from the database.
        """

        self.ts.delete_tmls(
            metadata_GUIDs=[self.ts_liveboard.guid], tml_type="liveboard"
        )

        with self.sf.conn_transaction() as conn:
            delete_liveboard_guid(
                conn=conn,
                guid=self.ts_liveboard.guid,
                updated_by=user_cisco_cco_id,
            )

    def update_display_name(self, new_name: str) -> None:
        """
        Update the Display Name of the TS Liveboard
        Args:
            new_name (str): New Name for the Liveboard
        """

        # Update the stored liveboard name
        self.liveboard_name = new_name

        liveboard = self.ts_liveboard
        liveboard.liveboard.name = new_name
        self._push_tml_and_refresh(liveboard)

    def _push_tml_and_refresh(self, liveboard: "Liveboard") -> None:
        """
        Push TML and refresh self.ts_liveboard
        :param liveboard: Liveboard object
        :return:
        """
        from dc_canvas_service.services.canvas.utils import formulate_ts_liveboard_name

        # Apply E_ and C_ tags for ThoughtSpot
        liveboard.liveboard.name = formulate_ts_liveboard_name(
            liveboard_name=self.liveboard_name,
            engagement_id=self.engagement_id,
            canvas_id=self.canvas_id,
        )

        ts_response = self.ts.push_tmls(tml_objs=[liveboard])
        if not (response := ts_response.get("valid_responses")):
            logger.exception(ts_response)
            raise LiveboardImportException()

        self.ts_liveboard = response[0]

    def share_liveboard(self, users: list[str], share_mode: "TSShareModeType") -> dict:
        """
        Shares Liveboard with the list of User and Share Mode
        Args:
            users (list[str]): List of CISCO_CCO_IDs to which Liveboards needs to be shared
            share_mode (TSShareModeType): Access allowed to the users

        Returns:
            dict: Unassigned Users to the Liveboard as the User has not logged into TS
        """

        share_response = self.ts.share_metadata(
            metadata_GUIDs=[self.ts_liveboard.guid],
            metadata_type="LIVEBOARD",
            users=users,
            share_mode=share_mode,
        )

        self.users = self._get_users()

        return share_response

    def push_tml_to_s3_and_db(self, user_cisco_cco_id: str) -> None:
        """
        Pushes the TML Object and Metadata to S3 and Snowflake
        Args:
            user_cisco_cco_id: CCOID of the User performing action
        """

        create_new_db_record = False
        if self.liveboard_id is None:
            create_new_db_record = True

        with self.sf.conn_transaction() as conn:
            if create_new_db_record:
                self.liveboard_id = get_liveboard_nextval(conn)
                if not self.parent_liveboard_id:
                    self.parent_liveboard_id = self.liveboard_id

            s3_location = self.ts.push_liveboard_to_s3(
                liveboard=self.ts_liveboard,
                canvas_id=self.canvas_id,
                liveboard_id=self.liveboard_id,
            )

            if create_new_db_record:
                row = LiveboardRowInsert(
                    parent_liveboard_id=self.parent_liveboard_id,
                    liveboard_id=self.liveboard_id,
                    display_name=self.liveboard_name,
                    liveboard_type=LiveboardType.CANVAS,
                    location=s3_location,
                    created_by=user_cisco_cco_id,
                    liveboard_type_value="",
                    liveboard_name=Path(s3_location).name,
                    guid=self.ts_liveboard.guid,
                    canvas_id=self.canvas_id,
                )

                create_liveboard(conn, row)

            else:
                row = LiveboardRowUpdate(
                    liveboard_id=self.liveboard_id,
                    display_name=self.liveboard_name,
                    updated_by=user_cisco_cco_id,
                )

                update_liveboard(conn, row)

    def add_custom_actions(self) -> None:
        """
        Add custom actions to the liveboard.
        :return:
        """

        self.ts_liveboard, updated_guids = add_custom_actions_in_tml(self.ts_liveboard)

        if updated_guids and self.ts_liveboard.guid:
            self._push_tml_and_refresh(self.ts_liveboard)
