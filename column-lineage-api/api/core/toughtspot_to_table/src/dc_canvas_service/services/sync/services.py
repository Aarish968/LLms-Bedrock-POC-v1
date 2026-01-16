import logging
import re
from collections import Counter
from pathlib import Path
from typing import TYPE_CHECKING, Callable, Literal, Union, get_args

from pydantic import BaseModel
from requests.exceptions import HTTPError
from thoughtspot_tml.types import GUID
from tqdm import tqdm

from dc_canvas_service.common import Env, Settings
from dc_canvas_service.services.canvas import CanvasService
from dc_canvas_service.services.s3 import S3Service
from dc_canvas_service.services.snowflake import (
    LiveboardRowInsert,
    LiveboardType,
    SnowflakeService,
    create_liveboard,
    get_active_liveboards,
    get_liveboard_nextval,
    get_thoughtspot_data_tables,
    get_ts_objects,
)
from dc_canvas_service.services.thoughtspot import (
    ThoughtSpotService,
    TSDeleteMetadataError,
)

from ..snowflake.actions import get_active_canvases
from .exceptions import SyncServiceMissingDataException, SyncServiceNotSupported

if TYPE_CHECKING:
    from sqlalchemy import Connection, Engine

logger = logging.getLogger(__name__)

TS_Metadata_Types = Literal[
    "LIVEBOARD", "WORKSHEET", "LOGICAL_TABLE", "TABLE", "CONNECTION"
]  # the order of deletion
TS_Metadata_Regexp_map = {
    "CONNECTION": "CANVAS-(\\d+)_CONNECTION",
    "TABLE": "CANVAS_(\\d+)_THOUGHT_SPOT_V",
    "WORKSHEET": "ws_canvas-(\\d+)",
    "LIVEBOARD": "E_\\d+ - C_(\\d+)$",
}


class TS_Canvas_Object(BaseModel):
    guid: GUID
    type: TS_Metadata_Types
    name: str
    canvas_id: int | None


class SyncService:
    """
    Sync Service is the service class for synchronisation between Snowflake, S3 and Thoughtspot.
    """

    db = s3 = ts = requested_user = None

    def __init__(
        self,
        env: Union[Env, str],
        requested_user: str,
        get_engine: Callable[[], "Engine"] | None = None,
    ):
        """
        Init.
        :param env: ENV name
        :param requested_user: Username who requested sync
        :param get_engine callable
        """

        self.env = Env(env)
        self.requested_user = requested_user
        self.settings = Settings(env)
        self.db = SnowflakeService(settings=self.settings, get_engine=get_engine)
        self.s3 = S3Service(aws_session=self.settings.aws_session)
        self.ts = ThoughtSpotService(self.settings, self.s3)

    def process_orphaned_ts_objects_by_type(self, dry_run: bool = False) -> set[int]:
        """
        Search in TS for orphaned objects (deleted in DataCanvas, but not TS).
        Deletes TMLs by type (in order of TS_Metadata_Types).
        This method does not include all objects (e.g., if canvas_id is not included in a liveboard name).
        :param dry_run: Enable dry run mode.
        :return: A list of orphaned canvas ids
        """

        ts_objects, orphaned_canvas_ids = self._get_orphaned_objects()

        if not orphaned_canvas_ids:
            return set()

        orphaned_ts_objects = [
            o for o in ts_objects if o.canvas_id in orphaned_canvas_ids
        ]

        for object_type in get_args(TS_Metadata_Types):
            guids = [o.guid for o in orphaned_ts_objects if o.type == object_type]
            names = [o.name for o in orphaned_ts_objects if o.type == object_type]
            logger.info(f"{len(guids)} {object_type}S found.")
            logger.debug(names)

            if dry_run:
                continue

            with tqdm(total=len(guids)) as pbar:
                for guid in guids:
                    try:
                        self.ts.delete_tmls(
                            guid, tml_type=object_type.lower(), check_tml=False
                        )
                        pbar.update()
                    except TSDeleteMetadataError:
                        logger.warning(f"Unable to delete TMLs: {guid}")
                        pbar.update()
                        continue

        return orphaned_canvas_ids

    def process_orphaned_ts_objects_by_canvas_id(
        self, dry_run: bool = False
    ) -> set[int]:
        """
        Search in TS for orphaned objects (deleted in DataCanvas, but not TS).
        Deletes TMLs by getting all dependencies for a given TS table, separately for each orphaned canvas.
        This method is a bit slower, but more effective than searching by object type, because it captures
        all canvas dependencies, even if canvas_id is not part of an object name.
        :param dry_run: Enable dry run mode.
        :return: A list of orphaned canvas ids
        """

        _, orphaned_canvas_ids = self._get_orphaned_objects()

        if not orphaned_canvas_ids:
            return set()

        if dry_run:
            logger.info(orphaned_canvas_ids)
            return orphaned_canvas_ids

        with tqdm(total=len(orphaned_canvas_ids)) as pbar:
            for canvas_id in sorted(orphaned_canvas_ids):
                self._handle_orphaned_canvas_id(canvas_id, pbar)
                pbar.update()

        return orphaned_canvas_ids

    def _get_orphaned_objects(self) -> (list[TS_Canvas_Object], list[int]):
        """
        Gets all TML objects from TS and a list of orphaned canvases.
        :return: ts_objects (a list of all TML objects in TS) and orphaned_canvas_ids (a list of orphaned canvases)
        """
        ts_objects = self.get_ts_objects()
        counter = Counter([t.type for t in ts_objects])
        logger.info(f"{len(ts_objects)} TS objects found")
        logger.info(dict(counter))
        ts_canvas_ids = {o.canvas_id for o in ts_objects} - {None}

        with self.db.conn_transaction() as conn:
            active_canvases = get_active_canvases(conn)
            logger.info(f"{len(active_canvases)} active canvases found")

        orphaned_canvas_ids = ts_canvas_ids - active_canvases

        logger.info(
            f"{len(orphaned_canvas_ids)} / {len(ts_canvas_ids)} orphaned canvases found in TS"
        )

        return ts_objects, orphaned_canvas_ids

    def _handle_orphaned_canvas_id(self, canvas_id: int, pbar: tqdm) -> None:
        """
        Search in TS for dependent objects by LOGICAL TABLE and delete them. Then, delete the canvas connection.
        :param canvas_id: Canvas ID
        :param pbar: tqdm progress bar
        :return:
        """
        pbar.set_description(f"{f'{canvas_id} - searching':.<40}")
        ts_dep_objects = self.ts.search_table_dependent_objects(
            CanvasService.get_ts_table_name(canvas_id)
        )
        guid = None
        for object_type in get_args(TS_Metadata_Types):
            pbar.set_description(f"{f'{canvas_id} - deleting {object_type}(s)':.<40}")
            try:
                if object_type == "CONNECTION" and (
                    guid := self.ts.get_connection_guid(
                        f"CANVAS-{canvas_id}_CONNECTION"
                    )
                ):
                    self.ts.ts_session.connection_delete(guid)
                else:
                    guids = [
                        guid
                        for guid, ts_type in ts_dep_objects.items()
                        if ts_type == object_type
                    ]
                    for guid in guids:
                        self.ts.delete_tmls(
                            [guid], tml_type=object_type.lower(), check_tml=False
                        )
            except HTTPError:
                logger.warning(f"Unable to delete {object_type} TML: {guid}")
            except TSDeleteMetadataError:
                if object_type != "LIVEBOARD":
                    break

    @staticmethod
    def extract_canvas_id_from_name(
        name: str, obj_type: TS_Metadata_Types
    ) -> int | None:
        """
        Extracts Canvas ID from Thoughtspot name using appropriate regexp for a given object type.
        :return: Canvas ID or None
        """
        regexp = TS_Metadata_Regexp_map.get(obj_type)
        if matched := re.findall(regexp, name):
            return int(matched[0])
        return None

    @staticmethod
    def extract_type_from_ts_object(obj: dict) -> TS_Metadata_Types:
        """
        Extract Object Type from TS Metadata Object.
        :param obj: metadata dict
        :return: TS Metadata Type
        """
        if obj.get("metadata_header", {}).get("type") == "WORKSHEET":
            return "WORKSHEET"
        elif obj.get("metadata_type") == "LOGICAL_TABLE":
            return "TABLE"
        return obj.get("metadata_type")

    def get_ts_objects(self) -> list[TS_Canvas_Object]:
        """
        Search TS for all CONNECTION/LOGICAL_TABLE objects. Return a list of TS_Canvas_Objects
        :return: List of TS_Canvas_Objects
        """
        ts_objects = self.ts.ts_session.metadata_search(
            {
                "record_size": -1,
                "include_auto_created_objects": True,
                "include_hidden_objects": True,
                "include_incomplete_objects": True,
                "metadata": [
                    {"type": "CONNECTION"},
                    {"type": "LOGICAL_TABLE"},
                    {"type": "LIVEBOARD"},
                ],
            }
        )

        ts_canvas_objects = []
        for obj in ts_objects:
            obj_name = obj.get("metadata_name")
            obj_type = self.extract_type_from_ts_object(obj)
            ts_canvas_object = TS_Canvas_Object(
                guid=obj.get("metadata_id"),
                type=obj_type,
                name=obj_name,
                canvas_id=self.extract_canvas_id_from_name(obj_name, obj_type),
            )
            ts_canvas_objects.append(ts_canvas_object)
        return ts_canvas_objects

    def sync(self, canvas_id: int, dry_run: bool = False) -> bool:
        """
        Sync canvas data between DB, S3 and Thoughtspot
        :param canvas_id: Canvas ID
        :param dry_run: Set to true if you don't want to apply any changes, just report the status.
        :return Sync status
        """

        errors = []

        with self.db.conn_transaction() as conn:
            # Check Thoughtspot data table and view
            db_data_table_view = get_thoughtspot_data_tables(
                conn=conn, schema=self.settings.sf_schema.value, canvas_id=canvas_id
            )
            if len(db_data_table_view) != 2:
                errors.append(
                    "Thoughtspot data table or view are missing in Snowflake."
                )

            # Get Liveboards from Snowflake and check S3
            db_liveboards = get_active_liveboards(conn, canvas_id)

            # Get and check s3 files referenced in db
            db_locations = {r.location for r in db_liveboards}
            s3_locations = {
                loc: self.s3.exists(**self.s3.parse_uri(loc)._asdict())
                for loc in db_locations
            }
            s3_missing_files = {
                item for item, exists in s3_locations.items() if not exists
            }
            if s3_missing_files:
                errors.append(
                    f"The following files are missing in S3: {s3_missing_files}"
                )

            # get table and worksheet data from db
            db_table_worksheet = get_ts_objects(
                conn, canvas_id, object_names=["table", "worksheet"]
            )

            # combine db objects with their guid and type
            db_objects = {
                r.guid: {"type": r.type, "location": r.location} for r in db_liveboards
            }

            db_objects.update(
                {row.guid: {"type": row.object_name} for row in db_table_worksheet}
            )

            # get objects from Thoughtspot
            ts_objects = self.ts.search_table_dependent_objects(
                CanvasService.get_ts_table_name(canvas_id)
            )

            # check if objects are missing in db or ts
            missing_in_ts = {
                k: v
                for k, v in db_objects.items()
                if k in db_objects and k not in ts_objects
            }
            missing_in_db = {
                k: v
                for k, v in ts_objects.items()
                if k not in db_objects and k in ts_objects
            }

            if missing_in_ts:
                logger.info(f"Data missing in Thoughtspot: {missing_in_ts!s}")
            if missing_in_db:
                logger.info(f"Data missing in Snowflake: {missing_in_db!s}")

            if errors:
                logger.info(f"{errors=}")
                if not dry_run:
                    raise SyncServiceMissingDataException(f"{errors=}")  # noqa: EM102

            if not missing_in_ts and not missing_in_db:
                logger.info("The canvas is in sync with Thoughtspot!")
                return True

            if dry_run:
                return False

            logger.info(
                "The canvas is out of sync with Thoughtspot! Attempting to sync..."
            )

            self._sync_from_s3_to_ts(missing_in_ts)
            self._sync_from_ts_to_s3_and_db(conn, canvas_id, missing_in_db)

            logger.info("Sync has completed.")
            return True

    def _sync_from_s3_to_ts(self, dict_items: dict) -> None:
        """
        Sync data from S3 to Thoughtspot.
        :param dict_items: Dict items to sync
        :return:
        """

        objects_to_push = []

        for guid, item in dict_items.items():
            if (item_type := item.get("type", "").lower()) != "liveboard":
                msg = f"{item_type=} is not supported for sync."
                raise SyncServiceNotSupported(msg)
            if (location := item.get("location")) is None:
                msg = f"S3 location is missing for {guid=}"
                raise SyncServiceMissingDataException(msg)

            tml = self.ts.create_liveboard_from_s3(s3_location=location)

            tml.guid = guid
            objects_to_push.append(tml)

        if objects_to_push:
            self.ts.push_tmls(tml_objs=objects_to_push)

    def _sync_from_ts_to_s3_and_db(
        self, conn: "Connection", canvas_id: int, dict_items: dict
    ) -> None:
        """
        Sync data from Thoughtspot to S3 and Snowflake..
        :param conn: DB connection
        :param dict_items: Dict items to sync
        :return:
        """

        for guid, item_type in dict_items.items():  # noqa: B007
            if (item_type := item_type.lower()) != "liveboard":
                msg = f"{item_type=} is not supported for sync."
                raise SyncServiceNotSupported(msg)

        guids = list(dict_items.keys())
        logger.info(f"Exporting {guids=} from ThoughtSpot...")
        tmls = self.ts.get_tmls(metadata_GUIDs=guids)

        for guid, tml in tmls.items():
            liveboard_id = get_liveboard_nextval(conn)

            s3_location = self.ts.push_liveboard_to_s3(
                liveboard=tml, canvas_id=canvas_id, liveboard_id=liveboard_id
            )

            row = LiveboardRowInsert(
                parent_liveboard_id=liveboard_id,
                liveboard_id=liveboard_id,
                display_name=tml.liveboard.name,
                liveboard_type=LiveboardType.CANVAS,
                location=s3_location,
                created_by=self.requested_user,
                liveboard_type_value="",
                liveboard_name=Path(s3_location).name,
                guid=guid,
                canvas_id=canvas_id,
            )

            create_liveboard(conn, row)
