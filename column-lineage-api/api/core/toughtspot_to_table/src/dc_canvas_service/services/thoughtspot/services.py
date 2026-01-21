import json
import re
from json import JSONDecodeError
from typing import TYPE_CHECKING, Any, List, Optional, Tuple

import requests
import yaml
from thoughtspot_rest_api_v1 import TSRestApiV2, TSTypes
from thoughtspot_tml import Liveboard, Table, Worksheet, TML

# Mock types since thoughtspot_tml.types is not available in current version
GUID = str
TMLObject = Any
TMLType = str

# Mock proto classes since _scriptability is not available in current thoughtspot_tml version
class MockProtoBase:
    """Mock base class for proto objects"""
    def __init__(self):
        pass
    
    def from_dict(self, data):
        """Mock from_dict method"""
        for key, value in data.items():
            setattr(self, key, value)

class MockLogicalTableEDocProto(MockProtoBase):
    """Mock LogicalTableEDocProto"""
    pass

class MockWorksheetEDocProto(MockProtoBase):
    """Mock WorksheetEDocProto"""
    def __init__(self):
        super().__init__()
        self.name = ""
        self.description = ""
        self.tables = []
        self.table_paths = []

class MockLogicalTableEDocProtoLogicalColumnEDocProto(MockProtoBase):
    """Mock LogicalTableEDocProtoLogicalColumnEDocProto"""
    pass

class MockWorksheetEDocProtoWorksheetColumn(MockProtoBase):
    """Mock WorksheetEDocProtoWorksheetColumn"""
    pass

# Mock ts_protos module
class MockTSProtos:
    LogicalTableEDocProto = MockLogicalTableEDocProto
    WorksheetEDocProto = MockWorksheetEDocProto
    LogicalTableEDocProtoLogicalColumnEDocProto = MockLogicalTableEDocProtoLogicalColumnEDocProto
    WorksheetEDocProtoWorksheetColumn = MockWorksheetEDocProtoWorksheetColumn

ts_protos = MockTSProtos()

from dc_canvas_service.common import Env, SFWarehouse
from dc_canvas_service.common.utils import class_constants_to_dict

from .exceptions import (
    TSCreateConnectionError,
    TSCredsNotFoundError,
    TSDeleteMetadataError,
    TSExportMetadataError,
    TSImportMetadataError,
    TSSearchMetadataError,
    TSShareMetadataError,
    TSTokenFetchError,
    TSUserMetadataSearchError,
)
from .formula_template import FormulaTemplateService
from .models import TSIdentity
from .utils import (
    format_data_mappings_data,
    format_import_response,
    format_metadata_tml_response,
    format_metadata_user_search_response,
    formulate_column_types,
    generate_search_metadata_user_payload,
    generate_tml_export_payload,
    generate_tml_import_payload,
    generate_tml_search_payload,
    generate_user_permissions_payload,
    tml_export_type_map,
)

if TYPE_CHECKING:
    from models import (
        TSExportMetadataType,
        TSShareMetadataType,
        TSShareModeType,
        TSTable,
        TSUserMetadataType,
        TSWorksheet,
    )

    from dc_canvas_service.common import Settings
    from dc_canvas_service.services.s3 import S3Service


class ThoughtSpotService:
    """
    ThoughtSpotService is the service class for ThoughtSpot API operations.

    Note:
        All ThoughtSpot objects and resources are assigned a Globally Unique Identifier (GUID)
        by default. Most endpoints require you to specify the GUID to access, query, or
        modify a specific object. You can query the metadata list to get a list of objects of a
        specific type and the GUIDs assigned to each of these objects.
    """

    def __init__(self, settings: "Settings", s3: "S3Service"):
        self.settings = settings
        self.s3 = s3

        self.ts_server_url = self._get_ts_server_url()
        self.ts_table_base_link = f"{self.ts_server_url}/v1/#/data/tables"
        self._ts_credentials = settings.get_ts_credentials()
        self.ts_session = self._create_ts_session()

    def __del__(self):
        """
        Revokes the authentication token issued for current TS Session.
        """
        # self.ts_session.auth_token_revoke()

    def _get_ts_server_url(self) -> str:
        """
        Retrieve the ThoughtSpot server URL based on the current environment.
        Returns:
            str: The server URL corresponding to the current environment. Returns the
             production URL if the environment is 'prod', otherwise returns the
             development URL.
        """

        if self.settings.env == Env.prod:
            return "https://cisco.thoughtspot.cloud"

        return "https://cisco-dev.thoughtspot.cloud"

    def _create_ts_session(self) -> TSRestApiV2:
        """
        Create and configure a ThoughtSpot session with authentication.
        Returns:
            TSRestApiV2: An authenticated ThoughtSpot REST API session.

        Raises:
            ThoughtSpotCredsNotFoundException: If ThoughtSpot credentials are not found.
            ThoughtSpotTokenFetchException: If there is an HTTP error during token retrieval.
        """

        ts_session = TSRestApiV2(server_url=self.ts_server_url)

        if not self._ts_credentials:
            raise TSCredsNotFoundError(
                "ThoughtSpot Credentials not found in AWS Secret Manager"
            )

        try:
            auth_token_response = ts_session.auth_token_full(
                username=self._ts_credentials.get("username"),
                password=self._ts_credentials.get("password"),
                validity_time_in_sec=self.settings.ts_session_timeout,
            )

            ts_session.bearer_token = auth_token_response["token"]
        except requests.exceptions.HTTPError as e:
            raise TSTokenFetchError() from e

        return ts_session

    def create_connection(self, connection_name: str, table_name: str) -> "TSIdentity":
        """
        Create a new connection in the ThoughtSpot System.
        Args:
            connection_name (str): The name of the connection to be created.
            table_name (str): The name of the table to be associated with the connection.

        Returns:
            GUID: The GUID of the newly created connection.

        Raises:
            TSCreateConnectionError: If there is an HTTP error during the connection creation process.
        """

        sf_secret = self.settings.get_aws_secret("prd_cps_dsci_etl_svc_cloud_conn_str")

        user = sf_secret.get("cam-eks-snowflake-user")
        password = sf_secret.get("cam-eks-snowflake-secret")

        connection_request = {
            "name": connection_name,
            "data_warehouse_type": "SNOWFLAKE",
            "data_warehouse_config": {
                "configuration": {
                    "accountName": self.settings.sf_account_name,
                    "user": user,
                    "password": password,
                    "role": self.settings.sf_role,
                    "warehouse": SFWarehouse.small,
                    "database": self.settings.sf_db,
                    "schema": self.settings.sf_schema,
                    "table": table_name,
                },
                "externalDatabases": [],
            },
            "validate": False,
        }

        try:
            connection_response = self.ts_session.connection_create(
                request=connection_request
            )
            connection_GUID = connection_response["id"]
        except requests.exceptions.HTTPError as e:
            raise TSCreateConnectionError() from e

        new_connection = TSIdentity(name=connection_name, fqn=connection_GUID)

        return new_connection

    def search_table_dependent_objects(self, name: str) -> dict:
        """
        Searches for TMLs and all dependent objects using LOGICAL TABLE table_name
        Args:
            name (str): LOGICAL TABLE name

        Returns:
            dict: A dictionary mapping each GUID to its corresponding object type

        Raises:
            TSSearchMetadataError: If an HTTP error occurs during the metadata search.
        """

        metadata_request = generate_tml_search_payload(
            export_type="LOGICAL_TABLE", name_patterns=[name]
        )

        try:
            search_response = self.ts_session.metadata_search(
                request={
                    "metadata": metadata_request,
                    "include_dependent_objects": True,
                }
            )
        except requests.exceptions.HTTPError as e:
            raise TSSearchMetadataError() from e

        if not search_response:
            return {}

        search_response = search_response[0]
        items = {
            search_response.get("metadata_id"): search_response.get("metadata_type")
        }
        dependent_objects = search_response.get("dependent_objects", {}).get(
            search_response.get("metadata_id")
        )
        dependent_objects_dict = {
            object.get("id"): object.get("type", object_type)
            for object_type, objects in dependent_objects.items()
            for object in objects
        }
        items.update(dependent_objects_dict)

        ts_object_mapping = class_constants_to_dict(TSTypes)
        return {k: ts_object_mapping.get(v, v) for k, v in items.items()}

    def search_connections(
        self, name: str, limit: int = 1, include_details: bool = False
    ) -> [dict | None]:
        """
        Searches for connections.
        :param name: Connection name pattern. You can use % as a wildcard
        :param limit: max number of connections to return
        :param include_details: Should result include connection details.
        :return: connection dict metadata.
        """

        payload = {
            "record_size": limit,
            "include_details": include_details,
            "data_warehouse_types": ["SNOWFLAKE"],
            "connections": [{"name_pattern": name}],
        }

        return self.ts_session.connection_search(payload)

    def get_connection_guid(self, name: str) -> GUID | None:
        """
        Get connection GUID.
        :param name: Connection name
        :return: GUID of connection.
        """
        connections = self.search_connections(name)
        if connections:
            return connections[0].get("id")

    def search_tmls(
        self,
        metadata_GUIDs: list[GUID] = (),
        name_patterns: list[str] = (),
        export_type: "TSExportMetadataType" = None,
        **kwargs,
    ) -> dict:
        """
        Searches for TMLs using metadata GUIDs and export type.
        Args:
            metadata_GUIDs (list[GUID]): A list of GUIDs representing the metadata to search.
            name_patterns (list[str]): A list of string pattern to match
            export_type (TSExportMetadataType | None): An optional export type for the metadata search.
            kwargs: Additional keyword arguments to pass to the metadata search request.

        Returns:
            dict: A dictionary mapping each GUID to its corresponding Search Response object
                or None if the Search Response object is not found.

        Raises:
            TSSearchMetadataError: If an HTTP error occurs during the metadata search.
        """

        if not metadata_GUIDs and not name_patterns:
            raise TSSearchMetadataError(
                "At least provide metadata_GUIDs or name_patterns."
            )

        metadata_request = generate_tml_search_payload(
            tml_GUIDs=metadata_GUIDs,
            export_type=export_type,
            name_patterns=name_patterns,
        )

        try:
            search_response = self.ts_session.metadata_search(
                request={
                    "metadata": metadata_request,
                    "include_details": True,
                    **kwargs,
                }
            )
        except requests.exceptions.HTTPError as e:
            raise TSSearchMetadataError() from e

        search_response_map = {}
        for response_item in search_response:
            search_response_map.update({response_item["metadata_id"]: response_item})

            for guid in metadata_GUIDs:
                if guid not in search_response_map:
                    search_response_map.update({guid: None})

        return search_response_map

    def push_tmls(self, tml_objs: list[TMLObject], **kwargs) -> dict:
        """
        Push TMLs (ThoughtSpot Markup Language) data to ThoughtSpot.
        Args:
            tml_objs (list[TMLObject]): A list of TML Object to push.
            **kwargs: Additional keyword arguments to pass to the `metadata_tml_import` method.

        Returns:
            dict: A dictionary containing two response details:
            - "valid_responses": Spot App for the Valid TMl Object created.
            - "error_responses": List of error responses.

        Raises:
            TSImportMetadataError: If there is an HTTP error during the import process.
        """

        metadata_tmls = generate_tml_import_payload(tml_objs=tml_objs)

        try:
            import_response = self.ts_session.metadata_tml_import(
                metadata_tmls=metadata_tmls, **kwargs
            )
        except requests.exceptions.HTTPError as e:
            raise TSImportMetadataError() from e

        formatted_response = format_import_response(
            tml_objs=tml_objs, import_response=import_response
        )

        valid_responses = formatted_response.get("valid_responses")
        error_responses = formatted_response.get("error_responses")

        return {"valid_responses": valid_responses, "error_responses": error_responses}

    def get_tmls(
        self, metadata_GUIDs: list[GUID], tml_type: TMLType = None, **kwargs
    ) -> dict[GUID, TMLObject | None]:
        """
        Retrieve TMLs (ThoughtSpot Markup Language) for given metadata GUIDs.
        Args:
            metadata_GUIDs (list[GUID]): A list of GUIDs for which TMLs are to be exported.
            tml_type (TMLType): An optional tml type for the metadata export.
            **kwargs: Additional keyword arguments to pass to the `metadata_tml_export` method.

        Returns:
            dict[GUID, TMLObject | None]: A dictionary mapping each GUID to its corresponding TMLObject object
                or None if the TMLObject is not found.

        Raises:
            TSExportMetadataError: If there is an HTTP error during the export process.
        """

        export_type = None
        if tml_type:
            export_type = tml_export_type_map.get(tml_type)

        search_response = self.search_tmls(
            metadata_GUIDs=metadata_GUIDs, export_type=export_type
        )

        valid_GUIDs = []
        metadata_response = {}

        for guid in search_response:
            if search_response.get(guid) is None:
                metadata_response.update({guid: None})
            else:
                valid_GUIDs.append(guid)

        if valid_GUIDs:
            export_payload = generate_tml_export_payload(
                tml_GUIDs=valid_GUIDs, export_type=export_type
            )

            try:
                metadata_tmls = self.ts_session.metadata_tml_export(
                    metadata_ids=metadata_GUIDs,
                    metadata_request=export_payload,
                    **kwargs,
                )
            except requests.exceptions.HTTPError as e:
                raise TSExportMetadataError() from e

            tmls_data = TML.from_api(payload={"object": metadata_tmls})

            formatted_valid_response = format_metadata_tml_response(
                tml_objs=tmls_data.tml
            )
            metadata_response.update(formatted_valid_response)

        return metadata_response

    def delete_tmls(
        self,
        metadata_GUIDs: list[GUID],
        tml_type: TMLType = None,
        check_tml: bool = True,
        **kwargs,
    ) -> dict[str, list[GUID]]:
        """
        Deletes TML objects based on their GUIDs and optional TML type.
        Args:
            metadata_GUIDs (list[GUID]): A list of GUIDs for the TML objects to be deleted.
            tml_type (TMLType, optional): The type of TML to narrow down the delete type. Defaults to None.
            check_tml (bool): Before deleting, use metadata search to verify if GUIDs are valid
            **kwargs: Additional keyword arguments for the metadata deletion request.

        Returns:
            dict[str, list[GUID]]: A dictionary containing two lists:
            - "deleted_GUIDs": List of GUIDs that were successfully found and deleted.
            - "invalid_GUIDs": List of GUIDs that were not found and thus not deleted.

        Raises:
            TSDeleteMetadataError: If an HTTP error occurs during the deletion process.
        """

        export_type = None
        valid_GUIDs = invalid_GUIDs = []
        if tml_type:
            export_type = tml_export_type_map.get(tml_type)

        if not check_tml:
            valid_GUIDs = metadata_GUIDs
        else:
            search_response = self.search_tmls(
                metadata_GUIDs=metadata_GUIDs, export_type=export_type
            )

            for guid in search_response:
                if search_response.get(guid) is None:
                    invalid_GUIDs.append(guid)
                else:
                    valid_GUIDs.append(guid)

        if valid_GUIDs:
            delete_payload = generate_tml_export_payload(
                tml_GUIDs=valid_GUIDs, export_type=export_type
            )

            try:
                self.ts_session.metadata_delete(
                    metadata_ids=metadata_GUIDs,
                    metadata_request=delete_payload,
                    **kwargs,
                )
            except requests.exceptions.HTTPError as e:
                raise TSDeleteMetadataError(e) from e

        return {"deleted_GUIDs": valid_GUIDs, "invalid_GUIDs": invalid_GUIDs}

    @staticmethod
    def create_base_table(table_data: "TSTable") -> Table:
        """
        Create a base Table object from provided table data.
        Args:
            table_data (TSTable): The data used to construct the Table object.

        Returns:
            Table: A Table object initialized with the provided data.
        """

        table_guid = ""
        table_proto = ts_protos.LogicalTableEDocProto()
        table_proto.from_dict(table_data.model_dump(exclude_none=True, by_alias=True))

        table_obj = Table(guid=table_guid, table=table_proto)

        return table_obj

    @staticmethod
    def create_base_worksheet(worksheet_data: "TSWorksheet") -> Worksheet:
        """
        Create a base Worksheet object from provided Worksheet data.
        Args:
            worksheet_data (TSWorksheet): The data used to construct the Worksheet object.

        Returns:
            Worksheet: A Worksheet object initialized with the provided data.
        """

        worksheet_guid = ""
        worksheet_proto = ts_protos.WorksheetEDocProto()

        worksheet_proto.name = worksheet_data.name
        worksheet_proto.description = worksheet_data.description
        worksheet_proto.tables = [
            {"name": worksheet_data.tables[0].name, "fqn": worksheet_data.tables[0].fqn}
        ]
        worksheet_proto.table_paths = [
            {
                "id": f"{worksheet_data.tables[0].name}_1",
                "table": worksheet_data.tables[0].name,
            }
        ]

        worksheet_obj = Worksheet(guid=worksheet_guid, worksheet=worksheet_proto)

        return worksheet_obj

    @staticmethod
    def _create_table_column(column_name: str, data_type: str, mapping: dict = None):
        """
        Create a table column proto from provided column data.
        Args:
            column_name (str): Table Column Name
            data_type (str): Table Column Data Type
            mapping (dict | None): TS Column Type Mapping data

        Returns:
            LogicalTableEDocProtoLogicalColumnEDocProto: A column proto object initialized
                with the provided data.

        """
        column_types = formulate_column_types(data_type=data_type, mapping=mapping)

        db_data_type = column_types.get("db_data_type")
        properties = column_types.get("properties")

        column_data = {
            "name": column_name,
            "db_column_name": column_name,
            "db_column_properties": {"data_type": db_data_type},
            "properties": properties,
        }

        table_col_proto = ts_protos.LogicalTableEDocProtoLogicalColumnEDocProto()
        table_col_proto.from_dict(column_data)

        return table_col_proto

    @staticmethod
    def _create_worksheet_column(
        column_name: str,
        data_type: str,
        table_name: str,
        mapping: dict = None,
    ):
        """
        Create a worksheet column proto from provided column data.
        Args:
            column_name (str): Table Column Name
            data_type (str): Table Column Data Type
            table_name (str): Refernece Table Name
            mapping (dict | None): TS Column Type Mapping data

        Returns:
            WorksheetEDocProtoWorksheetColumn: A column proto object initialized
                with the provided data.

        """

        column_types = formulate_column_types(data_type=data_type, mapping=mapping)

        properties = column_types.get("properties")

        column_data = {
            "name": column_name,
            "column_id": f"{table_name}_1::{column_name}".upper(),
            "properties": properties,
        }
        worksheet_col_proto = ts_protos.WorksheetEDocProtoWorksheetColumn()
        worksheet_col_proto.from_dict(column_data)

        return worksheet_col_proto

    def add_columns(
        self,
        tml_obj: Table | Worksheet,
        columns: dict[str, str],
        replace_all: bool = False,
        table_name: str = None,
    ) -> Table | Worksheet:
        """
        Add columns to a Table or Worksheet object.
        Args:
            tml_obj (Table | Worksheet): The Table or Worksheet  object to which columns will be added.
            columns (list[TSColumn] | list["TSWorksheetColumn"]): A list of column data to be added to the
                Table or Worksheet.
            replace_all (bool): If True, replaces all existing columns with the new columns.
                If False, appends the new columns to the existing columns. Defaults to False.
            table_name (str): TS Table Name for the canvas

        Returns:
            Table | Worksheet: The updated Table or Worksheet object with the new columns added.
        """

        data_types_mappings_file = self.s3.download_file(
            bucket=self.settings.ts_data_types_bucket_name,
            key=self.settings.ts_data_types_file_path,
        )

        data_types_mappings = format_data_mappings_data(data_types_mappings_file)

        # Match the type of the TML object
        match tml_obj:
            case Table():
                # Create table column objects from the provided column definitions
                new_columns = [
                    self._create_table_column(
                        column_name=c,
                        data_type=d,
                        mapping=data_types_mappings.get(c.lower()),
                    )
                    for c, d in columns.items()
                ]
                if replace_all:
                    tml_obj.table.columns = new_columns
                else:
                    tml_obj.table.columns += new_columns

            case Worksheet():
                # Create worksheet column objects from the provided column definitions
                new_columns = [
                    self._create_worksheet_column(
                        column_name=c,
                        data_type=d,
                        mapping=data_types_mappings.get(c.lower()),
                        table_name=table_name,
                    )
                    for c, d in columns.items()
                ]
                if replace_all:
                    tml_obj.worksheet.worksheet_columns = new_columns
                else:
                    tml_obj.worksheet.worksheet_columns += new_columns

        return tml_obj

    def search_metadata_users(
        self,
        metadata_GUIDs: list[GUID],
        metadata_type: "TSUserMetadataType" = None,
        **kwargs,
    ):
        """
        Searches for users associated with specific metadata GUIDs and retrieves their permissions.
        Args:
            metadata_GUIDs (list[GUID]): A list of GUIDs representing the metadata to search.
            metadata_type (TSUserMetadataType): An optional metadata type to specify the kind of
                user metadata to search for.
            **kwargs: Additional keyword arguments to pass to the search request.

        Returns:
            The response from the user metadata search, containing permissions and user details.

        Raises:
            TSUserMetadataSearchError: If an HTTP error occurs during the search operation.
        """

        user_search_payload = generate_search_metadata_user_payload(
            tml_GUIDs=metadata_GUIDs, metadata_type=metadata_type
        )

        try:
            user_search_response = self.ts_session.security_metadata_fetch_permissions(
                request={"metadata": user_search_payload, **kwargs}
            )
        except requests.exceptions.HTTPError as e:
            raise TSUserMetadataSearchError() from e

        formatted_user_search_response = format_metadata_user_search_response(
            metadata_permission_details=user_search_response.get(
                "metadata_permission_details"
            )
        )

        return formatted_user_search_response

    def search_users(self, cisco_cco_ids: list[str]):
        """
        Searches for users based on Cisco CCO IDs and categorizes them as valid or invalid.
        Args:
            cisco_cco_ids (list[str]): A list of Cisco CCO IDs to search for.

        Returns:
            dict: A dictionary mapping each cisco_cco_id to its corresponding User Response object
                or None if the User Response object is not found.
        """

        user_search_response = {}
        for cisco_cco_id in cisco_cco_ids:
            try:
                user_response = self.ts_session.users_search(
                    request={"name_pattern": cisco_cco_id}
                )
                user_search_response.update({cisco_cco_id: user_response})
            except requests.exceptions.HTTPError:
                user_search_response.update({cisco_cco_id: None})

        return user_search_response

    def share_metadata(
        self,
        metadata_GUIDs: list[GUID],
        metadata_type: "TSShareMetadataType",
        users: list[str],
        share_mode: "TSShareModeType",
    ) -> dict:
        """
        Shares metadata with specified users by setting permissions.
        Args:
            metadata_GUIDs (list[GUID]): A list of GUIDs representing the metadata to be shared.
            metadata_type (TSShareMetadataType): The type of metadata to share.
            users (list[str]): A list of users with whom to share the metadata.
            share_mode (TSShareModeType): The mode of sharing, which determines the access level.

        Returns:
            dict: Unassigned Users to the Liveboard as the User has not logged into TS

        Raises:
            TSShareMetadataError: If an HTTP error occurs during the sharing operation.
        """

        users_details = self.search_users(cisco_cco_ids=users)

        assigned_users = []
        unassigned_users = []
        valid_guids = []

        for cisco_cco_id in users_details:
            if users_details.get(cisco_cco_id) is None:
                unassigned_users.append(cisco_cco_id)
            else:
                user_details = users_details.get(cisco_cco_id)
                if user_details:
                    user_GUID = user_details[0].get("id")
                    assigned_users.append(cisco_cco_id)
                    valid_guids.append(user_GUID)
                else:
                    unassigned_users.append(cisco_cco_id)

        if valid_guids:
            permissions_payload = generate_user_permissions_payload(
                user_GUIDs=valid_guids, share_mode=share_mode
            )

            metadata = []
            for metadata_GUID in metadata_GUIDs:
                m = {"identifier": metadata_GUID}
                if metadata_type:
                    m.update({"type": metadata_type})
                metadata.append(m)

            try:
                share_response = self.ts_session.security_metadata_share(
                    request={
                        "metadata_type": metadata_type,
                        "metadata_identifiers": metadata_GUIDs,
                        "metadata": metadata,
                        "permissions": permissions_payload,
                        "emails": assigned_users,
                        "message": f"Sharing {metadata_GUIDs=}",
                        "notify_on_share": False,
                    }
                )
            except requests.exceptions.HTTPError as e:
                raise TSShareMetadataError() from e

        return {"unassigned_users": unassigned_users}

    def create_liveboard_from_s3(self, s3_location: str) -> Liveboard:
        """
        Creates Liveboard Object using the file present in S3
        Args:
            s3_location: Location of the TML Object File in S3

        Returns:
            Liveboard: Liveboard Object created using the file present in S3
        """
        bucket, key = tuple(self.s3.parse_uri(s3_location))

        bytes_val = self.s3.download_file(bucket, key)
        tml_str = bytes_val.decode("utf-8")
        tml_str = re.sub(
            "(context: )!!.*ActionContextE\\n\\s+- 3", "\\1CONTEXT_MENU", tml_str
        )
        try:
            tml_file = json.loads(tml_str)
        except JSONDecodeError:
            tml_file = yaml.safe_load(tml_str)

        # in TML v2 PinnedVisualization must not include guid
        for visualization in tml_file.get("liveboard", {}).get("visualizations"):
            if "guid" in visualization:
                del visualization["guid"]

        tml_file["guid"] = ""
        liveboard = Liveboard(**tml_file)
        liveboard.guid = None

        return liveboard

    def push_liveboard_to_s3(
        self, liveboard: "Liveboard", canvas_id: int, liveboard_id: int
    ):
        """
        Uploads Liveboard TML Object to S3 Bucket
        Args:
            liveboard: Liveboard TML Object to upload
            canvas_id: Canvas ID
            liveboard_id: Snowflake Liveboard ID

        Returns:
            s3_location: S3 Location of TML Object uploaded
        """
        s3_location = self.s3.make_liveboard_uri(
            bucket=self.settings.s3_bucket,
            env=self.settings.env,
            dest_type="canvas",
            object_id=canvas_id,
            liveboard_id=liveboard_id,
        )

        bucket, key = tuple(self.s3.parse_uri(s3_location))

        tml_contents = liveboard.dumps().encode("utf-8")
        self.s3.upload_file(bucket=bucket, key=key, content=tml_contents)

        return s3_location

    def add_formulas_to_worksheet(self, worksheet: Worksheet) -> Worksheet:
        """
        Downloads formula template from S3 and adds formulas to the provided worksheet.

        This method creates the FormulaTemplateService only once and performs all formula-related
        operations in a single method call for better efficiency.

        Args:
            worksheet: The worksheet to add formula elements to

        Returns:
            Worksheet: The updated worksheet with formula elements appended and references updated

        Raises:
            ValueError: If formula references could not be properly updated or if template
                        references remain after updates
        """

        # Ensure the provided object is a Worksheet, not a Model
        if not isinstance(worksheet, Worksheet):
            return worksheet

        formula_service = FormulaTemplateService(
            s3_service=self.s3, settings=self.settings
        )
        formula_elements = formula_service.get_formula_columns()
        return formula_service.append_formula_elements_to_worksheet(
            worksheet, formula_elements
        )
