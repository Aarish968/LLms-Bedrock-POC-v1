import ast
import json
from typing import Any

from thoughtspot_tml import Answer, Liveboard, SQLView, Table, View, Worksheet

# Mock types since thoughtspot_tml.types is not available in current version
GUID = str
TMLObject = Any
TMLObjectType = str
TMLType = str

from .models import (
    TSExportMetadataType,
    TSExportPayload,
    TSMetadataUserSearchPayload,
    TSSearchPayload,
    TSShareModeType,
    TSUserMetadataType,
    TSUserSearchPayload,
)

tml_type_class_map: dict[TMLType, TMLObjectType] = {
    "table": Table,
    "view": View,
    "sqlview": SQLView,
    "worksheet": Worksheet,
    "answer": Answer,
    "liveboard": Liveboard,
    "pinboard": Liveboard,
}

tml_export_type_map: dict[TMLType, TSExportMetadataType] = {
    "liveboard": "LIVEBOARD",
    "pinboard": "LIVEBOARD",
    "answer": "ANSWER",
    "table": "LOGICAL_TABLE",
    "view": "LOGICAL_TABLE",
    "worksheet": "LOGICAL_TABLE",
    "sqlview": "LOGICAL_TABLE",
}

sf_data_type_map: dict[str, str] = {
    "NUMBER": "INT64",
    "INT": "INT64",
    "FLOAT": "DOUBLE",
    "DOUBLE": "DOUBLE",
    "STRING": "VARCHAR",
    "TEXT": "VARCHAR",
    "VARCHAR": "VARCHAR",
    "CHAR": "VARCHAR",
    "BOOLEAN": "BOOLEAN",
    "DATE": "DATE",
    "DATETIME": "DATE_TIME",
    "TIMESTAMP": "TIMESTAMP",
    "TIMESTAMP_NTZ": "DATE_TIME",
}


def is_valid_tml_type(tml_class: TMLObjectType, tml_type: TMLType) -> bool:
    """
    Validate if the given TML class corresponds to the expected TML type.
    Args:
        tml_class (TMLObjectType): The TML class object to validate.
        tml_type (TMLType): The expected TML type to validate against.

    Returns:
        bool: True if the type of `tml_class` matches the expected class type for
          `tml_type`, otherwise False.
    """

    return type(tml_class) is tml_type_class_map.get(tml_type)


def generate_tml_import_payload(tml_objs: list[TMLObject]) -> list[str]:
    """
    Generate a payload for pushing TML objects to TS.
    Args:
        tml_objs (list[TMLObjectType]): A list of TML Objects representing the TML objects to be exported.

    Returns:
        list[str]: A list of TML str, each representing a TML Object
    """

    metadata_payload: list[str] = []

    for tml_obj in tml_objs:
        metadata_payload.append(tml_obj.dumps())

    return metadata_payload


def generate_tml_export_payload(
    tml_GUIDs: list[GUID], export_type: TSExportMetadataType = None
) -> list[TSExportPayload]:
    """
    Generate a payload for exporting TML objects.
    Args:
        tml_GUIDs (list[GUID]): A list of GUIDs representing the TML objects to be exported.
        export_type (TSExportMetadataType): The type of the TML objects to be exported.

    Returns:
        list[TSExportPayload]: A list of dictionaries, each representing a TML
            export payload with keys `type` and `identifier`..
    """

    metadata_payload: list[TSExportPayload] = []

    for tml_GUID in tml_GUIDs:
        payload = {"identifier": tml_GUID}
        if export_type:
            payload.update({"type": export_type.upper()})

        metadata_payload.append(payload)

    return metadata_payload


def generate_tml_search_payload(
    tml_GUIDs: list[GUID] = (),
    export_type: TSExportMetadataType = None,
    name_patterns: list[str] = (),
) -> list[TSSearchPayload]:
    """
    Generate a payload for searching TML objects.
    Args:
        tml_GUIDs (list[GUID]): A list of GUIDs representing the TML objects to be exported.
        export_type (TSExportMetadataType): The type of the TML objects to be exported.
        name_patterns (list[str]): A list of string pattern to match

    Returns:
        list[TSSearchPayload]: A list of dictionaries, each representing a TML
            search payload with keys `type`, `name_pattern` and `identifier`..
    """

    metadata_payload: list[TSSearchPayload] = []

    def process_type(search_type: str, value_list: []):
        """
        Generate payload item by search type
        :param search_type: Provide search type: "identifier" (GUID) or "name_pattern" (use % as wildcard)
        :param value_list: List of values for a given search_type
        :return: payload for metadata search request
        """
        for value in value_list:
            payload = {search_type: value}
            if export_type:
                payload.update({"type": export_type.upper()})
            metadata_payload.append(payload)

    process_type("identifier", tml_GUIDs)
    process_type("name_pattern", name_patterns)

    return metadata_payload


def format_metadata_tml_response(
    tml_objs: list[TMLObject],
) -> dict[GUID, TMLObject | None]:
    """
    Formats a list of TMLObjects into a dictionary response.
    Args:
        tml_objs (list[TMLObject]): A list of TMLObject instances, each containing a GUID.

    Returns:
        dict[GUID, TMLObject]: A dictionary mapping each GUID to its corresponding TMLObject.
    """

    metadata_response: dict[GUID, TMLObject | None] = {}

    for tml_obj in tml_objs:
        tml_guid = tml_obj.guid
        metadata_response.update({tml_guid: tml_obj})

    return metadata_response


def generate_user_search_payload(cisco_cco_ids: list[str]):
    """
    Generates a search payload for users based on Cisco CCO IDs.
    Args:
        cisco_cco_ids (list[str]): A list of Cisco CCO IDs for which the search payloads are to be generated.

    Returns:
        A list of dictionaries, each containing a search pattern for a Cisco CCO ID.
    """

    search_payload: list[TSUserSearchPayload] = []

    for cisco_cco_id in cisco_cco_ids:
        payload = {"name_pattern": cisco_cco_id}
        search_payload.append(payload)

    return search_payload


def generate_user_permissions_payload(
    user_GUIDs: list[GUID], share_mode: TSShareModeType
):
    """
    Generates a permissions payload for a list of user GUIDs with a specified share mode.
    Args:
        user_GUIDs (list[GUID]): A list of GUIDs representing the users for whom the permissions are to be set.
        share_mode (TSShareModeType): The share mode to apply, which will be converted to uppercase.

    Returns:
        list: A list of dictionaries, each representing a permissions payload for a user.
    """
    permissions_payload = []

    for user_GUID in user_GUIDs:
        permissions_payload.append(
            {"principal": {"identifier": user_GUID}, "share_mode": share_mode.upper()}
        )

    return permissions_payload


def generate_search_metadata_user_payload(
    tml_GUIDs: list[GUID], metadata_type: TSUserMetadataType = None
) -> list[TSMetadataUserSearchPayload]:
    """
    Generate a payload for searching Metadata Users.
    Args:
        tml_GUIDs (list[GUID]): A list of GUIDs representing the TML objects to be exported.
        metadata_type (TSUserMetadataType): The type of the TML objects.

    Returns:
        list[TSMetadataUserSearchPayload]: A list of dictionaries, each representing a TML
            search payload with keys `type` and `identifier`..
    """

    search_payload = []

    for tml_GUID in tml_GUIDs:
        payload = {"identifier": tml_GUID}
        if metadata_type:
            payload.update({"type": metadata_type.upper()})

        search_payload.append(payload)

    return search_payload


def format_metadata_user_search_response(metadata_permission_details: list):
    """
    Formats the metadata permission details into a structured response.
    Args:
        metadata_permission_details: A list of metadata permission details, each
            containing user and permission information.

    Returns:
        A dictionary mapping each metadata GUID to a dictionary of users,
        where each user is mapped to their details, including cisco_cco_id and permissions.
    """

    formatted_response = {}
    for metadata in metadata_permission_details:
        """
        `metadata` contains
            - metadata_id: GUID of the searched TML Object
            - metadata_name: TML Object name in TS
            - metadata_owner: Contains `id` -> `metadata_id` & `name` -> `metadata_name`
            - metadata_author: Contains `id` -> `Author's TS user_GUID & `name` -> `username`
            - principal_permissions_info: List of Principal Group with Permissions List
        """
        metadata_GUID = metadata.get("metadata_id")

        users = {}
        principal_infos = metadata.get("principal_permission_info")
        for principal_info in principal_infos:
            """
            `principal_info` contains
                - principal_type: USER or USER_GROUP
                - principal_sub_type: SAML_USER, LOCAL_USER, LOCAL_GROUP
                - principal_permissions: List of User or UserGroup with Permissions details
            """
            principal_type = principal_info.get("principal_type")
            principal_sub_type = principal_info.get("principal_sub_type")

            if principal_type == "USER" and principal_sub_type == "SAML_USER":
                for principal_permission in principal_info.get("principal_permissions"):
                    """
                    `principal_permission` contains
                        - principal_id: TS GUID for the User or USER_GROUP
                        - principal_name: username -> CISCO_CCO_ID
                        - permission: 'READ_ONLY', 'MODIFY', 'NO_ACCESS'
                        - shared_permission: 'READ_ONLY', 'MODIFY', 'NO_ACCESS'
                        - group_permission: LIST
                    """
                    principal_name = principal_permission.get("principal_name")
                    permission = principal_permission.get("permission")
                    principal_id = principal_permission.get("principal_id")

                    users.update(
                        {
                            principal_name: {
                                "user_GUID": principal_id,
                                "cisco_cco_id": principal_name,
                                "permission": permission,
                            }
                        }
                    )

        formatted_response.update({metadata_GUID: users})

    return formatted_response


def format_data_mappings_data(mappings_file: bytes):
    """
    Parses and formats data mappings from a JSON-encoded byte string.
    Args:
        mappings_file (bytes): A byte string containing JSON-encoded data mappings.

    Returns:
        dict: A dictionary where each value is parsed from a JSON string to a Python dictionary.
    """
    mappings_data = json.loads(mappings_file)
    mappings_data.update(
        {
            k: ast.literal_eval(v) if isinstance(v, str) else v
            for k, v in mappings_data.items()
        }
    )

    return mappings_data


def formulate_column_types(data_type: str, mapping: dict = None):
    """
    Formulates the TS Column Type & TS Data Type from SF Data Type and mapping JSON
    Args:
        data_type: Snowflake Column DataType
        mapping: Mapping JSON containing TS Column Type

    Returns:
        A dictionary containing TS Data Type and Column Properties
    """
    db_data_type = sf_data_type_map.get(data_type.upper(), "VARCHAR")

    ts_column_type = "ATTRIBUTE"
    if not mapping:
        if db_data_type in ["BIGINT", "INT", "DOUBLE"]:
            ts_column_type = "MEASURE"
        mapping = {
            "column_type": ts_column_type,
        }

    if "index_type" not in mapping:
        mapping.update({"index_type": "DONT_INDEX"})

    if "aggregation" not in mapping:
        mapping.update({"aggregation": "SUM" if ts_column_type == "MEASURE" else None})

    return {
        "db_data_type": db_data_type.upper(),
        "properties": mapping,
    }


def format_import_response(tml_objs: list[TMLObject], import_response: list):
    """
    Formats TS Import response as required for further processing
    Args:
        tml_objs (list[TMlObject]): List of TMLObject Requested for import
        import_response (list): TML Objects Import Responses

    Returns:
        A dictionary contain the Valid GUIDs and Error Responses
    """
    error_responses = []
    valid_responses = []

    for response_data in import_response:
        """
        `response_data` contains the following
         - request_index
         - response: contains the details of the TML Object
        """
        request_index = response_data.get("request_index")
        resp = response_data.get("response")

        if resp:
            """
            `response` contains the following
            - header: Header level info of the TML Object
            - status: Creation status of the TML Object
            """
            status_details = resp.get("status")
            status_code = status_details.get("status_code").upper()

            if status_code in ["OK", "WARNING"]:
                header = resp.get("header")
                metadata_GUID = header.get("id_guid")

                tml_obj = tml_objs[request_index]
                tml_obj.guid = metadata_GUID

                valid_responses.append(tml_obj)
            else:
                error_responses.append(resp)

    return {"valid_responses": valid_responses, "error_responses": error_responses}
