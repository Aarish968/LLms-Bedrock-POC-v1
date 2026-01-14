from enum import Enum


class MessageType(str, Enum):
    text = "text"
    download = "download"
    table = "table"
    code = "code"
    parameters = "parameters"

    def __str__(self) -> str:
        return str.__str__(self)


class MessageStatus(str, Enum):
    pending = "pending"
    result = "result"
    error = "error"

    def __str__(self) -> str:
        return str.__str__(self)


class UiEnum(str, Enum):
    """
    UI Enums used by `guided_workflow_backend` in `api/v2/models/enums.py`
    """

    add_to_contract = "add-to-contract"
    bulk_tagging = "bulk-tagging"
    canvas_actions = "canvas-actions"
    collector_upload = "collector-upload"
    customer_upload = "customer-upload"
    disengagement = "disengagement"
    general_notification = "general-notification"
    generate_docusign_scope = "generate-docusign-scope"
    instance_tagging = "instance-tagging"
    log_signoff = "log-signoff"
    renewal = "renewal"
    serial_tagging = "serial-tagging"
    site_report = "site-report"
    snif_report = "snif-report"
    tag_history = "tag-history"
    acat_discovery = "acat-discovery"
    host_name_relink = "host-name-relink"
    host_name_site_moves = "host-name-site-moves"

    def __str__(self) -> str:
        return str.__str__(self)
