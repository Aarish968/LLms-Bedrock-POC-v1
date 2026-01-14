import datetime

from . import Model


class ResolvedSerialRow(Model):
    serial_number: str
    instance_id: int


class AuditResolvedSerialRow(Model):
    requested_serial: str
    instance_id: int
    update_by: str | None
    update_dtm: datetime.datetime | None


class AuditResolvedCurrentSerialRow(Model):
    bill_to_site_use_id: int | None
    covered_status: str | None
    duplicate_ib_flag: str | None
    dup_cnt: int | None
    installed_at_gu_id: int | None
    instance_id: int | None
    instance_status_desc: str | None
    is_good_status: str | None
    is_guid: str | None
    is_managed_contract: str | None
    mapped_to_service_flag: str | None
    mx_maintenance_so_number: str | None
    parent_instance_id: int | None
    product_last_date_of_support_ldos: datetime.date | None
    product_so: str | None
    resolved_instance: int | None
    resolved_instance_id: int | None
    score: float | None
    score_rank: int | None
    serial_number: str | None


__all__ = [
    "AuditResolvedCurrentSerialRow",
    "AuditResolvedSerialRow",
    "ResolvedSerialRow",
]
