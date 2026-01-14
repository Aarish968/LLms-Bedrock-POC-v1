from enum import Enum
from typing import TYPE_CHECKING, Union

from prefect.events import Event, emit_event

from . import EventStage

if TYPE_CHECKING:
    from ..blocks.environment import Env, TEnv


class EngagementEventType(str, Enum):
    engagement_share = "datacanvas.engagement.share"
    engagement_view_refresh = "datacanvas.engagement.refresh"

    def __str__(self) -> str:
        return str.__str__(self)


def _emit_engagement_share(
    env: Union["TEnv", "Env"],
    dc_engagement_id: int,
    notification_id: int,
    dc_user_id: int,
    shared_with_dc_user_id: int,
    request_id: int,
    stage: EventStage,
) -> "Event":
    event = f"{EngagementEventType.engagement_share!s}.{stage!s}"
    return emit_event(
        event=event,
        resource={
            "prefect.resource.id": f"datacanvas.{env}.engagement.{dc_engagement_id}",
            "prefect.resource.name": f"DataCanvas Engagement {env.title()}",
        },
        payload={
            "env": str(env),
            "dc_engagement_id": dc_engagement_id,
            "notification_id": notification_id,
            "dc_user_id": dc_user_id,
            "request_id": request_id,
            "shared_with_dc_user_id": shared_with_dc_user_id,
        },
    )


def emit_engagement_share_started(
    env: Union["TEnv", "Env"],
    dc_engagement_id: int,
    notification_id: int,
    dc_user_id: int,
    shared_with_dc_user_id: int,
    request_id: int,
) -> "Event":
    return _emit_engagement_share(
        env=env,
        dc_engagement_id=dc_engagement_id,
        notification_id=notification_id,
        dc_user_id=dc_user_id,
        shared_with_dc_user_id=shared_with_dc_user_id,
        request_id=request_id,
        stage=EventStage.start,
    )


def emit_engagement_share_success(
    env: Union["TEnv", "Env"],
    dc_engagement_id: int,
    notification_id: int,
    dc_user_id: int,
    shared_with_dc_user_id: int,
    request_id: int,
) -> "Event":
    return _emit_engagement_share(
        env=env,
        dc_engagement_id=dc_engagement_id,
        notification_id=notification_id,
        dc_user_id=dc_user_id,
        shared_with_dc_user_id=shared_with_dc_user_id,
        request_id=request_id,
        stage=EventStage.success,
    )


def emit_engagement_share_failure(
    env: Union["TEnv", "Env"],
    dc_engagement_id: int,
    notification_id: int,
    dc_user_id: int,
    shared_with_dc_user_id: int,
    request_id: int,
) -> "Event":
    return _emit_engagement_share(
        env=env,
        dc_engagement_id=dc_engagement_id,
        notification_id=notification_id,
        dc_user_id=dc_user_id,
        shared_with_dc_user_id=shared_with_dc_user_id,
        request_id=request_id,
        stage=EventStage.failure,
    )


def emit_engagement_share_requested(
    env: Union["TEnv", "Env"],
    dc_engagement_id: int,
    notification_id: int,
    dc_user_id: int,
    shared_with_dc_user_id: int,
    request_id: int,
) -> "Event":
    return _emit_engagement_share(
        env=env,
        dc_engagement_id=dc_engagement_id,
        notification_id=notification_id,
        dc_user_id=dc_user_id,
        shared_with_dc_user_id=shared_with_dc_user_id,
        request_id=request_id,
        stage=EventStage.requested,
    )


def _emit_engagement_refresh(
    env: Union["TEnv", "Env"],
    dc_engagement_id: int,
    notification_id: int,
    dc_user_id: int,
    request_id: int | None,
    stage: EventStage,
) -> "Event":
    event = f"{EngagementEventType.engagement_view_refresh!s}.{stage!s}"
    return emit_event(
        event=event,
        resource={
            "prefect.resource.id": f"datacanvas.{env}.engagement.{dc_engagement_id}",
            "prefect.resource.name": f"Data Canvas {env.title()}",
        },
        payload={
            "env": str(env),
            "notification_id": notification_id,
            "dc_user_id": dc_user_id,
            "dc_engagement_id": dc_engagement_id,
            "request_id": request_id,
        },
    )


def emit_engagement_refresh_requested(
    env: Union["TEnv", "Env"],
    dc_engagement_id: int,
    notification_id: int,
    dc_user_id: int,
    request_id: int | None,
) -> "Event":
    return _emit_engagement_refresh(
        env=env,
        dc_engagement_id=dc_engagement_id,
        notification_id=notification_id,
        dc_user_id=dc_user_id,
        request_id=request_id,
        stage=EventStage.requested,
    )


def emit_engagement_refresh_started(
    env: Union["TEnv", "Env"],
    dc_engagement_id: int,
    notification_id: int,
    dc_user_id: int,
    request_id: int | None,
) -> "Event":
    return _emit_engagement_refresh(
        env=env,
        dc_engagement_id=dc_engagement_id,
        notification_id=notification_id,
        dc_user_id=dc_user_id,
        request_id=request_id,
        stage=EventStage.start,
    )


def emit_engagement_refresh_success(
    env: Union["TEnv", "Env"],
    dc_engagement_id: int,
    notification_id: int,
    dc_user_id: int,
    request_id: int | None,
) -> "Event":
    return _emit_engagement_refresh(
        env=env,
        dc_engagement_id=dc_engagement_id,
        notification_id=notification_id,
        dc_user_id=dc_user_id,
        request_id=request_id,
        stage=EventStage.success,
    )


def emit_engagement_refresh_failure(
    env: Union["TEnv", "Env"],
    dc_engagement_id: int,
    notification_id: int,
    dc_user_id: int,
    request_id: int | None,
) -> "Event":
    return _emit_engagement_refresh(
        env=env,
        dc_engagement_id=dc_engagement_id,
        notification_id=notification_id,
        dc_user_id=dc_user_id,
        request_id=request_id,
        stage=EventStage.failure,
    )
