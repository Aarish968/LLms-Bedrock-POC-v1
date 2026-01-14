from enum import Enum
from typing import TYPE_CHECKING, Union

from prefect.events import Event, emit_event

from . import EventStage

if TYPE_CHECKING:
    from ..blocks.environment import Env, TEnv


class ThoughtSpotLiveboardEventType(str, Enum):
    manage_liveboards = "thoughtspot.liveboards.manage"
    discover_liveboards = "thoughtspot.liveboards.discover"

    def __str__(self) -> str:
        return str.__str__(self)


def _emit_manage_liveboards(
    env: Union["TEnv", "Env"],
    canvas_id: int,
    notification_id: int,
    dc_user_id: int,
    dc_engagement_id: int,
    request_id: int,
    stage: EventStage,
) -> "Event":
    env = str(env)
    event = f"{ThoughtSpotLiveboardEventType.manage_liveboards!s}.{stage!s}"
    return emit_event(
        event=event,
        resource={
            "prefect.resource.id": f"datacanvas.{env!s}.canvas.{canvas_id}",
            "prefect.resource.name": f"Data Canvas {env.title()}",
        },
        payload={
            "env": str(env),
            "canvas_id": canvas_id,
            "notification_id": notification_id,
            "dc_user_id": dc_user_id,
            "dc_engagement_id": dc_engagement_id,
            "request_id": request_id,
        },
    )


def emit_manage_liveboards_started(
    env: Union["TEnv", "Env"],
    canvas_id: int,
    notification_id: int,
    dc_user_id: int,
    dc_engagement_id: int,
    request_id: int,
) -> "Event":
    return _emit_manage_liveboards(
        env=env,
        canvas_id=canvas_id,
        notification_id=notification_id,
        dc_user_id=dc_user_id,
        dc_engagement_id=dc_engagement_id,
        request_id=request_id,
        stage=EventStage.start,
    )


def emit_manage_liveboards_success(
    env: Union["TEnv", "Env"],
    canvas_id: int,
    notification_id: int,
    dc_user_id: int,
    dc_engagement_id: int,
    request_id: int,
) -> "Event":
    return _emit_manage_liveboards(
        env=env,
        canvas_id=canvas_id,
        notification_id=notification_id,
        dc_user_id=dc_user_id,
        dc_engagement_id=dc_engagement_id,
        request_id=request_id,
        stage=EventStage.success,
    )


def emit_manage_liveboards_failure(
    env: Union["TEnv", "Env"],
    canvas_id: int,
    notification_id: int,
    dc_user_id: int,
    dc_engagement_id: int,
    request_id: int,
) -> "Event":
    return _emit_manage_liveboards(
        env=env,
        canvas_id=canvas_id,
        notification_id=notification_id,
        dc_user_id=dc_user_id,
        dc_engagement_id=dc_engagement_id,
        request_id=request_id,
        stage=EventStage.failure,
    )


def emit_manage_liveboards_requested(
    env: Union["TEnv", "Env"],
    canvas_id: int,
    notification_id: int,
    dc_user_id: int,
    dc_engagement_id: int,
    request_id: int,
) -> "Event":
    return _emit_manage_liveboards(
        env=env,
        canvas_id=canvas_id,
        notification_id=notification_id,
        dc_user_id=dc_user_id,
        dc_engagement_id=dc_engagement_id,
        request_id=request_id,
        stage=EventStage.requested,
    )


def _emit_discover_liveboards(
    env: Union["TEnv", "Env"],
    canvas_id: int,
    notification_id: int,
    dc_user_id: int,
    dc_engagement_id: int,
    request_id: int,
    stage: EventStage,
) -> "Event":
    env = str(env)
    event = f"{ThoughtSpotLiveboardEventType.discover_liveboards!s}.{stage!s}"
    return emit_event(
        event=event,
        resource={
            "prefect.resource.id": f"datacanvas.{env!s}.canvas.{canvas_id}",
            "prefect.resource.name": f"Data Canvas {env.title()}",
        },
        payload={
            "env": str(env),
            "canvas_id": canvas_id,
            "notification_id": notification_id,
            "dc_user_id": dc_user_id,
            "dc_engagement_id": dc_engagement_id,
            "request_id": request_id,
        },
    )


def emit_discover_liveboards_requested(
    env: Union["TEnv", "Env"],
    canvas_id: int,
    notification_id: int,
    dc_user_id: int,
    dc_engagement_id: int,
    request_id: int,
) -> "Event":
    return _emit_discover_liveboards(
        env=env,
        canvas_id=canvas_id,
        notification_id=notification_id,
        dc_user_id=dc_user_id,
        dc_engagement_id=dc_engagement_id,
        stage=EventStage.requested,
        request_id=request_id,
    )


def emit_discover_liveboards_started(
    env: Union["TEnv", "Env"],
    canvas_id: int,
    notification_id: int,
    dc_user_id: int,
    dc_engagement_id: int,
    request_id: int,
) -> "Event":
    return _emit_discover_liveboards(
        env=env,
        canvas_id=canvas_id,
        notification_id=notification_id,
        dc_user_id=dc_user_id,
        dc_engagement_id=dc_engagement_id,
        request_id=request_id,
        stage=EventStage.start,
    )


def emit_discover_liveboards_success(
    env: Union["TEnv", "Env"],
    canvas_id: int,
    notification_id: int,
    dc_user_id: int,
    dc_engagement_id: int,
    request_id: int,
) -> "Event":
    return _emit_discover_liveboards(
        env=env,
        canvas_id=canvas_id,
        notification_id=notification_id,
        dc_user_id=dc_user_id,
        dc_engagement_id=dc_engagement_id,
        request_id=request_id,
        stage=EventStage.success,
    )


def emit_discover_liveboards_failure(
    env: Union["TEnv", "Env"],
    canvas_id: int,
    notification_id: int,
    dc_user_id: int,
    dc_engagement_id: int,
    request_id: int,
) -> "Event":
    return _emit_discover_liveboards(
        env=env,
        canvas_id=canvas_id,
        notification_id=notification_id,
        dc_user_id=dc_user_id,
        dc_engagement_id=dc_engagement_id,
        request_id=request_id,
        stage=EventStage.failure,
    )
