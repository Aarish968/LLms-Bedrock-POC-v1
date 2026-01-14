from enum import Enum
from typing import TYPE_CHECKING, Any, TypedDict

from prefect.events import emit_event

from common_prefect_next.blocks.environment import Env, TEnv

from . import EventStage

if TYPE_CHECKING:
    from prefect.events import Event


class PrefectEventData(TypedDict):
    event: str
    resource: dict[str, str]
    payload: dict[str, Any] | None


class CanvasEventType(str, Enum):
    canvas_create = "datacanvas.canvas.create"
    canvas_view_refresh = "datacanvas.canvas.refresh"
    canvas_rebuild = "datacanvas.canvas.rebuild"
    canvas_delete = "datacanvas.canvas.delete"
    canvas_share = "datacanvas.canvas.share"
    canvas_cleanup = "datacanvas.canvas.cleanup"

    def __str__(self) -> str:
        return str.__str__(self)


def _emit_canvas_create(
    env: TEnv | Env,
    canvas_id: int,
    notification_id: int,
    dc_user_id: int,
    dc_engagement_id: int,
    request_id: int | None,
    stage: EventStage,
) -> "Event":
    event = f"{CanvasEventType.canvas_create!s}.{stage!s}"
    return emit_event(
        event=event,
        resource={
            "prefect.resource.id": f"datacanvas.{env}.canvas.{canvas_id}",
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


def emit_canvas_create_started(
    env: TEnv | Env,
    canvas_id: int,
    notification_id: int,
    dc_user_id: int,
    dc_engagement_id: int,
    request_id: int | None,
) -> "Event":
    return _emit_canvas_create(
        env=env,
        canvas_id=canvas_id,
        notification_id=notification_id,
        dc_user_id=dc_user_id,
        dc_engagement_id=dc_engagement_id,
        request_id=request_id,
        stage=EventStage.start,
    )


def emit_canvas_create_success(
    env: TEnv | Env,
    canvas_id: int,
    notification_id: int,
    dc_user_id: int,
    dc_engagement_id: int,
    request_id: int | None,
) -> "Event":
    return _emit_canvas_create(
        env=env,
        canvas_id=canvas_id,
        notification_id=notification_id,
        dc_user_id=dc_user_id,
        dc_engagement_id=dc_engagement_id,
        request_id=request_id,
        stage=EventStage.success,
    )


def emit_canvas_create_failure(
    env: TEnv | Env,
    canvas_id: int,
    notification_id: int,
    dc_user_id: int,
    dc_engagement_id: int,
    request_id: int | None,
) -> "Event":
    return _emit_canvas_create(
        env=env,
        canvas_id=canvas_id,
        notification_id=notification_id,
        dc_user_id=dc_user_id,
        dc_engagement_id=dc_engagement_id,
        request_id=request_id,
        stage=EventStage.failure,
    )


def _emit_canvas_refresh(
    env: TEnv | Env,
    canvas_id: int,
    notification_id: int,
    dc_user_id: int,
    dc_engagement_id: int,
    request_id: int | None,
    stage: EventStage,
) -> "Event":
    event = f"{CanvasEventType.canvas_view_refresh!s}.{stage!s}"
    return emit_event(
        event=event,
        resource={
            "prefect.resource.id": f"datacanvas.{env}.canvas.{canvas_id}",
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


def emit_canvas_refresh_started(
    env: TEnv | Env,
    canvas_id: int,
    notification_id: int,
    dc_user_id: int,
    dc_engagement_id: int,
    request_id: int | None,
) -> "Event":
    return _emit_canvas_refresh(
        env=env,
        canvas_id=canvas_id,
        notification_id=notification_id,
        dc_user_id=dc_user_id,
        dc_engagement_id=dc_engagement_id,
        request_id=request_id,
        stage=EventStage.start,
    )


def emit_canvas_refresh_success(
    env: TEnv | Env,
    canvas_id: int,
    notification_id: int,
    dc_user_id: int,
    dc_engagement_id: int,
    request_id: int | None,
) -> "Event":
    return _emit_canvas_refresh(
        env=env,
        canvas_id=canvas_id,
        notification_id=notification_id,
        dc_user_id=dc_user_id,
        dc_engagement_id=dc_engagement_id,
        request_id=request_id,
        stage=EventStage.success,
    )


def emit_canvas_refresh_failure(
    env: TEnv | Env,
    canvas_id: int,
    notification_id: int,
    dc_user_id: int,
    dc_engagement_id: int,
    request_id: int | None,
) -> "Event":
    return _emit_canvas_refresh(
        env=env,
        canvas_id=canvas_id,
        notification_id=notification_id,
        dc_user_id=dc_user_id,
        dc_engagement_id=dc_engagement_id,
        request_id=request_id,
        stage=EventStage.failure,
    )


def _emit_canvas_rebuild(
    env: TEnv | Env,
    canvas_id: int,
    notification_id: int,
    dc_user_id: int,
    dc_engagement_id: int,
    request_id: int | None,
    stage: EventStage,
) -> "Event":
    event = f"{CanvasEventType.canvas_rebuild!s}.{stage!s}"
    return emit_event(
        event=event,
        resource={
            "prefect.resource.id": f"datacanvas.{env}.canvas.{canvas_id}",
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


def emit_canvas_rebuild_started(
    env: TEnv | Env,
    canvas_id: int,
    notification_id: int,
    dc_user_id: int,
    dc_engagement_id: int,
    request_id: int | None,
) -> "Event":
    return _emit_canvas_rebuild(
        env=env,
        canvas_id=canvas_id,
        notification_id=notification_id,
        dc_user_id=dc_user_id,
        dc_engagement_id=dc_engagement_id,
        request_id=request_id,
        stage=EventStage.start,
    )


def emit_canvas_rebuild_success(
    env: TEnv | Env,
    canvas_id: int,
    notification_id: int,
    dc_user_id: int,
    dc_engagement_id: int,
    request_id: int | None,
) -> "Event":
    return _emit_canvas_rebuild(
        env=env,
        canvas_id=canvas_id,
        notification_id=notification_id,
        dc_user_id=dc_user_id,
        dc_engagement_id=dc_engagement_id,
        request_id=request_id,
        stage=EventStage.success,
    )


def emit_canvas_rebuild_failure(
    env: TEnv | Env,
    canvas_id: int,
    notification_id: int,
    dc_user_id: int,
    dc_engagement_id: int,
    request_id: int | None,
) -> "Event":
    return _emit_canvas_rebuild(
        env=env,
        canvas_id=canvas_id,
        notification_id=notification_id,
        dc_user_id=dc_user_id,
        dc_engagement_id=dc_engagement_id,
        request_id=request_id,
        stage=EventStage.failure,
    )


def _emit_canvas_delete(
    env: TEnv | Env,
    canvas_id: int,
    notification_id: int,
    dc_user_id: int,
    dc_engagement_id: int,
    request_id: int | None,
    stage: EventStage,
) -> "Event":
    event = f"{CanvasEventType.canvas_delete!s}.{stage!s}"
    return emit_event(
        event=event,
        resource={
            "prefect.resource.id": f"datacanvas.{env}.canvas.{canvas_id}",
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


def emit_canvas_delete_requested(
    env: TEnv | Env,
    canvas_id: int,
    notification_id: int,
    dc_user_id: int,
    dc_engagement_id: int,
    request_id: int | None,
) -> "Event":
    return _emit_canvas_delete(
        env=env,
        canvas_id=canvas_id,
        notification_id=notification_id,
        dc_user_id=dc_user_id,
        dc_engagement_id=dc_engagement_id,
        request_id=request_id,
        stage=EventStage.requested,
    )


def emit_canvas_delete_started(
    env: TEnv | Env,
    canvas_id: int,
    notification_id: int,
    dc_user_id: int,
    dc_engagement_id: int,
    request_id: int | None,
) -> "Event":
    return _emit_canvas_delete(
        env=env,
        canvas_id=canvas_id,
        notification_id=notification_id,
        dc_user_id=dc_user_id,
        dc_engagement_id=dc_engagement_id,
        request_id=request_id,
        stage=EventStage.start,
    )


def emit_canvas_delete_success(
    env: TEnv | Env,
    canvas_id: int,
    notification_id: int,
    dc_user_id: int,
    dc_engagement_id: int,
    request_id: int | None,
) -> "Event":
    return _emit_canvas_delete(
        env=env,
        canvas_id=canvas_id,
        notification_id=notification_id,
        dc_user_id=dc_user_id,
        dc_engagement_id=dc_engagement_id,
        request_id=request_id,
        stage=EventStage.success,
    )


def emit_canvas_delete_failure(
    env: TEnv | Env,
    canvas_id: int,
    notification_id: int,
    dc_user_id: int,
    dc_engagement_id: int,
    request_id: int | None,
) -> "Event":
    return _emit_canvas_delete(
        env=env,
        canvas_id=canvas_id,
        notification_id=notification_id,
        dc_user_id=dc_user_id,
        dc_engagement_id=dc_engagement_id,
        request_id=request_id,
        stage=EventStage.failure,
    )


def _emit_canvas_share(
    env: TEnv | Env,
    canvas_id: int,
    notification_id: int,
    dc_user_id: int,
    dc_engagement_id: int,
    request_id: int | None,
    shared_with_dc_user_id: int,
    stage: EventStage,
) -> "Event":
    event = f"{CanvasEventType.canvas_share!s}.{stage!s}"
    return emit_event(
        event=event,
        resource={
            "prefect.resource.id": f"datacanvas.{env}.canvas.{canvas_id}",
            "prefect.resource.name": f"Data Canvas {env.title()}",
        },
        payload={
            "env": str(env),
            "canvas_id": canvas_id,
            "notification_id": notification_id,
            "dc_user_id": dc_user_id,
            "dc_engagement_id": dc_engagement_id,
            "request_id": request_id,
            "shared_with_dc_user_id": shared_with_dc_user_id,
        },
    )


def emit_canvas_share_started(
    env: TEnv | Env,
    canvas_id: int,
    notification_id: int,
    dc_user_id: int,
    dc_engagement_id: int,
    request_id: int | None,
    shared_with_dc_user_id: int,
) -> "Event":
    return _emit_canvas_share(
        env=env,
        canvas_id=canvas_id,
        notification_id=notification_id,
        dc_user_id=dc_user_id,
        dc_engagement_id=dc_engagement_id,
        request_id=request_id,
        shared_with_dc_user_id=shared_with_dc_user_id,
        stage=EventStage.start,
    )


def emit_canvas_share_success(
    env: TEnv | Env,
    canvas_id: int,
    notification_id: int,
    dc_user_id: int,
    dc_engagement_id: int,
    request_id: int | None,
    shared_with_dc_user_id: int,
) -> "Event":
    return _emit_canvas_share(
        env=env,
        canvas_id=canvas_id,
        notification_id=notification_id,
        dc_user_id=dc_user_id,
        dc_engagement_id=dc_engagement_id,
        request_id=request_id,
        shared_with_dc_user_id=shared_with_dc_user_id,
        stage=EventStage.success,
    )


def emit_canvas_share_failure(
    env: TEnv | Env,
    canvas_id: int,
    notification_id: int,
    dc_user_id: int,
    dc_engagement_id: int,
    request_id: int | None,
    shared_with_dc_user_id: int,
) -> "Event":
    return _emit_canvas_share(
        env=env,
        canvas_id=canvas_id,
        notification_id=notification_id,
        dc_user_id=dc_user_id,
        dc_engagement_id=dc_engagement_id,
        request_id=request_id,
        shared_with_dc_user_id=shared_with_dc_user_id,
        stage=EventStage.failure,
    )


def emit_canvas_share_requested(
    env: TEnv | Env,
    canvas_id: int,
    notification_id: int,
    dc_user_id: int,
    dc_engagement_id: int,
    request_id: int | None,
    shared_with_dc_user_id: int,
) -> "Event":
    return _emit_canvas_share(
        env=env,
        canvas_id=canvas_id,
        notification_id=notification_id,
        dc_user_id=dc_user_id,
        dc_engagement_id=dc_engagement_id,
        request_id=request_id,
        shared_with_dc_user_id=shared_with_dc_user_id,
        stage=EventStage.requested,
    )


def _emit_canvas_cleanup(env: TEnv | Env, canvas_id: int, stage: EventStage) -> "Event":
    event = f"{CanvasEventType.canvas_cleanup!s}.{stage!s}"
    return emit_event(
        event=event,
        resource={
            "prefect.resource.id": f"datacanvas.{env}.canvas.{canvas_id}",
            "prefect.resource.name": f"Data Canvas {env.title()}",
        },
        payload={
            "env": str(env),
            "canvas_id": canvas_id,
        },
    )


def emit_canvas_cleanup_started(env: TEnv | Env, canvas_id: int) -> "Event":
    return _emit_canvas_cleanup(env=env, canvas_id=canvas_id, stage=EventStage.start)


def emit_canvas_cleanup_success(env: TEnv | Env, canvas_id: int) -> "Event":
    return _emit_canvas_cleanup(env=env, canvas_id=canvas_id, stage=EventStage.success)


def emit_canvas_cleanup_failure(env: TEnv | Env, canvas_id: int) -> "Event":
    return _emit_canvas_cleanup(env=env, canvas_id=canvas_id, stage=EventStage.failure)


def emit_canvas_cleanup_requested(env: TEnv | Env, canvas_id: int) -> "Event":
    """
    Event is called from a process which finds inactive/old canvases. Since this is not a user
    initiated action, it will not have a notification_id, dc_user_id, or request_id
    """

    return _emit_canvas_cleanup(
        env=env, canvas_id=canvas_id, stage=EventStage.requested
    )
