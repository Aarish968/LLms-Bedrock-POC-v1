from {{cookiecutter.project_slug}}.common import setup_extra_loggers
from {{cookiecutter.project_slug}}.common.models import Settings, MyFlowPayload
{%- if cookiecutter.user_facing %}
from common_prefect_next.blocks.data_canvas import get_notification_block
from common_prefect_next.handlers.failure import handle_failure
{%- endif %}
from prefect import flow

{% if cookiecutter.user_facing %}
@flow(on_failure=[handle_failure])
{%- else %}
@flow()
{%- endif %}
@setup_extra_loggers
def {{cookiecutter.project_slug}}_flow(
    payload: MyFlowPayload,
    env: TEnv | Env,
    warehouse: TWarehouse | Warehouse,
    ) -> None:

    logger = get_run_logger()
    settings = Settings(env=env, warehouse=warehouse)
    get_engine = settings.get_engine
    {% if cookiecutter.user_facing %}
    notify_block = get_notification_block(env=env)
    notify_block.notification_id = payload.notification_id
    notify_block.send_text("Starting {{cookiecutter.project_slug}}")
    {% endif %}

    try:
        engine = get_engine()
        with engine.begin() as conn:
            # Do something with the database
            # On __exit__, the transaction is committed automatically
            ...
    except Exception:
        logger.exception("An error occurred during {{cookiecutter.project_slug}}")
        raise
    {% if cookiecutter.user_facing %}
    else:
        notify_block.send_text("{{cookiecutter.project_slug}} completed successfully")
    {% endif %}
    