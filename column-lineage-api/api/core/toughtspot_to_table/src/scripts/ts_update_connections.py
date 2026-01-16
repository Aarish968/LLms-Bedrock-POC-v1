from dc_canvas_service.services.thoughtspot import ThoughtSpotService
from dc_canvas_service.common import Settings, Env
import re
import logging

STATIC_CONNECTION_NAME = "CANVAS_CONNECTION"

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())


def update_connections(env: Env = Env.dev, limit: int = 9999) -> None:
    """
    Bulk Update Canvases to use individual connection (CANVAS-xxxx_CONNECTION) instead of static CANVAS_CONNECTION.
    :param env: Env
    :param limit: max number of canvases to search for
    :return:
    """

    settings = Settings(env)
    ts = ThoughtSpotService(settings=settings, s3=None)  # NOQA
    static_connection = ts.search_connections(STATIC_CONNECTION_NAME, include_details=True)[0]
    update_ts_connection(ts, static_connection)


def update_ts_connection(ts: ThoughtSpotService, connection: dict) -> None:
    """
    Update TS connection to use individual canvas connection.
    :param ts: ThoughSpotService
    :param connection: connection metadata dict
    :return:
    """

    connection_name = connection.get("name")
    tables = connection.get("details", {}).get("tables")
    logger.info(f"Processing {len(tables)} tables for {connection_name}...")
    tables = connection.get("details", {}).get("tables")
    for ix, table in enumerate(tables, 1):

        new_connection_name = re.sub("CANVAS_(\\d+)_THOUGHT_SPOT_V",
                                     "CANVAS-\\1_CONNECTION",
                                     table.get("name"))
        logger.info(f"Updating {table.get("name")} to use {new_connection_name} ({ix}/{len(tables)})...")

        if not ts.get_connection_guid(new_connection_name):
            ts.create_connection(connection_name=new_connection_name, table_name=table.get("name"))

        table_id = table.get("id")

        # get tml
        tmls = ts.get_tmls(metadata_GUIDs=[table_id])
        tml_table = tmls.get(table_id)

        # update connection
        tml_table.table.connection.name = new_connection_name

        # push tml
        response = ts.push_tmls([tml_table])
        if error := response.get("error_responses"):
            raise ImportError(error)

if __name__ == "__main__":
    update_connections()
