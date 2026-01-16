from services.snowflake import SnowflakeService
from services.snowflake.actions import get_actions, get_column_types, get_ts_objects

from dc_canvas_service.common.settings import Settings
from dc_canvas_service.services.canvas import CanvasService
from dc_canvas_service.services.liveboard import LiveboardService
from dc_canvas_service.services.s3 import S3Service
from dc_canvas_service.services.thoughtspot import ThoughtSpotService, TSTable

if __name__ == "__main__":
    # settings = Settings(env="prod")
    # s3 = S3Service()
    # sf = SnowflakeService(settings=settings)
    # ts = ThoughtSpotService(settings=settings, s3=s3)

    # canvas_service = CanvasService(
    #     canvas_id=33112, engagement_id=21107, env="prod", user_cisco_cco_id="deepeaga@cisco.com"
    # )

    canvas_service = CanvasService(
        canvas_id=4389,
        engagement_id=94,
        env="dev",
        user_cisco_cco_id="deepeaga@cisco.com",
    )

    # w = canvas_service._create_ts_worksheet()
    #
    # print(w)

    # canvas_service.refresh_ts_datasource()

    print(canvas_service.ts_worksheet, canvas_service.ts_table)
