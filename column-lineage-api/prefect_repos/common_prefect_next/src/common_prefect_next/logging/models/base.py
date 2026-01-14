import datetime

from pydantic import BaseModel, ConfigDict

from common_prefect_next.utils import isoformat_utc


class Model(BaseModel):
    model_config = ConfigDict(
        populate_by_name=True,
        use_enum_values=True,
        json_encoders={datetime.datetime: isoformat_utc},
    )
