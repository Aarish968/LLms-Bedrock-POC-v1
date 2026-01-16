from pydantic import BaseModel


class TSParameterListConfigListChoice(BaseModel):
    value: str | None = None
    display_name: str | None = None


class TSParameterRangeConfig(BaseModel):
    range_min: str | None = None
    range_max: str | None = None
    include_min: bool | None = None
    include_max: bool | None = None


class TSParameterListConfig(BaseModel):
    list_choice: list[TSParameterListConfigListChoice] | None = None


class TSParameter(BaseModel):
    id: str | None = None
    name: str | None = None
    data_type: str | None = None
    default_value: str | None = None
    list_config: TSParameterListConfig | None = None
    list_column_id: str | None = None
    range_config: TSParameterRangeConfig | None = None
    sap_parameter_name: str | None = None
    linked_parameters: list[str] | None = None
    description: str | None = None
    is_hidden: bool | None = None
