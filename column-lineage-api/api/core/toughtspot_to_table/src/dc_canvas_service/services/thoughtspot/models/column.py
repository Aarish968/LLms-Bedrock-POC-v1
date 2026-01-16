from pydantic import BaseModel

from ..enums import TSGeometryE


class TSColumnPropertiesGeoConfigSubRegion(BaseModel):
    country: str | None = None
    region_name: str | None = None


class TSColumnPropertiesCurrencyFormat(BaseModel):
    is_browser: bool | None = None
    column: str | None = None
    iso_code: str | None = None


class TSColumnPropertiesGeoConfig(BaseModel):
    latitude: bool | None = None
    longitude: bool | None = None
    country: bool | None = None
    region_name: TSColumnPropertiesGeoConfigSubRegion | None = None
    custom_file_guid: str | None = None
    geometryType: TSGeometryE | None = None


class TSColumnProperties(BaseModel):
    column_type: str | None = None
    aggregation: str | None = None
    index_type: str | None = None
    index_priority: float | None = None
    synonyms: list[str] | None = None
    is_attribution_dimension: bool | None = None
    is_additive: bool | None = None
    calendar: str | None = None
    format_pattern: str | None = None
    currency_type: TSColumnPropertiesCurrencyFormat | None = None
    is_hidden: bool | None = None
    geo_config: TSColumnPropertiesGeoConfig | None = None
    spotiq_preference: str | None = None
    search_iq_preferred: bool | None = None
    hierarchical_column_name: str | None = None
    synonym_type: str | None = None
    value_casing: str | None = None
    custom_order: list[str] | None = None
    use_cases: list[str] | None = None


class TSDBColumnProperties(BaseModel):
    data_type: str | None


class TSColumn(BaseModel):
    name: str
    description: str | None = None
    db_column_name: str | None = None
    properties: TSColumnProperties | None = None
    db_column_properties: TSDBColumnProperties | None = None
