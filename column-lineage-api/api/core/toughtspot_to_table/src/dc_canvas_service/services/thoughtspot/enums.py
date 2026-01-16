from enum import Enum


class TSComparatorE(Enum):
    COMPARATOR_UNSPECIFIED = 0
    COMPARATOR_LT = 1
    COMPARATOR_GT = 2
    COMPARATOR_LEQ = 3
    COMPARATOR_GEQ = 4
    COMPARATOR_EQ = 5
    COMPARATOR_NEQ = 6


class TSPercentageChangeComparatorE(Enum):
    PERCENTAGE_CHANGE_COMPARATOR_UNSPECIFIED = 0
    PERCENTAGE_CHANGE_COMPARATOR_INCREASES_BY = 1
    PERCENTAGE_CHANGE_COMPARATOR_DECREASES_BY = 2
    PERCENTAGE_CHANGE_COMPARATOR_CHANGES_BY = 3


class TSFormatConfigCategoryE(Enum):
    NUMBER = 1
    PERCENTAGE = 2
    CURRENCY = 3
    CUSTOM = 4


class TSFormatConfigUnitE(Enum):
    NONE = 1
    THOUSANDS = 2
    MILLION = 3
    BILLION = 4
    TRILLION = 5
    AUTO = 6


class TSFormatConfigNegativeValueFormatE(Enum):
    PREFIX_DASH = 1
    SUFFIX_DASH = 2
    BRACES_NODASH = 3


class TSGeometryE(Enum):
    POINT = 0
    LINE_STRING = 1
    LINEAR_RING = 2
    POLYGON = 3
    MULTI_POINT = 4
    MULTI_LINE_STRING = 5
    MULTI_POLYGON = 6
    GEOMETRY_COLLECTION = 7
    CIRCLE = 8


class TSActionContextE(Enum):
    NONE = 0
    PRIMARY = 1
    MENU = 2
    CONTEXT_MENU = 3


class TSActionObjectApplicationE(Enum):
    NONE = 0
    SLACK = 1
    SALESFORCE = 2
    GOOGLE_SHEET = 3


class TSFrequencySpecFrequencyGranularityE(Enum):
    EVERY_MINUTE = 0
    HOURLY = 1
    DAILY = 2
    WEEKLY = 3
    MONTHLY = 4


class TSActionE(Enum):
    CALLBACK = 1
    URL = 2


class TSAvailabilityContentE(Enum):
    GLOBAL = 0
    LOCAL = 1


class TSUrlActionDetailsAuthenticationE(Enum):
    NONE = 0
    BASIC = 1
    BEARER = 2
    API_KEY = 3
