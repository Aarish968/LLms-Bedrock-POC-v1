from sqlalchemy import table
from sqlalchemy.sql import literal_column as col

"""
This table is used to get the engagement number from the engagement id.
"""

TABLE_HDR = table(
    "SERVICES_DB.SERVICES_ENT_FBV.BV_MCE_AM_ENGAGEMENT_HDR",
    col("engagement_number"),
    col("engagement_id"),
)
