from config import COLUMNS

API_DESCRIPTION = """
API for querying a dataset composed of accident records.
Source: [https://www.kaggle.com/datasets/sobhanmoosavi/us-accidents](https://www.kaggle.com/datasets/sobhanmoosavi/us-accidents).
"""

API_TAGS = [{
    "name": "accidents",
    "description": "Query accident records."
}]

FIELDS_DESCRIPTION = f"Data fields to query. Allowed values: [{', '.join(COLUMNS)}]"
START_DESCRIPTION = "Initial timestamp. Accidents happening after this timestamp will be fetched."
END_DESCRIPTION = "Ending timestamp. Accidents happening before this timestamp will be fetched."
LIMIT_DESCRIPTION = "Limit. Max number of records to fetch."
YEAR_DESCRIPTION = "Year. Count records happening on the specified year."