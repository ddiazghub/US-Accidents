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
YEAR_DESCRIPTION = "Year. Query records happening on the specified year."
START_YEAR_DESCRIPTION = "Start year. Query records happening from the specified year."
END_YEAR_DESCRIPTION = "Start year. Query records happening until the specified year."
START_TIME_DESCRIPTION = "Start time. Query records happening from the specified time of the day."
END_TIME_DESCRIPTION = "End time. Query records happening until the specified time of the day."
MONTHS_DESCRIPTION = "Months. Accidents happening in the selected months will be queried"