from dotenv import dotenv_values
import logging

logging.config.fileConfig('logging.conf', disable_existing_loggers=False)

# Loads environment variables defined in .env file.
_env = dotenv_values(".env")
USERNAME = _env["DB_USERNAME"]
PASSWORD = _env["DB_PASSWORD"]
HOSTNAME = _env["DB_HOSTNAME"]
DATABASE = _env["DB_NAME"]
DB_PORT = _env["DB_PORT"]

# Dataset constants
DATASET_FILENAME = "archive.zip"
DATASET_URL = "https://storage.googleapis.com/kaggle-data-sets/199387/3286750/bundle/archive.zip?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=gcp-kaggle-com%40kaggle-161607.iam.gserviceaccount.com%2F20230420%2Fauto%2Fstorage%2Fgoog4_request&X-Goog-Date=20230420T003904Z&X-Goog-Expires=259200&X-Goog-SignedHeaders=host&X-Goog-Signature=98ab2788aa35a2ed2097e55ddf442a10d7d726901988f9aa940e3f36f77c57c886c3c2d2a7c4ba68254713812fd665af8e8ac32de449e9970212937ed0d21eb0c28ad5b29bb30ffa887039c34a2d353ea07d3c77c15d1252548dce40b5c8c46837d08323f91b426ffd4f86c5ec19873cc98e90fd4281909c5cfbca3b02810d16d0586391441ce1a0bae7d12985761d5b4462fc51795f2770371b1eb04a145cb9ec8197db4b5510585fa5e8a934c83883bfeff341c610e30cae44db400d1eb4611eded297bdcf921c7656cb65ff6c82d9649c7d5777e92ce9a759b1e58a83bfd39746f3f5adc89652c63154836289163ca34f91d10c8ef62919559af53e72494a"
DATASET_SIZE = 2845342
TARGET_SIZE = 110000
CHUNK_SIZE = 1000

# All dataset columns
ALL_COLUMNS = """
ID
Severity
Start_Time
End_Time
Start_Lat
Start_Lng
End_Lat
End_Lng
Distance
Description
Number
Street
Side
City
County
State
Zipcode
Country
Timezone
Airport_Code
Weather_Timestamp
Temperature
Wind_Chill
Humidity
Pressure
Visibility
Wind_Direction
Wind_Speed
Precipitation
Weather_Condition
Amenity
Bump
Crossing
Give_Way
Junction
No_Exit
Railway
Roundabout
Station
Stop
Traffic_Calming
Traffic_Signal
Turning_Loop
Sunrise_Sunset
Civil_Twilight
Nautical_Twilight
Astronomical_Twilight
""".strip().split("\n")

FILTERED = { "Description" } # Useless dataset columns
COLUMNS = [column for column in ALL_COLUMNS if column not in FILTERED] # Usefull dataset columns

# Columns to include in all queries
DEFAULT_COLUMNS = {
    "ID",
    "Start_Time",
    "End_Time",
    "Start_Lat",
    "Start_Lng"
}

STATES = """
AK
AL
AZ
AR
CA
CO
CT
DE
FL
GA
HI
ID
IL
IN
IA
KS
KY
LA
ME
MD
MA
MI
MN
MS
MO
MT
NE
NV
NH
NJ
NM
NY
NC
ND
OH
OK
OR
PA
RI
SC
SD
TN
TX
UT
VT
VA
WA
WV
WI
WY
AS
DC
FM
GU
MH
MP
PW
PR
VI
""".strip().split("\n")