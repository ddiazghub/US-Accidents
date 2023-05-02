from model import Accident
from typing import Sequence
from psycopg.rows import dict_row
from psycopg.sql import Composed, Identifier, SQL
from db.connect import connect
from datetime import datetime
from config import DATABASE, DEFAULT_COLUMNS

def build_query(fields: set[str], where_clause: str | None = None) -> Composed:
    """
    Builds a query using the given fields (database columns) and where clause.
    
    :param fields: Database columns to fetch with the query.
    :param where_clause: Query's where clause to filter rows.
    """

    query = f"""
    WITH "base_query" AS (
        SELECT {{}}
        FROM "Accidents"
        {f"WHERE {where_clause}" if where_clause else ""}
    ), "resolution"("value") AS (
        SELECT GREATEST(1, COUNT(*) / (%s))::INTEGER
        FROM base_query
    )
    SELECT "base_query".*
    FROM "base_query", "resolution"
    WHERE "base_query"."ID" %% "resolution"."value" = 0
    LIMIT (%s)
    """

    return SQL(query).format(SQL(',').join(Identifier(column) for column in fields))

def all_accidents(fields: set[str], where_clause: str | None = None, params: Sequence | None = None) -> list[Accident]:
    """
    Queries the accidents table.
    
    :param fields: Database columns to fetch with the query.
    :param where_clause: Query's where clause to filter rows.
    :param params: Values used to query.
    """

    with connect(DATABASE, row_factory=dict_row) as connection:
        query = build_query(fields, where_clause)

        return connection.execute(query, params).fetchall()

def accidents(fields: set[str], start: datetime | None = None, end: datetime | None = None, limit: int = 100) -> list[Accident]:
    """
    Queries the accidents table.
    
    :param fields: Database columns to fetch with the query.
    :param where_clause: Query's where clause to filter rows.
    :param limit: Max number of records to fetch.
    """
    fields = DEFAULT_COLUMNS.union(fields)
    
    match (start, end):
        case (None, None):
            return all_accidents(fields, params=(limit, limit))
        case (_, None):
            return accidents_since(fields, start, limit)
        case (None, _):
            return accidents_until(fields, end, limit)
        case _:
            return accidents_between(fields, start, end, limit)

def accidents_since(fields: set[str], start: datetime, limit: int = 100) -> list[Accident]:
    """
    Queries accidents since a given timestamp.
    
    :param fields: Database columns to fetch with the query.
    :param start: Initial timestamp. Accidents happening after this timestamp will be fetched.
    :param limit: Max number of records to fetch.
    """

    return all_accidents(fields, where_clause="\"End_Time\" > (%s)", params=(start, limit, limit))

def accidents_until(fields: set[str], end: datetime, limit: int = 100) -> list[Accident]:
    """
    Queries accidents until a given timestamp.
    
    :param fields: Database columns to fetch with the query.
    :param end: Ending timestamp. Accidents happening before this timestamp will be fetched.
    :param limit: Max number of records to fetch.
    """

    return all_accidents(fields, where_clause="\"Start_Time\" < (%s)", params=(end, limit, limit))

def accidents_between(fields: set[str], start: datetime, end: datetime, limit: int = 100) -> list[Accident]:
    """
    Queries accidents in a timestamp range.
    
    :param fields: Database columns to fetch with the query.
    :param start: Initial timestamp. Accidents happening after this timestamp will be fetched.
    :param start: Ending timestamp. Accidents happening before this timestamp will be fetched.
    :param limit: Max number of records to fetch.
    """
    return all_accidents(fields, where_clause="\"End_Time\" > (%s) AND \"Start_Time\" < (%s)", params=(start, end, limit, limit))

def count_by_state(year: int | None) -> list[Accident]:
    """
    Counts the number of accidents in each state.
    
    :param year: Accidents happening in the specified year will be counted.
    """
    query = f"""
    SELECT "State", COUNT(*) AS "AccidentCount"
    FROM "Accidents"
    {'WHERE EXTRACT(YEAR FROM "Start_Time") = (%s)' if year else ""}
    GROUP BY "State"
    """

    with connect(DATABASE, row_factory=dict_row) as connection:
        return connection.execute(*((query, (year,)) if year else (query,))).fetchall()
    
def accident_severities(year: int | None, limit: int) -> list[Accident]:
    """
    Queries accidents severity and location.
    
    :param year: Accidents happening in the specified year will be queried.
    :param limit: Max number of records to fetch.
    """
    query = f"""
    WITH "base_query" AS (
        SELECT "ID", "Severity", "Start_Lat", "Start_Lng"
        FROM "Accidents"
        {'WHERE EXTRACT(YEAR FROM "Start_Time") = (%s)' if year else ""}
    ), "resolution"("value") AS (
        SELECT GREATEST(1, COUNT(*) / (%s))::INTEGER
        FROM base_query
    )
    SELECT "base_query"."Severity", "base_query"."Start_Lat", "base_query"."Start_Lng"
    FROM "base_query", "resolution"
    WHERE "base_query"."ID" %% "resolution"."value" = 0
    LIMIT (%s)
    """

    with connect(DATABASE, row_factory=dict_row) as connection:
        return connection.execute(*(query, (year, limit, limit) if year else (limit, limit))).fetchall()
    
def count_by_weather(year: int | None) -> list[Accident]:
    """
    Counts accidents by weather condition.
    
    :param year: Accidents happening in the specified year will be queried.
    """
    query = f"""
    SELECT "Weather_Condition", COUNT(*) AS "AccidentCount"
    FROM "Accidents"
    {'WHERE EXTRACT(YEAR FROM "Start_Time") = (%s)' if year else ""}
    GROUP BY "Weather_Condition"
    """

    with connect(DATABASE, row_factory=dict_row) as connection:
        return connection.execute(*((query, (year,)) if year else (query,))).fetchall()
    
def count_by_month(start: datetime | None, end: datetime | None) -> list[Accident]:
    """
    Counts the number of accidents happening each month.
    
    :param start: Initial timestamp. Accidents happening after this timestamp will be counted.
    :param end: Ending timestamp. Accidents happening before this timestamp will be counted.
    """
    where_clause = ""
    params: list[str] = []

    match (start, end):
        case (None, None):
            pass
        case (_, None):
            where_clause = "WHERE \"Start_Time\" > (%s)"
            params.append(start)
        case (None, _):
            where_clause = "WHERE \"End_Time\" < (%s)"
            params.append(end)
        case _:
            where_clause = "WHERE \"Start_Time\" > (%s) AND \"End_Time\" < (%s)"
            params.append(start)
            params.append(end)

    query = f"""
    SELECT EXTRACT(MONTH FROM "Start_Time") AS "Month", COUNT(*) AS "AccidentCount"
    FROM "Accidents"
    {where_clause}
    GROUP BY "Month"
    """

    with connect(DATABASE, row_factory=dict_row) as connection:
        return connection.execute(query, params).fetchall()
    
def count_by_day_of_week(start_time: str, end_time: str, start_year: int, end_year: int, months: list[int]) -> list[Accident]:
    """
    Counts the number of accidents happening in a timerange in the given year range.
    
    :param start_time: Initial time. Accidents happening after this time of the day will be counted.
    :param end_time: Ending timestamp. Accidents happening before this time of the day will be counted.
    :param start_year: Initial year. Accidents happening after this year will be counted.
    :param end_year: Ending year. Accidents happening before this year will be counted.
    :param months: Accidents happening in these months will be counted.
    """


    query = f"""
        SELECT EXTRACT(ISODOW FROM "Start_Time") AS "DayOfWeek", COUNT(*) AS "AccidentCount"
        FROM "Accidents"
        WHERE EXTRACT(MONTH FROM "Start_Time") IN ({", ".join(["(%s)" for _ in months])})
        AND EXTRACT(YEAR FROM "Start_Time") BETWEEN (%s) AND (%s)
        AND "Start_Time"::TIME BETWEEN (%s) AND (%s)
        GROUP BY "DayOfWeek"
    """

    with connect(DATABASE, row_factory=dict_row) as connection:
        return connection.execute(query, (*months, start_year, end_year, start_time, end_time)).fetchall()