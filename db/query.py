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
    :param params: Values used to query.
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
    """

    return all_accidents(fields, where_clause="\"End_Time\" > (%s)", params=(start, limit, limit))

def accidents_until(fields: set[str], end: datetime, limit: int = 100) -> list[Accident]:
    """
    Queries accidents until a given timestamp.
    
    :param fields: Database columns to fetch with the query.
    :param end: Ending timestamp. Accidents happening before this timestamp will be fetched.
    """

    return all_accidents(fields, where_clause="\"Start_Time\" < (%s)", params=(end, limit, limit))

def accidents_between(fields: set[str], start: datetime, end: datetime, limit: int = 100) -> list[Accident]:
    """
    Queries accidents in a timestamp range.
    
    :param fields: Database columns to fetch with the query.
    :param start: Initial timestamp. Accidents happening after this timestamp will be fetched.
    :param start: Ending timestamp. Accidents happening before this timestamp will be fetched.
    """
    return all_accidents(fields, where_clause="\"End_Time\" > (%s) AND \"Start_Time\" < (%s)", params=(start, end, limit, limit))

def accident_count(year: int | None) -> list[Accident]:
    """
    Queries the number of accidents in each state.
    
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