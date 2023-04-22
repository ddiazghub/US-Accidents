from model import Accident
from typing import Sequence
from psycopg.rows import dict_row
from psycopg.sql import Composed, Identifier, SQL
from db.connect import connect
from datetime import datetime
from config import DATABASE

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
        {f'WHERE {where_clause}' if where_clause else ""}
    ), "resolution"("value") AS (
        SELECT GREATEST(1, COUNT(*) / 1000)
        FROM base_query
    )
    SELECT "base_query".*
    FROM "base_query", "resolution"
    WHERE "base_query"."ID" % "resolution"."value" = 0
    """

    return SQL(query).format(SQL(',').join(Identifier(column) for column in fields))

def accidents(fields: set[str], where_clause: str | None = None, params: Sequence | None = None) -> list[Accident]:
    """
    Queries the accidents table.
    
    :param fields: Database columns to fetch with the query.
    :param where_clause: Query's where clause to filter rows.
    :param params: Values used to query.
    """

    with connect(DATABASE, row_factory=dict_row) as connection:
        query = build_query(fields, where_clause)

        return connection.execute(query, params).fetchall()

def accidents_since(fields: set[str], start: datetime) -> list[Accident]:
    """
    Queries accidents since a given timestamp.
    
    :param fields: Database columns to fetch with the query.
    :param start: Initial timestamp. Accidents happening after this timestamp will be fetched.
    """

    return accidents(fields, where_clause="\"End_Time\" > (%s)", params=(start))

def accidents_until(fields: set[str], end: datetime) -> list[Accident]:
    """
    Queries accidents until a given timestamp.
    
    :param fields: Database columns to fetch with the query.
    :param end: Ending timestamp. Accidents happening before this timestamp will be fetched.
    """

    return accidents(fields, where_clause="\"Start_Time\" < (%s)", params=(end))

def accidents_between(fields: set[str], start: datetime, end: datetime) -> list[Accident]:
    """
    Queries accidents in a timestamp range.
    
    :param fields: Database columns to fetch with the query.
    :param start: Initial timestamp. Accidents happening after this timestamp will be fetched.
    :param start: Ending timestamp. Accidents happening before this timestamp will be fetched."
    """
    return accidents(fields, where_clause="\"End_Time\" > (%s) AND \"Start_Time\" < (%s)", params=(start, end))