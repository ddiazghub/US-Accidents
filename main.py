import logging
from config import ALL_MONTHS, STATES
from model import Accident
from typing import Annotated
from fastapi import FastAPI
from fastapi.params import Query
from fastapi.staticfiles import StaticFiles
from contextlib import asynccontextmanager
from db.load_db import create_database
from db.query import count_by_day_of_week, count_by_month, count_by_state, accident_severities, accidents, count_by_weather
from docs import API_DESCRIPTION, END_DESCRIPTION, END_TIME_DESCRIPTION, END_YEAR_DESCRIPTION, FIELDS_DESCRIPTION, LIMIT_DESCRIPTION, MONTHS_DESCRIPTION, START_DESCRIPTION, START_TIME_DESCRIPTION, START_YEAR_DESCRIPTION, YEAR_DESCRIPTION

"""Checks if database exists on startup and creates it if doesn't"""
@asynccontextmanager
async def lifespan(app: FastAPI):
    create_database()
    yield
    pass

app = FastAPI(
    lifespan=lifespan,
    title="US Accidents (2016 - 2021)",
    description=API_DESCRIPTION,
)

log = logging.getLogger(__name__)

"""Query accidents"""
@app.get("/accidents", tags=["accidents"])
async def get_accidents(
    fields: Annotated[list[str], Query(description=FIELDS_DESCRIPTION)] = [],
    start: Annotated[str | None, Query(description=START_DESCRIPTION)] = None,
    end: Annotated[str | None, Query(description=END_DESCRIPTION)] = None,
    limit: Annotated[int, Query(description=LIMIT_DESCRIPTION)] = 100
) -> list[Accident]:
    return accidents(fields, start, end, limit)

"""Query accident count per states"""
@app.get("/accidents/count", tags=["accidents"])
async def get_accident_count(
    year: Annotated[int | None, Query(description=YEAR_DESCRIPTION)] = None
) -> list[Accident]:
    counts = count_by_state(year)
    states_with_accidents = {count["State"] for count in counts}

    for state in STATES:
        if state not in states_with_accidents:
            counts.append({
                "State": state,
                "AccidentCount": 0
            })
    
    return counts

"""Query accident severities"""
@app.get("/accidents/severities", tags=["accidents"])
async def get_severities(
    year: Annotated[int | None, Query(description=YEAR_DESCRIPTION)] = None,
    limit: Annotated[int, Query(description=LIMIT_DESCRIPTION)] = 1000
) -> list[Accident]:
    return accident_severities(year, limit)

"""Count accidents by weather condition"""
@app.get("/accidents/count/by/weather", tags=["accidents"])
async def get_count_by_weather(
    year: Annotated[int | None, Query(description=YEAR_DESCRIPTION)] = None
) -> list[Accident]:
    return count_by_weather(year)

"""Count accidents by month"""
@app.get("/accidents/count/by/month", tags=["accidents"])
async def get_count_by_month(
    start: Annotated[str | None, Query(description=START_DESCRIPTION)] = None,
    end: Annotated[str | None, Query(description=END_DESCRIPTION)] = None
) -> list[Accident]:
    return count_by_month(start, end)

"""Count accidents by day of week"""
@app.get("/accidents/count/by/dow", tags=["accidents"])
async def get_count_by_dow(
    start_year: Annotated[int | None, Query(description=START_YEAR_DESCRIPTION)] = None,
    end_year: Annotated[int | None, Query(description=END_YEAR_DESCRIPTION)] = None,
    start_time: Annotated[str | None, Query(description=START_TIME_DESCRIPTION)] = None,
    end_time: Annotated[str | None, Query(description=END_TIME_DESCRIPTION)] = None,
    months: Annotated[list[int], Query(description=MONTHS_DESCRIPTION)] = ALL_MONTHS
) -> list[Accident]:
    return count_by_day_of_week(start_time, end_time, start_year, end_year, months)

#app.mount("/", StaticFiles(directory="static", html=True), name="static")