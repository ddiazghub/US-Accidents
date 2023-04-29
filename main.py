import logging
from config import STATES
from model import Accident
from typing import Annotated
from fastapi import FastAPI
from fastapi.params import Query
from fastapi.staticfiles import StaticFiles
from contextlib import asynccontextmanager
from db.load_db import create_database
from db.query import accident_count, accident_severities, accidents, count_by_weather
from docs import API_DESCRIPTION, END_DESCRIPTION, FIELDS_DESCRIPTION, LIMIT_DESCRIPTION, START_DESCRIPTION, YEAR_DESCRIPTION

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
async def get_accidents(fields: Annotated[list[str], Query(description=FIELDS_DESCRIPTION)] = [], start: Annotated[str | None, Query(description=START_DESCRIPTION)] = None, end: Annotated[str | None, Query(description=END_DESCRIPTION)] = None, limit: Annotated[int, Query(description=LIMIT_DESCRIPTION)] = 100) -> list[Accident]:
    return accidents(fields, start, end, limit)

"""Query accident count per states"""
@app.get("/accidents/count", tags=["accidents"])
async def get_accident_count(year: Annotated[int | None, Query(description=YEAR_DESCRIPTION)] = None) -> list[Accident]:
    counts = accident_count(year)
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
async def get_severities(year: Annotated[int | None, Query(description=YEAR_DESCRIPTION)] = None, limit: Annotated[int, Query(description=LIMIT_DESCRIPTION)] = 1000) -> list[Accident]:
    return accident_severities(year, limit)

"""Count accidents by weather condition"""
@app.get("/accidents/count/by/weather", tags=["accidents"])
async def get_count_by_weather(year: Annotated[int | None, Query(description=YEAR_DESCRIPTION)] = None) -> list[Accident]:
    return count_by_weather(year)

app.mount("/", StaticFiles(directory="static", html=True), name="static")