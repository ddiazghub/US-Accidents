from model import Accident
from typing import Annotated
from fastapi import FastAPI
from fastapi.params import Query
from fastapi.staticfiles import StaticFiles
from contextlib import asynccontextmanager
from config import DEFAULT_COLUMNS
from db.import_db import create_database
from datetime import datetime
from db.query import accidents, accidents_until, accidents_since, accidents_between
from docs import API_DESCRIPTION, END_DESCRIPTION, FIELDS_DESCRIPTION, START_DESCRIPTION

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

"""Query accidents"""
@app.get("/accidents", tags=["accidents"])
async def read_item(fields: Annotated[list[str], Query(description=FIELDS_DESCRIPTION)] = [], start: Annotated[datetime | None, Query(description=START_DESCRIPTION)] = None, end: Annotated[datetime | None, Query(description=END_DESCRIPTION)] = None) -> list[Accident]:
    fields = DEFAULT_COLUMNS.union(fields)
    
    match (start, end):
        case (None, None):
            return accidents(fields)
        case (_, None):
            return accidents_since(fields, start)
        case (None, _):
            return accidents_until(fields, end)
        case _:
            return accidents_between(fields, start, end)

app.mount("/", StaticFiles(directory="static", html=True), name="static")