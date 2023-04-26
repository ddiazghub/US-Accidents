import logging
from model import Accident
from typing import Annotated
from fastapi import FastAPI
from fastapi.params import Query
from fastapi.staticfiles import StaticFiles
from contextlib import asynccontextmanager
from db.load_db import create_database
from db.query import accidents
from docs import API_DESCRIPTION, END_DESCRIPTION, FIELDS_DESCRIPTION, LIMIT_DESCRIPTION, START_DESCRIPTION

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
async def read_item(fields: Annotated[list[str], Query(description=FIELDS_DESCRIPTION)] = [], start: Annotated[str | None, Query(description=START_DESCRIPTION)] = None, end: Annotated[str | None, Query(description=END_DESCRIPTION)] = None, limit: Annotated[int, Query(description=LIMIT_DESCRIPTION)] = 100) -> list[Accident]:
    #log.info("Range - Start:", start, "- End:", end)

    return accidents(fields, start, end, limit)

app.mount("/", StaticFiles(directory="static", html=True), name="static")