import pandas as pd
import numpy as np

from psycopg.cursor import Cursor
from psycopg.sql import Identifier, SQL
from config import ALL_COLUMNS, CHUNK_SIZE, DATASET_FILENAME, DATASET_SIZE, DATASET_URL, DATABASE, COLUMNS, TARGET_SIZE
from db.record_array import RecordArray
from db.connect import connect

import os
import sys
import logging

COLS = [column for column in COLUMNS if column != "ID"]
COPY_QUERY = SQL("COPY \"Accidents\" ({}) FROM STDIN").format(SQL(', ').join(Identifier(column) for column in COLS))
log = logging.getLogger(__name__)

def create_database():
    """Automatically creates the database if it doesn't exist. The dataset is then loaded into the database."""

    # Creates database
    with connect("postgres", autocommit=True) as connection:
        with connection.cursor() as cursor:
            if cursor.execute(f"SELECT datname FROM pg_catalog.pg_database WHERE datname = '{DATABASE}'").fetchone():
                log.info("The database already exists. Skipping creation.")
            else:
                log.info("Creating database.")
                cursor.execute(f"CREATE DATABASE \"{DATABASE}\"")

    old_version = False

    # Creates schema and loads data
    with connect(DATABASE) as connection, connection.cursor() as cursor:
        if not cursor.execute("SELECT * FROM pg_tables WHERE schemaname = 'public' AND tablename = 'Accidents'").fetchone():
            if not os.path.exists(DATASET_FILENAME):
                log.critical("Dataset not found. Please download the dataset to the projects directory and name it \"archive.zip\".")
                sys.exit(1)
            
            log.info("Creating table schema.")

            with open("schema.sql") as schema:
                cursor.execute(schema.read())

            read_csv(DATASET_FILENAME, cursor)
            connection.commit()

        db_size = cursor.execute("SELECT COUNT(*) FROM \"Accidents\"").fetchone()[0]
        old_version = db_size > DATASET_SIZE + CHUNK_SIZE or db_size < DATASET_SIZE - CHUNK_SIZE

    if old_version:
        with connect("postgres", autocommit=True) as connection:
            connection.execute(f"DROP DATABASE \"{DATABASE}\"")

        log.info("Outdated database found. Creating database with updated data.")
        create_database()

def read_csv(filename: str, cursor: Cursor):
    """
    Reads a stream of CSV data while loading it into the database.
    
    :param filename: Name of the zipped csv file.
    :param cursor: Database cursor for writing data.
    """
    log.info("Loading CSV into database.")
    progress = 0

    with cursor.copy(COPY_QUERY) as copy:
        for chunk in pd.read_csv(filename, compression="zip", skiprows=1, usecols=COLS, names=ALL_COLUMNS, sep=",", quotechar="\"", chunksize=CHUNK_SIZE):
            for record in chunk.replace({np.nan: None}).values:
                copy.write_row(RecordArray(record))

            progress += CHUNK_SIZE
            percent = progress * 100 / DATASET_SIZE
            print(f"[{'#' * int(percent)}{' ' * int(100 - percent)}] {round(percent, 1)}%", end='\r')

        print()
        log.info("Finished populating database")

if __name__ == "__main__":
    create_database()