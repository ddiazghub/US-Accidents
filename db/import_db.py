import pandas as pd
import numpy as np

from psycopg.cursor import Cursor
from psycopg.sql import Identifier, SQL
from config import ALL_COLUMNS, CHUNK_SIZE, DATASET_FILENAME, DATASET_SIZE, DATASET_URL, DATABASE, COLUMNS
from db.record_array import RecordArray
from db.connect import connect

import os
import sys
import logging

COPY_QUERY = SQL("COPY \"Accidents\" ({}) FROM STDIN").format(SQL(', ').join(Identifier(column) for column in COLUMNS))
log = logging.getLogger(__name__)

def download_dataset(url: str, filename: str):
    """
    Downloads a dataset from the internet using the cURL command.
    
    :param url: The dataset's URL.
    :param filename: Name of the file where the dataset will be saved.
    """
    log.info(f"Downloading dataset.")

    if os.system(f"curl -o \"{filename}\" \"{url}\"") == 0:
        log.info("Dataset downloaded successfully.")
    else:
        log.critical("Failed to download dataset.")
        sys.exit(1)

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

    # Creates schema and loads data
    with connect(DATABASE) as connection:
        with connection.cursor() as cursor:
            if not cursor.execute("SELECT * FROM pg_tables WHERE schemaname = 'public' AND tablename = 'Accidents'").fetchone():
                if not os.path.exists(DATASET_FILENAME):
                    download_dataset(DATASET_URL, DATASET_FILENAME)
                
                log.info("Creating table schema.")

                with open("schema.sql") as schema:
                    cursor.execute(schema.read())

                read_csv(DATASET_FILENAME, cursor)
                connection.commit()

def read_csv(filename: str, cursor: Cursor):
    """
    Reads a stream of CSV data while loading it into the database.
    
    :param filename: Name of the zipped csv file.
    :param cursor: Database cursor for writing data.
    """
    log.info("Loading CSV into database.")
    progress = 0

    with cursor.copy(COPY_QUERY) as copy:
        for chunk in pd.read_csv(filename, compression="zip", skiprows=1, usecols=COLUMNS, names=ALL_COLUMNS, sep=",", quotechar="\"", chunksize=CHUNK_SIZE):
            for record in chunk.replace({np.nan: None}).values:
                copy.write_row(RecordArray(record))

            progress += CHUNK_SIZE
            percent = progress * 100 / DATASET_SIZE
            print(f"[{'#' * int(percent)}{' ' * int(100 - percent)}] {round(percent, 1)}%", end='\r')

        print()
        log.info("Finished populating database")

if __name__ == "__main__":
    create_database()