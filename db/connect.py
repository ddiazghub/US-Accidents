import psycopg
from psycopg.connection import Connection
from psycopg.rows import Row, RowFactory
from config import DB_PORT, USERNAME, HOSTNAME, PASSWORD

def connect(database: str, row_factory: RowFactory[Row] | None = None, autocommit: bool = False) -> Connection:
    """
    Establishes a connection to the database, using the environment configuration.

    :param database: Database name.
    :param row_factory: dunno
    :param autocommit: nope
    """
    return psycopg.connect(f"host={HOSTNAME} user={USERNAME} port={DB_PORT} password={PASSWORD} dbname={database} connect_timeout=10", row_factory=row_factory, autocommit=autocommit)