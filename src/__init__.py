from src.config import REDSHIFT_ETL_URI, WRITE_DEV_DB_URI, WRITE_ETL_DB_URI, TEST
from sqlalchemy import create_engine

REDSHIFT_ETL = create_engine(REDSHIFT_ETL_URI)
if TEST:
    WRITE_ENGINE = create_engine(WRITE_DEV_DB_URI)
else:
    WRITE_ENGINE = create_engine(WRITE_ETL_DB_URI)