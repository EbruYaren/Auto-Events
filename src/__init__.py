from src.config import REDSHIFT_ETL_URI, WRITE_DEV_DB_URI, WRITE_ETL_DB_URI, TEST, MONGO_CLIENT_URI
from sqlalchemy import create_engine
from pymongo import MongoClient


REDSHIFT_ETL = create_engine(REDSHIFT_ETL_URI)
MONGO_CLIENT = MongoClient(MONGO_CLIENT_URI)
ROUTES_COLLECTION = MONGO_CLIENT.getir.routes

if TEST:
    WRITE_ENGINE = create_engine(WRITE_DEV_DB_URI)
else:
    WRITE_ENGINE = create_engine(WRITE_ETL_DB_URI)