from src.config import REDSHIFT_ETL_URI, WRITE_DEV_DB_URI, WRITE_ETL_DB_URI, TEST, MONGO_CLIENT_URI
from sqlalchemy import create_engine
from pymongo import MongoClient
import ssl

try:
    REDSHIFT_ETL = create_engine(REDSHIFT_ETL_URI)
except:
    REDSHIFT_ETL = None

try:
    MONGO_CLIENT = MongoClient(MONGO_CLIENT_URI)
except:
    MONGO_CLIENT = None
try:
    ROUTES_COLLECTION = MONGO_CLIENT.getir.routes
except:
    ROUTES_COLLECTION = None

try:
    if TEST:
        WRITE_ENGINE = create_engine(WRITE_DEV_DB_URI)
    else:
        WRITE_ENGINE = REDSHIFT_ETL
except Exception as e:
    print(e)
    WRITE_ENGINE = None