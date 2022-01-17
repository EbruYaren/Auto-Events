from src.config import REDSHIFT_ETL_URI, WRITE_DEV_DB_URI, WRITE_ETL_DB_URI, TEST, MONGO_ROUTES_URI, ENGINE_BITEST_URI
from sqlalchemy import create_engine
from pymongo import MongoClient
from pyathena import connect
import ssl

try:
    REDSHIFT_ETL = create_engine(REDSHIFT_ETL_URI)
except:
    REDSHIFT_ETL = None

try:
    ATHENA = connect(s3_staging_dir="s3://aws-athena-query-result-164762854291-eu-west-1/", profile_name="data-prod")
except:
    ATHENA = None

try:
    MONGO_ROUTES = MongoClient(MONGO_ROUTES_URI, ssl_cert_reqs=ssl.CERT_NONE)
except:
    MONGO_ROUTES = None
try:
    ROUTES_COLLECTION = MONGO_ROUTES.get_database().routes
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

try:
    if config.TEST:
        ENGINE_BITEST = create_engine(ENGINE_BITEST_URI)
    else:
        ENGINE_BITEST = None
except:
    ENGINE_BITEST = None
