from src.config import REDSHIFT_ETL_URI, WRITE_DEV_DB_URI, WRITE_ETL_DB_URI, TEST, MONGO_ROUTES_URI, \
    REDSHIFT_S3_REGION, REDSHIFT_S3_BUCKET, REDSHIFT_IAM_ROLE
from sqlalchemy import create_engine
from pymongo import MongoClient
from pyathena import connect, __version__
import ssl
print('ATHENA Version:', __version__)

try:
    REDSHIFT_ETL = create_engine(REDSHIFT_ETL_URI)
except:
    REDSHIFT_ETL = None

try:
    if not config.TEST:
        ATHENA = connect(s3_staging_dir=config.S3_STAGING_DIR, region_name='eu-west-1')
    else:
        ATHENA = connect(s3_staging_dir=config.S3_STAGING_DIR, profile_name='data-prod')
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
