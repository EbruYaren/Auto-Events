import os

TEST = True

REDSHIFT_ETL_URI = os.environ.get('REDSHIFT_ETL_URI')
MONGO_ENGINE = os.environ.get('MONGO_URI')
ROUTE_OBJECT_COLLETION = None

WRITE_DEV_DB_URI = os.environ.get('WRITE_DEV_DB_URI')
WRITE_ETL_DB_URI = os.environ.get('WRITE_ETL_DB_URI')

WRITE_TABLE_NAME = "reach_date_prediction"
SCHEMA_NAME = "public" if TEST else "project_auto_events"

test_pickle_file = "rick.pickle"
chunk_size = None
intercept = 0.414
coefficients = [-0.815, 0.407]
minimum_location_limit = 5
