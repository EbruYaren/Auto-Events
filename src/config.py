import os

TEST = True

REDSHIFT_ETL_URI = os.environ.get('REDSHIFT_ETL_URI')
MONGO_ENGINE = os.environ.get('MONGO_URI')

WRITE_TABLE_NAME = "reach_date_prediction"
SCHEMA_NAME = "public" if TEST else "project_auto_events"


test_pickle_file = "rick.pickle"
chunk_size = 100
intercept = 0.414
coefficients = [-0.815, 0.407]