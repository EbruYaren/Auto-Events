import os
from datetime import timedelta
TEST = False
REACH_CREATE_TABLE = False
DEPART_CREATE_TABLE = False
DEPART_FROM_WAREHOUSE_NEW_MODEL_TABLE = True
DEPART_FROM_CLIENT_CREATE_TABLE = False
REACH_TO_SHOP_TABLE = False
REACH_TO_RESTAURANT_TABLE = False
DELIVERY_CREATE_TABLE = False
ARTISAN_DEPART_CREATE_TABLE = False
FOOD_DEPART_CREATE_TABLE = False
ARTISAN_DEPART_FROM_MERCHANT_CREATE_TABLE = False
FOOD_DEPART_FROM_MERCHANT_CREATE_TABLE = False

FOOD_REACH_TO_CLIENT_TABLE = False
ARTISAN_REACH_TO_CLIENT_TABLE = False

WATER_REACH_CREATE_TABLE = False
WATER_DEPART_CREATE_TABLE = False
WATER_DELIVERY_CREATE_TABLE = False
WATER_DEPART_FROM_CLIENT_CREATE_TABLE = False

REDSHIFT_ETL_URI = os.environ.get('REDSHIFT_ETL_URI')
MONGO_ROUTES_URI = os.environ.get('MONGO_ROUTES_URI')
ROUTE_OBJECT_COLLETION = None
S3_STAGING_DIR = os.environ.get('S3_STAGING_DIR', 's3://aws-athena-query-result-164762854291-eu-west-1/')

REDSHIFT_S3_BUCKET = os.getenv('REDSHIFT_S3_BUCKET', 'getir-data-redshift-temp-files')
REDSHIFT_S3_REGION = os.getenv('REDSHIFT_S3_REGION', 'eu-west-1')
REDSHIFT_IAM_ROLE = os.getenv('REDSHIFT_IAM_ROLE', 'arn:aws:iam::164762854291:role/data-cron-temp-files-role')

WRITE_DEV_DB_URI = os.environ.get('DEV_DB_URI')
WRITE_ETL_DB_URI = os.environ.get('WRITE_ETL_DB_URI')

REACH_TABLE_NAME = "reach_date_prediction"
REACH_TABLE_COLUMNS = ['order_id',
                     'predicted_reach_date',
                     'predicted_reach_dateL',
                     'latitude', 'longitude']

REACH_TO_SHOP_TABLE_NAME = "reach_to_shop_date_prediction"
REACH_TO_SHOP_TABLE_COLUMNS = ['order_id',
                               'predicted_reach_date',
                               'predicted_reach_dateL',
                               'latitude', 'longitude']

REACH_TO_RESTAURANT_TABLE_NAME = "reach_to_restaurant_date_prediction"
REACH_TO_RESTAURANT_TABLE_COLUMNS = ['order_id',
                                     'predicted_reach_date',
                                     'predicted_reach_dateL',
                                     'latitude', 'longitude']

FOOD_REACH_TO_CLIENT_TABLE_NAME = "food_reach_date_prediction"
FOOD_REACH_TO_CLIENT_TABLE_COLUMNS = ['order_id',
                                      'predicted_depart_date',
                                      'predicted_depart_dateL',
                                      'latitude', 'longitude']
ARTISAN_REACH_TO_CLIENT_TABLE_NAME = "artisan_reach_date_prediction"
ARTISAN_REACH_TO_CLIENT_TABLE_COLUMNS = ['order_id',
                                         'predicted_depart_date',
                                         'predicted_depart_dateL',
                                         'latitude', 'longitude']

DEPART_TABLE_NAME = "depart_date_prediction"
DEPART_TABLE_COLUMNS = ['order_id',
                     'predicted_depart_date',
                     'predicted_depart_dateL',
                     'latitude', 'longitude']


DEPART_FROM_WAREHOUSE_NEW_MODEL_TABLE_NAME = "depart_date_prediction_new_model"
DEPART_FROM_WAREHOUSE_NEW_MODEL_TABLE_COLUMNS = ['order_id', 'domain_type',
                                                 'predicted_depart_date',
                                                 'predicted_depart_dateL',
                                                 'latitude', 'longitude']

FOOD_DEPART_TABLE_NAME = "food_depart_date_prediction"
FOOD_DEPART_TABLE_COLUMNS = ['order_id',
                     'predicted_depart_date',
                     'predicted_depart_dateL',
                     'latitude', 'longitude']

ARTISAN_DEPART_TABLE_NAME = "artisan_depart_date_prediction"
ARTISAN_DEPART_TABLE_COLUMNS = ['order_id',
                     'predicted_depart_date',
                     'predicted_depart_dateL',
                     'latitude', 'longitude']

FOOD_DEPART_FROM_MERCHANT_TABLE_NAME = "food_depart_from_merchant_date_prediction"
FOOD_DEPART_FROM_MERCHANT_TABLE_COLUMNS = ['order_id',
                                           'predicted_depart_date',
                                           'predicted_depart_dateL',
                                           'latitude', 'longitude']

WATER_DEPART_TABLE_NAME = "water_depart_date_prediction"
WATER_DEPART_TABLE_COLUMNS = ['order_id',
                              'predicted_depart_date',
                              'predicted_depart_dateL',
                              'latitude', 'longitude']

WATER_REACH_TABLE_NAME = "water_reach_date_prediction"
WATER_REACH_TABLE_COLUMNS = ['order_id',
                             'predicted_reach_date',
                             'predicted_reach_dateL',
                             'latitude', 'longitude']

ARTISAN_DEPART_FROM_MERCHANT_TABLE_NAME = "artisan_depart_from_merchant_date_prediction"
ARTISAN_DEPART_FROM_MERCHANT_TABLE_COLUMNS = ['order_id',
                     'predicted_depart_date',
                     'predicted_depart_dateL',
                     'latitude', 'longitude']

WATER_DEPART_FROM_CLIENT_TABLE_NAME = "water_depart_from_client_date_prediction"
WATER_DEPART_FROM_CLIENT_TABLE_COLUMNS = ['order_id',
                                          'predicted_depart_from_client_date',
                                          'predicted_depart_from_client_dateL',
                                          'latitude', 'longitude']

WATER_DELIVERY_TABLE_NAME = "water_delivery_date_prediction"
WATER_DELIVERY_TABLE_COLUMNS = ['order_id',
                                'predicted_delivery_date',
                                'predicted_delivery_dateL',
                                'latitude', 'longitude']

DEPART_FROM_CLIENT_TABLE_NAME = "depart_from_client_date_prediction"
DEPART_FROM_CLIENT_TABLE_COLUMNS = ['order_id',
                                    'predicted_depart_from_client_date',
                                    'predicted_depart_from_client_dateL',
                                    'latitude', 'longitude']

DELIVERY_TABLE_NAME = "delivery_date_prediction"
DELIVERY_TABLE_COLUMNS = ['order_id',
                          'predicted_delivery_date',
                          'predicted_delivery_dateL',
                          'latitude', 'longitude']

SCHEMA_NAME = "public" if TEST else "project_auto_events"
DB_USER_GROUP = "data_rw" if TEST else "data_general_ro"

test_pickle_file = "rick.pickle"
chunk_size = 3000

REACH_INTERCEPT = -0.414
REACH_COEFFICIENTS = [-0.815, 0.407]
DEPART_INTERCEPT = -5.234
DEPART_COEFFICIENTS = [0.557, 0.047, 1.165]
DEPART_FROM_CLIENT_INTERCEPT = -5.234
DEPART_FROM_CLIENT_COEFFICIENTS = [0.557, 0.047, 1.165]

MINIMUM_LOCATION_LIMIT = 3
FIBONACCI_BASE = 50
MAX_DISTANCE_FOR_DEPART_PREDICTION = 150
MAX_DISTANCE_FOR_DEPART_FROM_CLIENT_PREDICTION = 150

COURIER_IDS = ['59421963381aa80004bd893f', '5ddf736d8d0272e6b8f11ada', '5c0aa39b0164a964a487f52f', '5c0a9625a7f4932a68d0b5b9', '5e99d909601ad4741e69db1d', '5e2edd0c295a7519a6c05feb', '5dd54b912030422198d007ce', '5e12bfb9a12a543a55069029', '5ec952faf2ba0e798806f7d5', '5cfb7a5b6d84890001b16572', '5e9e302617599b2cb1a59086', '5e9e4f81e4d7762d7803c89a', '5e8b9ec8eb09c74a24cd1352', '5e99d9938df893363250d116', '599d3b6f1138d800045348a4', '5e99d90fed8d711a159e7b55', '5932be40f184d90004323395', '5e9e4fb9b0e668b3a7eb1b40', '5e9e2dd617599b9144a5902c', '5e9e4fdd200ada31fb4a9dd2', '5e3adcc9a31b286fe3d35e7c', '5dc500627aa58cc172e35830', '587b88d6d61ce20004e75a31', '5df1612281efff2b5b692269', '5dbd88f656892f0b727d7b92', '586f909fab7cea0004622bc8', '5d36a759dee74e000188f63e', '5e9e3e6b200ada68504a9c14', '5e9e3083752e02eea3626596', '5997c83ebd8c5d0004d2059e', '5e0d768e117b3e6aa25db2b2', '5c0aa38d90a00c64d76c3064', '5e714a717e392af23a65e946', '5e358251b3afcd0b86c24725', '5bf2c0ff355c320018c800e6', '5e8b67735ac2827d770437c2', '5e9e4e8280d44f0a89e49a48', '5e9e2dadfbbf37b5adf7298c', '5e9e4fa905bc92f16bfa1942', '5e9e50a132b24de657df4ff2']

CREATE_TABLE_QUERY = """
CREATE TABLE project_auto_events.reach_date_prediction
(
    prediction_id         BIGINT IDENTITY (0,1) NOT NULL,
    order_id              varchar(256) sortkey,
    predicted_reach_date  timestamp,
    predicted_reach_dateL timestamp,
    latitude              double precision,
    longitude             double precision,
    predictedat  timestamp default getdate()
);
"""

CREATE_REACH_TO_SHOP_TABLE_QUERY = """
CREATE TABLE if not exists {schema}.reach_to_shop_date_prediction
(
    prediction_id         BIGINT IDENTITY (0,1) NOT NULL,
    order_id              varchar(256) sortkey,
    predicted_reach_date  timestamp,
    predicted_reach_dateL timestamp,
    latitude              double precision,
    longitude             double precision,
    predictedat  timestamp default getdate()
);
""".format(schema=SCHEMA_NAME)

CREATE_REACH_TO_RESTAURANT_TABLE_QUERY = """
CREATE TABLE if not exists {schema}.reach_to_restaurant_date_prediction
(
    prediction_id         BIGINT IDENTITY (0,1) NOT NULL,
    order_id              varchar(256) sortkey,
    predicted_reach_date  timestamp,
    predicted_reach_dateL timestamp,
    latitude              double precision,
    longitude             double precision,
    predictedat  timestamp default getdate()
);
""".format(schema=SCHEMA_NAME)

DEPART_CREATE_TABLE_QUERY = """
CREATE TABLE project_auto_events.depart_date_prediction
(
    prediction_id         BIGINT IDENTITY (0,1) NOT NULL,
    order_id              varchar(256) sortkey,
    predicted_depart_date  timestamp,
    predicted_depart_dateL timestamp,
    latitude              double precision,
    longitude             double precision,
    predictedat  timestamp default getdate()
);
"""

DEPART_FROM_WAREHOUSE_NEW_MODEL_TABLE_QUERY = """
CREATE TABLE project_auto_events.depart_date_prediction_new_model
(
    prediction_id         BIGINT IDENTITY (0,1) NOT NULL,
    order_id              varchar(256) sortkey,
    domain_type           int,
    predicted_depart_date  timestamp,
    predicted_depart_dateL timestamp,
    latitude              double precision,
    longitude             double precision,
    predictedat  timestamp default getdate()
);
"""


FOOD_DEPART_CREATE_TABLE_QUERY = """
CREATE TABLE project_auto_events.food_depart_date_prediction
(
    prediction_id         BIGINT IDENTITY (0,1) NOT NULL,
    order_id              varchar(256) sortkey,
    predicted_depart_date  timestamp,
    predicted_depart_dateL timestamp,
    latitude              double precision,
    longitude             double precision,
    predictedat  timestamp default getdate()
);
"""

ARTISAN_DEPART_CREATE_TABLE_QUERY = """
CREATE TABLE project_auto_events.artisan_depart_date_prediction
(
    prediction_id         BIGINT IDENTITY (0,1) NOT NULL,
    order_id              varchar(256) sortkey,
    predicted_depart_date  timestamp,
    predicted_depart_dateL timestamp,
    latitude              double precision,
    longitude             double precision,
    predictedat  timestamp default getdate()
);
"""

FOOD_DEPART_FROM_MERCHANT_CREATE_TABLE_QUERY = """
CREATE TABLE project_auto_events.food_depart_from_merchant_date_prediction
(
    prediction_id         BIGINT IDENTITY (0,1) NOT NULL,
    order_id              varchar(256) sortkey,
    predicted_depart_date  timestamp,
    predicted_depart_dateL timestamp,
    latitude              double precision,
    longitude             double precision,
    predictedat  timestamp default getdate()
);
"""

ARTISAN_DEPART_FROM_MERCHANT_CREATE_TABLE_QUERY = """
CREATE TABLE project_auto_events.artisan_depart_from_merchant_date_prediction
(
    prediction_id         BIGINT IDENTITY (0,1) NOT NULL,
    order_id              varchar(256) sortkey,
    predicted_depart_date  timestamp,
    predicted_depart_dateL timestamp,
    latitude              double precision,
    longitude             double precision,
    predictedat  timestamp default getdate()
);
"""

FOOD_REACH_TABLE_CREATE_QUERY = """
CREATE TABLE project_auto_events.food_reach_date_prediction
(
    prediction_id         BIGINT IDENTITY (0,1) NOT NULL,
    order_id              varchar(256) sortkey,
    predicted_reach_date  timestamp,
    predicted_reach_dateL timestamp,
    latitude              double precision,
    longitude             double precision,
    predictedat  timestamp default getdate()
);
"""

ARTISAN_REACH_TABLE_CREATE_QUERY = """
CREATE TABLE project_auto_events.artisan_reach_date_prediction
(
    prediction_id         BIGINT IDENTITY (0,1) NOT NULL,
    order_id              varchar(256) sortkey,
    predicted_reach_date  timestamp,
    predicted_reach_dateL timestamp,
    latitude              double precision,
    longitude             double precision,
    predictedat  timestamp default getdate()
);
"""
WATER_DEPART_CREATE_TABLE_QUERY = """
CREATE TABLE project_auto_events.water_depart_date_prediction
(
    prediction_id         BIGINT IDENTITY (0,1) NOT NULL,
    order_id              varchar(256) sortkey,
    predicted_depart_date  timestamp,
    predicted_depart_dateL timestamp,
    latitude              double precision,
    longitude             double precision,
    predictedat  timestamp default getdate()
);
"""

WATER_REACH_CREATE_TABLE_QUERY = """
CREATE TABLE project_auto_events.water_reach_date_prediction
(
    prediction_id         BIGINT IDENTITY (0,1) NOT NULL,
    order_id              varchar(256) sortkey,
    predicted_reach_date  timestamp,
    predicted_reach_dateL timestamp,
    latitude              double precision,
    longitude             double precision,
    predictedat  timestamp default getdate()
);
"""

WATER_DEPART_FROM_CLIENT_CREATE_TABLE_QUERY = """
CREATE TABLE {schema}.water_depart_from_client_date_prediction
(
    prediction_id                       BIGINT IDENTITY (0,1) NOT NULL,
    order_id                            varchar(256) sortkey,
    predicted_depart_from_client_date   timestamp,
    predicted_depart_from_client_dateL  timestamp,
    latitude                            double precision,
    longitude                           double precision,
    predictedat                         timestamp default getdate()
);
""".format(schema=SCHEMA_NAME)

WATER_DELIVERY_CREATE_TABLE_QUERY = """
CREATE TABLE if not exists {schema}.water_delivery_date_prediction
(
    prediction_id                       BIGINT IDENTITY (0,1) NOT NULL,
    order_id                            varchar(256) sortkey,
    predicted_delivery_date             timestamp,
    predicted_delivery_dateL            timestamp,
    latitude                            double precision,
    longitude                           double precision,
    predictedat                         timestamp default getdate()
);
""".format(schema=SCHEMA_NAME)

DEPART_FROM_CLIENT_CREATE_TABLE_QUERY = """
CREATE TABLE {schema}.depart_from_client_date_prediction
(
    prediction_id                       BIGINT IDENTITY (0,1) NOT NULL,
    order_id                            varchar(256) sortkey,
    predicted_depart_from_client_date   timestamp,
    predicted_depart_from_client_dateL  timestamp,
    latitude                            double precision,
    longitude                           double precision,
    predictedat                         timestamp default getdate()
);
""".format(schema=SCHEMA_NAME)

DELIVERY_CREATE_TABLE_QUERY = """
CREATE TABLE if not exists {schema}.delivery_date_prediction
(
    prediction_id                       BIGINT IDENTITY (0,1) NOT NULL,
    order_id                            varchar(256) sortkey,
    predicted_delivery_date   timestamp,
    predicted_delivery_dateL  timestamp,
    latitude                            double precision,
    longitude                           double precision,
    predictedat                         timestamp default getdate()
);
""".format(schema=SCHEMA_NAME)

RUN_INTERVAL = timedelta(hours=1, minutes=30)

DOMAIN_LIST = ['depart', 'reach', 'depart,reach', 'depart_from_client', 'depart,reach,depart_from_client',
               'reach_to_merchant', 'depart_from_merchant', 'deliver', 'depart_from_courier_warehouse',
               'depart,reach,depart_from_client,reach_to_merchant',
               'depart,reach,depart_from_client,reach_to_merchant,deliver,depart_from_merchant,depart_from_courier_warehouse']
DEFAULT_DOMAIN = 'depart,reach,depart_from_client,reach_to_merchant,deliver,depart_from_merchant,depart_from_courier_warehouse'

DEFAULT_TYPE = 'HOURLY'

WITH_PERIOD_TYPE = 'HOURLY,PERIOD'
