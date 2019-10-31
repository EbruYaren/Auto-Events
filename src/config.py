import os
from sqlalchemy import create_engine
from pymongo import MongoClient

# DB CONNECTION URI
MARKET_ORDER_REDSHIFT = create_engine(os.environ.get('G30_REDSHIFT'))
MONGO_GETIREVENTS_LIVE = MongoClient(os.environ.get('MONGO_GETIREVENTS_LIVE')).get_database()
REDSHIFT_MARKET_ANALYTICS_LIVE = create_engine(os.environ.get('REDSHIFT_MARKET_ANALYTICS_LIVE'))
MONGO_MARKET_ANALYSIS_LIVE = MongoClient(os.environ.get('MONGO_MARKET_ANALYSIS_LIVE')).get_database()


# MAIN_DB_URI = os.environ.get('MAIN_DB_URI', '<MAIN_DB_DEV_URI>')
# LOG_DB_URI = os.environ.get('MAIN_DB_URI', '<_LOG_DB_DEV_URI>')

# ADDITIONAL VARIABLES
# SLACK_HOOK_URL = os.environ.get(SLACK_HOOK, '<test_channel_hook>')
# S3_ACCESS_KEY = '<access_key>'
# S3_SECRET_KEY = '<secret_key>'
# S3_REGION = '<region>'
# S3_BUCKET_NAME = '<bucket_name>'
