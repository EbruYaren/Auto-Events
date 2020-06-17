# import os
# DB CONNECTION URI
# ANALYSIS_DB_URI = os.environ.get('ANALYSIS_DB_URI', '<ANALYSIS_DEV_URI>')
# MAIN_DB_URI = os.environ.get('MAIN_DB_URI', '<MAIN_DB_DEV_URI>')
# LOG_DB_URI = os.environ.get('MAIN_DB_URI', '<_LOG_DB_DEV_URI>')

# ADDITIONAL VARIABLES
# SLACK_HOOK_URL = os.environ.get(SLACK_HOOK, '<test_channel_hook>')
# S3_ACCESS_KEY = '<access_key>'
# S3_SECRET_KEY = '<secret_key>'
# S3_REGION = '<region>'
# S3_BUCKET_NAME = '<bucket_name>'

class Config:
    chunk_size = 1000
    domain_types = [1]
    statuses = [900, 1000]