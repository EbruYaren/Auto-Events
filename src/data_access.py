import time
from src.utils import timer, read_query
import pandas as pd
import datetime
import src.constants as constants
from src.config import MONGO_GETIREVENTS_LIVE, MARKET_ORDER_REDSHIFT


@timer
def get_tab_count(start_date, end_date):
    match1 = {
        'createdAt': {'$gte': start_date - datetime.timedelta(hours=3), '$lt': end_date - datetime.timedelta(hours=3)},  ## Burayı düzelt
        'data.serviceId': {'$in': [constants.service_domain]}
    }
    project1 = {
        '_id': 1,
        'date': {'$substr': ['$createdAtL', 0, 10]},
        'createdAtL': 1,
        'client': 1,
        'category': 1,
        'event': 1,
        'serviceId': '$data.serviceId'
    }
    query = [
        {'$match': match1}, {'$project': project1}]

    df = pd.DataFrame([i for i in MONGO_GETIREVENTS_LIVE.clienteventlogs.aggregate(query, allowDiskUse=True)])
    return df


def get_order_detail(query_start, query_end, given_error):
    query_commercial = read_query('./sql/commercial_data.sql')
    query = query_commercial.format(query_start=query_start, query_end=query_end, given_error=given_error)
    df = pd.read_sql_query(query, MARKET_ORDER_REDSHIFT)
    return df

# def get_unsuc_orders_due_to_getir(query_start, query_end, given_error):
