from src import config, ROUTES_COLLECTION
from src.Order import Order
from src.Route import Route
from src.DataProcessor import DataProcessor
from src.Predictor import *
from src.Writer import Writer
from src import REDSHIFT_ETL, WRITE_ENGINE
from src.utils import timer, get_run_dates, get_local_current_time
from src.data_access import grant_access, create_table, remove_duplicates, drop_table

@timer()
def main():


    now = get_local_current_time().replace(minute=0, second=0, microsecond=0)
    start = now-config.RUN_INTERVAL


    if config.TEST:
        start_date = '2020-06-06'
        end_date = '2020-06-08'
        courier_ids = config.COURIER_IDS
    else:
        start_date = str(start)
        end_date = str(now)
        courier_ids = []

    print('Create Table Status:', config.CREATE_TABLE)
    if config.CREATE_TABLE:
        with WRITE_ENGINE.begin() as connection:
            create_table(connection, config.CREATE_TABLE_QUERY)
            grant_access(connection, config.TABLE_NAME, config.SCHEMA_NAME)

    orders = Order(start_date, end_date, REDSHIFT_ETL, courier_ids, chunk_size=config.chunk_size)
    for chunk_df in orders.fetch_orders_df():

        order_ids = pd.DataFrame(chunk_df['_id_oid'], columns=['_id_oid'])

        route_ids = list(chunk_df['delivery_route_oid'].unique())

        routes = Route(
            route_ids, ROUTES_COLLECTION, config.TEST, config.test_pickle_file)
        routes_df = routes.fetch_routes_df()

        processed_data = DataProcessor(
            orders=chunk_df, routes=routes_df, minimum_location_limit=config.MINIMUM_LOCATION_LIMIT,
            fibonacci_base=config.FIBONACCI_BASE).process(include_all=False)

        single_predictor = LogisticReachSinglePredictor(config.INTERCEPT, config.COEFFICIENTS)
        bulk_predictor = BulkPredictor(processed_data, single_predictor)
        predictions = bulk_predictor.predict_in_bulk()
        predictions = order_ids.merge(predictions, on='_id_oid', how='left')
        writer = Writer(predictions, WRITE_ENGINE, config.TABLE_NAME, config.SCHEMA_NAME)
        writer.write()

    with WRITE_ENGINE.begin() as connection:
        remove_duplicates(connection, config.TABLE_NAME, 'prediction_id', ['order_id'], config.SCHEMA_NAME)





if __name__ == '__main__':
    main()
