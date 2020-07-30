from src import config, ROUTES_COLLECTION
from src.Order import Order
from src.Route import Route
from src.DataProcessor import ReachDataProcessor, DepartDataProcessor
from src.Predictor import *
from src.Writer import Writer
from src import REDSHIFT_ETL, WRITE_ENGINE, ENGINE_BITEST
from src.utils import timer, get_run_params, get_local_current_time
from src.data_access import grant_access, create_table, remove_duplicates, drop_table


@timer()
def main():
    print("Cron started")

    if config.TEST:
        start_date = '2020-07-01'
        end_date = '2020-07-21'
        domain = config.DEFAULT_DOMAIN
        courier_ids = [] #config.COURIER_IDS
    else:
        params = get_run_params()
        start_date = params.start_date
        end_date = params.end_date
        domain = params.domain
        courier_ids = []

    print("Start date:", start_date)
    print("End date:", end_date)

    if domain == 'reach' or domain == 'depart,reach':
        if config.REACH_CREATE_TABLE:
            with WRITE_ENGINE.begin() as connection:
                create_table(connection, config.CREATE_TABLE_QUERY)
                grant_access(connection, config.REACH_TABLE_NAME, config.SCHEMA_NAME)
                print("Reach Table created")
        total_processed_routes_for_reach = 0

    if domain == 'depart' or domain == 'depart,reach':
        if config.DEPART_CREATE_TABLE:
            with WRITE_ENGINE.begin() as connection:
                create_table(connection, config.DEPART_CREATE_TABLE_QUERY)
                grant_access(connection, config.DEPART_TABLE_NAME, config.SCHEMA_NAME)
                print("Depart Table created")
        total_processed_routes_for_depart = 0

    orders = Order(start_date, end_date, REDSHIFT_ETL, courier_ids, chunk_size=config.chunk_size)
    for chunk_df in orders.fetch_orders_df():
        print('in fetch_orders_df')

        route_ids = list(chunk_df['delivery_route_oid'].unique())

        routes = Route(
            route_ids, ROUTES_COLLECTION, config.TEST, ENGINE_BITEST)
        routes_df = routes.fetch_routes_df()

        if domain == 'reach' or domain == 'depart,reach':
            processed_reach_orders = reach_main(chunk_df, routes_df)
            total_processed_routes_for_reach += processed_reach_orders
            print("Total Processed Routes For Reach : ", total_processed_routes_for_reach)

        if domain == 'depart' or domain == 'depart,reach':
            processed_depart_orders = depart_main(chunk_df, routes_df)
            total_processed_routes_for_depart += processed_depart_orders
            print("Total Processed Routes for Depart: ", total_processed_routes_for_depart)

    with WRITE_ENGINE.begin() as connection:
        remove_duplicates(connection, config.REACH_TABLE_NAME, 'prediction_id', ['order_id'], config.SCHEMA_NAME)
        remove_duplicates(connection, config.DEPART_TABLE_NAME, 'prediction_id', ['order_id'], config.SCHEMA_NAME)

    print("Duplicates are removed")


def reach_main(chunk_df: pd.DataFrame, routes_df: pd.DataFrame):
    order_ids = pd.DataFrame(chunk_df['_id_oid'], columns=['_id_oid'])
    processed_data = ReachDataProcessor(
        orders=chunk_df, routes=routes_df, minimum_location_limit=config.MINIMUM_LOCATION_LIMIT,
        fibonacci_base=config.FIBONACCI_BASE).process(include_all=False)

    single_predictor = ReachLogisticReachSinglePredictor(config.REACH_INTERCEPT, config.REACH_COEFFICIENTS)
    bulk_predictor = ReachBulkPredictor(processed_data, single_predictor)
    predictions = bulk_predictor.predict_in_bulk()
    predictions = order_ids.merge(predictions, on='_id_oid', how='left')

    writer = Writer(predictions, WRITE_ENGINE, config.REACH_TABLE_NAME, config.SCHEMA_NAME,
                    config.REACH_TABLE_COLUMNS)
    writer.write()

    return chunk_df['delivery_route_oid'].nunique()


def depart_main(chunk_df: pd.DataFrame, routes_df: pd.DataFrame):
    order_ids = pd.DataFrame(chunk_df['_id_oid'], columns=['_id_oid'])
    processed_data = DepartDataProcessor(
        orders=chunk_df, routes=routes_df,
        minimum_location_limit=config.MINIMUM_LOCATION_LIMIT).process()

    single_predictor = DepartLogisticReachSinglePredictor(config.DEPART_INTERCEPT, config.DEPART_COEFFICIENTS)
    bulk_predictor = DepartBulkPredictor(processed_data, single_predictor, config.MAX_DISTANCE_FOR_DEPART_PREDICTION)
    predictions = bulk_predictor.predict_in_bulk()
    predictions = order_ids.merge(predictions, on='_id_oid', how='left')

    writer = Writer(predictions, WRITE_ENGINE, config.DEPART_TABLE_NAME, config.SCHEMA_NAME,
                    config.DEPART_TABLE_COLUMNS)
    writer.write()

    return chunk_df['delivery_route_oid'].nunique()


if __name__ == '__main__':
    main()
