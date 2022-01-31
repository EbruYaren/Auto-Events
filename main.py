from src import config, ROUTES_COLLECTION
from src.Order import Order
from src.Route import Route
from src.DataProcessor import ReachDataProcessor, DepartDataProcessor, DepartFromClientDataProcessor, \
    ReachToMerchantDataProcessor
from src.Predictor import *
from src.Writer import Writer
from src import REDSHIFT_ETL, WRITE_ENGINE, ENGINE_BITEST
from src.utils import timer, get_run_params, get_date_pairs
from src.data_access import grant_access, create_table, remove_duplicates, drop_table


@timer()
def main():
    print("Cron started")

    if config.TEST:
        start_date = '2022-01-16 18:30:00'
        end_date = '2022-01-16 20:00:00'
        domain = config.DEFAULT_DOMAIN
        courier_ids = []  # config.COURIER_IDS
    else:
        params = get_run_params()
        start_date = params.start_date
        end_date = params.end_date
        domain = params.domain
        courier_ids = []

    domains = domain.split(',')

    for pair in get_date_pairs(start_date, end_date):
        start, end = pair
        run(start, end, domains, courier_ids)


def run(start_date: str, end_date: str, domains: list, courier_ids: list):
    print("Start date:", start_date)
    print("End date:", end_date)
    print("Domains:", domains)

    if config.REACH_CREATE_TABLE:
        with WRITE_ENGINE.begin() as connection:
            create_table(connection, config.CREATE_TABLE_QUERY)
            grant_access(connection, config.REACH_TABLE_NAME, config.SCHEMA_NAME, config.DB_USER_GROUP)
            print("Reach Table created")

    if config.REACH_TO_SHOP_TABLE:
        with WRITE_ENGINE.begin() as connection:
            create_table(connection, config.CREATE_REACH_TO_SHOP_TABLE_QUERY)
            grant_access(connection, config.REACH_TO_SHOP_TABLE_NAME, config.SCHEMA_NAME, config.DB_USER_GROUP)
            print("Reach To Shop table created")

    if config.REACH_TO_RESTAURANT_TABLE:
        with WRITE_ENGINE.begin() as connection:
            create_table(connection, config.CREATE_REACH_TO_RESTAURANT_TABLE_QUERY)
            grant_access(connection, config.REACH_TO_RESTAURANT_TABLE_NAME, config.SCHEMA_NAME, config.DB_USER_GROUP)
            print("Reach To Restaurant table created")

    if config.DEPART_CREATE_TABLE:
        with WRITE_ENGINE.begin() as connection:
            create_table(connection, config.DEPART_CREATE_TABLE_QUERY)
            grant_access(connection, config.DEPART_TABLE_NAME, config.SCHEMA_NAME, config.DB_USER_GROUP)
            print("Depart Table created")

    if config.DEPART_FROM_CLIENT_CREATE_TABLE:
        with WRITE_ENGINE.begin() as connection:
            create_table(connection, config.DEPART_FROM_CLIENT_CREATE_TABLE_QUERY)
            grant_access(connection, config.DEPART_FROM_CLIENT_TABLE_NAME, config.SCHEMA_NAME, config.DB_USER_GROUP)
            print("Depart From Client Table created")

    orders = Order(start_date, end_date, REDSHIFT_ETL, courier_ids, chunk_size=config.chunk_size, domains=domains,
                   domain_type=1)
    food_orders = Order(start_date, end_date, REDSHIFT_ETL, courier_ids, chunk_size=config.chunk_size, domain_type=2)
    artisan_orders = Order(start_date, end_date, REDSHIFT_ETL, courier_ids, chunk_size=config.chunk_size,
                           domain_type=6)

    for chunk_df in orders.fetch_orders_df():
        get_routes_and_process(chunk_df, domains, 1, start_date, end_date)
    for chunk_df in artisan_orders.fetch_orders_df():
        get_routes_and_process(chunk_df, domains, 6, start_date, end_date)
    for chunk_df in food_orders.fetch_orders_df():
        get_routes_and_process(chunk_df, domains, 2, start_date, end_date)


def get_routes_and_process(chunk_df, domains, domain_type, start_date, end_date):
    total_processed_routes_for_reach = 0
    total_processed_routes_for_depart = 0
    total_processed_routes_for_depart_from_client = 0
    total_processed_routes_for_reach_to_merchant = 0

    print('in fetch_orders_df')

    route_ids = list(chunk_df['delivery_route_oid'].dropna().unique())
    courier_ids = list(chunk_df['courier_courier_oid'].dropna().unique())

    routes = Route(
        route_ids, ROUTES_COLLECTION, config.TEST, REDSHIFT_ETL)
    routes_df = routes.fetch_routes_df()
    print('Routes fetched:', len(routes_df))

    if 'reach' in domains:
        processed_reach_orders = reach_main(chunk_df, routes_df)
        total_processed_routes_for_reach += processed_reach_orders
        print("Total Processed Routes For Reach : ", total_processed_routes_for_reach)

    if 'depart' in domains:
        processed_depart_orders = depart_main(chunk_df, routes_df)
        total_processed_routes_for_depart += processed_depart_orders
        print("Total Processed Routes for Depart: ", total_processed_routes_for_depart)

    if 'depart_from_client' in domains:
        from src.CourierTrajectory import CourierTrajectory
        trajectories = CourierTrajectory(courier_ids, start_date, end_date).fetch()
        print('Return trajectories fetched:', len(trajectories))
        processed_depart_from_client_orders = depart_from_client_main(chunk_df, routes_df, trajectories)
        total_processed_routes_for_depart_from_client += processed_depart_from_client_orders
        print("Total Processed Routes for Depart from Client: ", total_processed_routes_for_depart_from_client)

    if 'reach_to_merchant' in domains:
        processed_reach_to_merchant_orders = reach_to_merchant_main(chunk_df, routes_df, domain_type)
        total_processed_routes_for_reach_to_merchant += processed_reach_to_merchant_orders
        print("Total Processed Routes For Reach To Merchant : ", total_processed_routes_for_reach_to_merchant)


    with WRITE_ENGINE.begin() as connection:
        remove_duplicates(connection, config.REACH_TABLE_NAME, 'prediction_id', ['order_id'], config.SCHEMA_NAME)
        remove_duplicates(connection, config.DEPART_TABLE_NAME, 'prediction_id', ['order_id'], config.SCHEMA_NAME)
        remove_duplicates(connection, config.DEPART_FROM_CLIENT_TABLE_NAME, 'prediction_id', ['order_id'],
                          config.SCHEMA_NAME)
        remove_duplicates(connection, config.REACH_TO_SHOP_TABLE_NAME, 'prediction_id', ['order_id'],
                          config.SCHEMA_NAME)
        remove_duplicates(connection, config.REACH_TO_RESTAURANT_TABLE_NAME, 'prediction_id', ['order_id'],
                          config.SCHEMA_NAME)

    print("Duplicates are removed")


def reach_main(chunk_df: pd.DataFrame, routes_df: pd.DataFrame):
    order_ids = pd.DataFrame(chunk_df['_id_oid'], columns=['_id_oid'])
    processed_data = ReachDataProcessor(orders=chunk_df, routes=routes_df,
                                        minimum_location_limit=config.MINIMUM_LOCATION_LIMIT,
                                        fibonacci_base=config.FIBONACCI_BASE).process(include_all=False)

    single_predictor = ReachLogisticReachSinglePredictor(config.REACH_INTERCEPT, config.REACH_COEFFICIENTS)
    bulk_predictor = ReachBulkPredictor(processed_data, single_predictor)
    predictions = bulk_predictor.predict_in_bulk()
    predictions = order_ids.merge(predictions, on='_id_oid', how='left')

    writer = Writer(predictions, WRITE_ENGINE, config.REACH_TABLE_NAME, config.SCHEMA_NAME,
                    config.REACH_TABLE_COLUMNS)
    writer.write()

    return chunk_df['delivery_route_oid'].nunique()


def reach_to_merchant_main(chunk_df: pd.DataFrame, routes_df: pd.DataFrame, domain_type):
    order_ids = pd.DataFrame(chunk_df['_id_oid'], columns=['_id_oid'])
    processed_data = ReachToMerchantDataProcessor(
            orders=chunk_df, routes=routes_df, minimum_location_limit=config.MINIMUM_LOCATION_LIMIT,
            fibonacci_base=config.FIBONACCI_BASE, domain_type=domain_type).process(include_all=False)

    single_predictor = ReachLogisticReachSinglePredictor(config.REACH_INTERCEPT, config.REACH_COEFFICIENTS)
    bulk_predictor = ReachBulkPredictor(processed_data, single_predictor)
    predictions = bulk_predictor.predict_in_bulk()
    predictions = order_ids.merge(predictions, on='_id_oid', how='left')

    table_name = ""
    table_columns = []

    if domain_type == 2:
        table_name = config.REACH_TO_RESTAURANT_TABLE_NAME
        table_columns = config.REACH_TO_RESTAURANT_TABLE_COLUMNS
    elif domain_type == 6:
        table_name = config.REACH_TO_SHOP_TABLE_NAME
        table_columns = config.REACH_TO_SHOP_TABLE_COLUMNS

    writer = Writer(predictions, WRITE_ENGINE, table_name, config.SCHEMA_NAME,
                    table_columns)
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


def depart_from_client_main(chunk_df: pd.DataFrame, routes_df: pd.DataFrame, trajectory_df: pd.DataFrame):
    order_ids = pd.DataFrame(chunk_df['_id_oid'], columns=['_id_oid'])
    processed_data = DepartFromClientDataProcessor(
        orders=chunk_df, routes=routes_df, trajectories=trajectory_df,
        minimum_location_limit=config.MINIMUM_LOCATION_LIMIT).process()

    single_predictor = DepartFromClientLogisticReachSinglePredictor(config.DEPART_FROM_CLIENT_INTERCEPT,
                                                                    config.DEPART_FROM_CLIENT_COEFFICIENTS)
    bulk_predictor = DepartFromClientBulkPredictor(processed_data, single_predictor,
                                                   config.MAX_DISTANCE_FOR_DEPART_FROM_CLIENT_PREDICTION)
    predictions = bulk_predictor.predict_in_bulk()
    predictions = order_ids.merge(predictions, on='_id_oid', how='left')

    writer = Writer(predictions, WRITE_ENGINE, config.DEPART_FROM_CLIENT_TABLE_NAME, config.SCHEMA_NAME,
                    config.DEPART_FROM_CLIENT_TABLE_COLUMNS)
    writer.write()

    return chunk_df['delivery_route_oid'].nunique()


if __name__ == '__main__':
    main()
