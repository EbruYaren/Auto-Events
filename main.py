from src import config, ROUTES_COLLECTION
from src.Order import Order
from src.Route import Route
from src.DataProcessor import ReachDataProcessor, DepartDataProcessor, DepartFromClientDataProcessor, \
    ReachToMerchantDataProcessor
from src.Predictor import *
from src.Writer import Writer
from src import REDSHIFT_ETL, WRITE_ENGINE
from src.utils import timer, get_run_params, get_date_pairs
from src.data_access import grant_access, create_table, remove_duplicates, drop_table
from src import ATHENA
from src import FillUnpredictedDepartBatches
from src.FillUnpredictedDepartBatches import FillUnpredictedDepartBatches


@timer()
def main():
    print("Cron started")

    params = {}

    if config.TEST:
        start_date = '2022-04-04 15:30:00'
        end_date = '2022-04-04 16:00:00'
        domain = config.DEFAULT_DOMAIN
        type = config.DEFAULT_TYPE
        courier_ids = []  # config.COURIER_IDS
    else:
        params = get_run_params()
        type = params.type
        start_date = params.start_date
        end_date = params.end_date
        domain = params.domain
        courier_ids = []

    domains = domain.split(',')

    for pair in get_date_pairs(start_date, end_date):
        start, end = pair
        run(start, end, domains, courier_ids)

    if 'PERIOD' in type:
        start = params.period_start_date
        end = params.period_end_date
        FillUnpredictedDepartBatches(start, end).fill()
        print('Batched orders between {} and {} are copied for depart from warehouse event. '.format(start, end))




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

    if config.DELIVERY_CREATE_TABLE:
        with WRITE_ENGINE.begin() as connection:
            create_table(connection, config.DELIVERY_CREATE_TABLE_QUERY)
            grant_access(connection, config.DELIVERY_TABLE_NAME, config.SCHEMA_NAME, config.DB_USER_GROUP)
            print("Delivery Table created")

    orders = Order(start_date, end_date, REDSHIFT_ETL, courier_ids, chunk_size=config.chunk_size, domains=domains,
                   domain_type=1)
    food_orders = Order(start_date, end_date, REDSHIFT_ETL, courier_ids, chunk_size=config.chunk_size, domain_type=2)
    artisan_orders = Order(start_date, end_date, REDSHIFT_ETL, courier_ids, chunk_size=config.chunk_size,
                           domain_type=6)

    for chunk_df in orders.fetch_orders_df():
        get_routes_and_process(chunk_df, domains, 1, start_date, end_date)
    for chunk_df in food_orders.fetch_orders_df():
        get_routes_and_process(chunk_df, domains, 2, start_date, end_date)
    for chunk_df in artisan_orders.fetch_orders_df():
        get_routes_and_process(chunk_df, domains, 6, start_date, end_date)


def get_routes_and_process(chunk_df, domains, domain_type, start_date, end_date):
    total_processed_routes_for_reach = 0
    total_processed_routes_for_depart = 0
    total_processed_routes_for_depart_from_client = 0
    total_processed_routes_for_reach_to_merchant = 0
    total_processed_orders_for_delivery = 0


    print('in fetch_orders_df. Shape:', chunk_df.shape)

    if chunk_df.size > 0:

        order_ids = list(chunk_df._id_oid.unique())

        routes = Route(start_date, end_date, ROUTES_COLLECTION, config.TEST, REDSHIFT_ETL, ATHENA,
                       order_ids, domain_type)
        routes_df = routes.fetch_routes_df()
        print('Routes fetched:', len(routes_df))

        reach_predictions = []
        depart_from_client_predictions = []

        if 'reach' in domains and domain_type not in (2, 6):
            processed_reach_orders = reach_main(chunk_df, routes_df).get('routes')
            total_processed_routes_for_reach += processed_reach_orders
            reach_predictions = reach_main(chunk_df, routes_df).get('preds')
            orders = chunk_df[['_id_oid', 'deliver_location__coordinates_lon', 'deliver_location__coordinates_lat']]
            reach_predictions = reach_predictions.merge(orders, left_on="_id_oid", right_on="_id_oid", how="inner")
            print("Total Processed Routes For Reach : ", total_processed_routes_for_reach)

        if 'depart' in domains and domain_type not in (2, 6):
            processed_depart_orders = depart_main(chunk_df, routes_df)
            total_processed_routes_for_depart += processed_depart_orders
            print("Total Processed Routes for Depart: ", total_processed_routes_for_depart)

        if 'depart_from_client' in domains and domain_type not in (2, 6):
            reach_predictions = reach_predictions[reach_predictions.time.notna()][['_id_oid', 'time']].\
                rename(columns={'time': 'predicted_reach_date'}).copy()
            depart_from_client_dict = depart_from_client_main(chunk_df, routes_df, reach_predictions)
            processed_depart_from_client_orders = depart_from_client_dict.get('routes')
            total_processed_routes_for_depart_from_client += processed_depart_from_client_orders
            print("Total Processed Routes for Depart from Client: ", total_processed_routes_for_depart_from_client)
            depart_from_client_predictions = depart_from_client_dict.get('preds')

        if 'depart' in domains and domain_type not in (2, 6):
            processed_depart_orders = depart_main(chunk_df, routes_df)
            total_processed_routes_for_depart += processed_depart_orders
            print("Total Processed Routes for Depart: ", total_processed_routes_for_depart)


        if 'deliver' in domains and domain_type not in (2, 6):
            reach_predictions = reach_predictions.rename(columns={'predicted_reach_date': 'time'}).copy()
            result_dict = deliver_main(reach_predictions, depart_from_client_predictions)
            total_processed_orders_for_delivery += result_dict.get('orders')
            print("Total Processed Orders For Delivery : ", total_processed_orders_for_delivery)


        if 'reach_to_merchant' in domains and domain_type in (2, 6):
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
        remove_duplicates(connection, config.DELIVERY_TABLE_NAME, 'prediction_id', ['order_id'], config.SCHEMA_NAME)

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

    dict = {
        'preds': predictions,
        'routes': chunk_df['delivery_route_oid'].nunique()
    }

    return dict


def reach_to_merchant_main(chunk_df: pd.DataFrame, routes_df: pd.DataFrame, domain_type):
    print('Length:', len(chunk_df), 'NA column counts:', chunk_df.isna().sum()[chunk_df.isna().sum().gt(0)].to_dict())
    order_ids = pd.DataFrame(chunk_df['_id_oid'], columns=['_id_oid'])
    processed_data = ReachToMerchantDataProcessor(
        orders=chunk_df, routes=routes_df, minimum_location_limit=config.MINIMUM_LOCATION_LIMIT,
        fibonacci_base=config.FIBONACCI_BASE, domain_type=domain_type).process(include_all=False)

    if processed_data is None:
        print("Prediction could not be performed because there is no route information.")
        return 0

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
    bulk_predictor = DepartBulkPredictor(processed_data, single_predictor, config.MAX_DISTANCE_FOR_DEPART_PREDICTION, chunk_df)
    predictions = bulk_predictor.predict_in_bulk()
    predictions = order_ids.merge(predictions, on='_id_oid', how='left')

    writer = Writer(predictions, WRITE_ENGINE, config.DEPART_TABLE_NAME, config.SCHEMA_NAME,
                    config.DEPART_TABLE_COLUMNS)
    writer.write()

    return chunk_df['delivery_route_oid'].nunique()


def depart_from_client_main(chunk_df: pd.DataFrame, routes_df: pd.DataFrame, reach_predictions_df: pd.DataFrame):
    order_ids = pd.DataFrame(chunk_df['_id_oid'], columns=['_id_oid'])
    processed_data = DepartFromClientDataProcessor(
        orders=chunk_df, routes=routes_df,
        minimum_location_limit=config.MINIMUM_LOCATION_LIMIT).process()

    single_predictor = DepartFromClientLogisticReachSinglePredictor(config.DEPART_FROM_CLIENT_INTERCEPT,
                                                                    config.DEPART_FROM_CLIENT_COEFFICIENTS)
    bulk_predictor = DepartFromClientBulkPredictor(processed_data, single_predictor,
                                                   config.MAX_DISTANCE_FOR_DEPART_FROM_CLIENT_PREDICTION,
                                                   reach_predictions_df=reach_predictions_df)
    predictions = bulk_predictor.predict_in_bulk()
    predictions = order_ids.merge(predictions, on='_id_oid', how='left')

    writer = Writer(predictions, WRITE_ENGINE, config.DEPART_FROM_CLIENT_TABLE_NAME, config.SCHEMA_NAME,
                    config.DEPART_FROM_CLIENT_TABLE_COLUMNS)
    writer.write()

    dict = {
        'preds': predictions,
        'routes': chunk_df['delivery_route_oid'].nunique()
    }

    return dict


def deliver_main(reach_df: pd.DataFrame, depart_from_client_df: pd.DataFrame):
    reach_df = reach_df[reach_df.time.notna()]
    depart_from_client_df = depart_from_client_df[depart_from_client_df.time.notna()]
    delivery_predictions = reach_df.merge(depart_from_client_df, on="_id_oid", how="inner")
    orders = depart_from_client_df['_id_oid'].nunique()
    delivery_predictions['time'] = delivery_predictions['time_x'] + (
                delivery_predictions['time_y'] - delivery_predictions['time_x']) / 2
    delivery_predictions = delivery_predictions[['_id_oid', 'time', 'lat', 'lon']]

    writer = Writer(delivery_predictions, WRITE_ENGINE, config.DELIVERY_TABLE_NAME, config.SCHEMA_NAME,
                    config.DELIVERY_TABLE_COLUMNS)
    writer.write()

    dict = {
        'preds': delivery_predictions,
        'orders': orders
    }

    return dict


if __name__ == '__main__':
    main()
