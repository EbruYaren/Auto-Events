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
from src.data_access import data_from_sql_file
from datetime import datetime, timedelta
from src.config import chunk_size
from threading import Thread
import numpy as np


@timer()
def main():
    print("Cron started")

    if config.TEST:
        start_date = '2022-04-19 13:30:00'
        end_date = '2022-04-19 13:32:00'
        domain = config.DEFAULT_DOMAIN
        courier_ids = []  # config.COURIER_IDS
    else:
        params = get_run_params()
        start_date = params.start_date
        end_date = params.end_date
        domain = params.domain
        courier_ids = []

    domains = domain.split(',')

    print("Start date before hourly loop:", start_date)
    print("End date before hourly loop:", end_date)

    for pair in get_date_pairs(start_date, end_date):
        start, end = pair
        run(start, end, domains, courier_ids)

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
        remove_duplicates(connection, config.FOOD_DEPART_TABLE_NAME, 'prediction_id', ['order_id'],
                          config.SCHEMA_NAME)
        remove_duplicates(connection, config.ARTISAN_DEPART_TABLE_NAME, 'prediction_id', ['order_id'],
                          config.SCHEMA_NAME)

        remove_duplicates(connection, config.WATER_REACH_TABLE_NAME, 'prediction_id', ['order_id'],
                          config.SCHEMA_NAME)
        remove_duplicates(connection, config.WATER_DEPART_TABLE_NAME, 'prediction_id', ['order_id'],
                          config.SCHEMA_NAME)
        remove_duplicates(connection, config.WATER_DEPART_FROM_CLIENT_TABLE_NAME, 'prediction_id', ['order_id'],
                          config.SCHEMA_NAME)
        remove_duplicates(connection, config.WATER_DELIVERY_TABLE_NAME, 'prediction_id', ['order_id'],
                          config.SCHEMA_NAME)

    print("Duplicates are removed")

    print('NOW HOUR: ', datetime.now().hour)

    if datetime.utcnow().hour == 1:
        start = datetime.now().replace(minute=0, second=0, microsecond=0) - timedelta(hours=24)
        end = start + timedelta(hours=26)

        params = {
            'start': start,
            'end': end
        }
        data_from_sql_file('./sql/depart_batches.sql', **params)
        print('Batched orders between {} and {} are copied for depart from warehouse event. '.format(start, end))


def create_auto_table(CREATE_TABLE_QUERY: str, CREATE_TABLE_NAME: str, name: str):
    with WRITE_ENGINE.begin() as connection:
        create_table(connection, CREATE_TABLE_QUERY)
        grant_access(connection, CREATE_TABLE_NAME, config.SCHEMA_NAME, config.DB_USER_GROUP)
        print("{} Table created", name)


def run(start_date: str, end_date: str, domains: list, courier_ids: list):
    print("Start date:", start_date)
    print("End date:", end_date)
    print("Domains:", domains)

    start_time = datetime.now().strftime('%Y%m%d%h%m%s')

    if config.REACH_CREATE_TABLE:
        create_auto_table(config.CREATE_TABLE_QUERY, config.REACH_TABLE_NAME, 'Reach')

    if config.REACH_TO_SHOP_TABLE:
        create_auto_table(config.CREATE_REACH_TO_SHOP_TABLE_QUERY, config.REACH_TO_SHOP_TABLE_NAME, 'Reach To Merchant')

    if config.REACH_TO_RESTAURANT_TABLE:
        create_auto_table(config.CREATE_REACH_TO_RESTAURANT_TABLE_QUERY, config.REACH_TO_RESTAURANT_TABLE_NAME,
                     'Reach To Restaurant')

    if config.DEPART_CREATE_TABLE:
        create_auto_table(config.DEPART_CREATE_TABLE_QUERY, config.DEPART_TABLE_NAME, 'Depart')

    if config.DEPART_FROM_CLIENT_CREATE_TABLE:
        create_auto_table(config.DEPART_FROM_CLIENT_CREATE_TABLE_QUERY, config.DEPART_FROM_CLIENT_TABLE_NAME,
                     'Depart From Client')

    if config.DELIVERY_CREATE_TABLE:
        create_auto_table(config.DELIVERY_CREATE_TABLE_QUERY, config.DELIVERY_TABLE_NAME, 'Deliver')

    if config.FOOD_DEPART_CREATE_TABLE:
        create_auto_table(config.FOOD_DEPART_CREATE_TABLE_QUERY, config.FOOD_DEPART_TABLE_NAME, 'Depart for Food')

    if config.ARTISAN_DEPART_CREATE_TABLE:
        create_auto_table(config.ARTISAN_DEPART_CREATE_TABLE_QUERY, config.ARTISAN_DEPART_TABLE_NAME,
                          'Depart for Artisan')

    if config.WATER_REACH_CREATE_TABLE:
        create_auto_table(config.WATER_REACH_CREATE_TABLE_QUERY, config.WATER_REACH_TABLE_NAME, 'Reach for Water')

    if config.WATER_DEPART_CREATE_TABLE:
        create_auto_table(config.WATER_DEPART_CREATE_TABLE_QUERY, config.WATER_DEPART_TABLE_NAME, 'Depart for Water')

    if config.WATER_DEPART_FROM_CLIENT_CREATE_TABLE:
        create_auto_table(config.WATER_DEPART_FROM_CLIENT_CREATE_TABLE_QUERY,
                          config.WATER_DEPART_FROM_CLIENT_TABLE_NAME,
                          'Water Depart From Client')

    if config.WATER_DELIVERY_CREATE_TABLE:
        create_auto_table(config.WATER_DELIVERY_CREATE_TABLE_QUERY, config.WATER_DELIVERY_TABLE_NAME, 'Water Deliver')

    if config.FOOD_DEPART_FROM_MERCHANT_CREATE_TABLE:
        create_auto_table(config.FOOD_DEPART_FROM_MERCHANT_CREATE_TABLE_QUERY,
                          config.FOOD_DEPART_FROM_MERCHANT_TABLE_NAME, 'Depart From Merchant for Food')

    if config.ARTISAN_DEPART_FROM_MERCHANT_CREATE_TABLE:
        create_auto_table(config.ARTISAN_DEPART_FROM_MERCHANT_CREATE_TABLE_QUERY,
                          config.ARTISAN_DEPART_FROM_MERCHANT_TABLE_NAME, 'Depart From Merchant for Artisan')

    if config.FOOD_REACH_TO_CLIENT_TABLE:
        create_auto_table(config.FOOD_REACH_TABLE_CREATE_QUERY,
                          config.FOOD_REACH_TO_CLIENT_TABLE_NAME, 'Reach to client for Food')

    if config.ARTISAN_REACH_TO_CLIENT_TABLE:
        create_auto_table(config.ARTISAN_REACH_TABLE_CREATE_QUERY,
                          config.ARTISAN_REACH_TO_CLIENT_TABLE_NAME, 'Reach to client for Artisan')

    if config.DEPART_FROM_WAREHOUSE_NEW_MODEL_TABLE:
        create_auto_table(config.DEPART_FROM_WAREHOUSE_NEW_MODEL_TABLE_QUERY,
                          config.DEPART_FROM_WAREHOUSE_NEW_MODEL_TABLE_NAME, 'New Depart from Warehouse Model Table')

    threads = {}
    domain_types = [1, 2, 4, 6]
    for domain_type in domain_types:
        threads[domain_type] = Thread(target=fetch_orders_and_process,
                                      args=[start_date, end_date, start_time, courier_ids, domains, domain_type])

    for domain_type in domain_types:
        threads[domain_type].start()

    for domain_type in domain_types:
        threads[domain_type].join()


def fetch_orders_and_process(start_date, end_date, start_time, courier_ids: [], domains, domain_type: int):
    orders = Order(start_date, end_date, REDSHIFT_ETL, courier_ids, chunk_size=config.chunk_size, domains=domains,
                   domain_type=domain_type)

    order_index = -1
    orders_fetch = orders.fetch_orders_df()
    orders_len = orders_fetch.gi_frame.f_locals['result'].rowcount
    last_chunk = False

    for chunk_df in orders_fetch:
        order_index += 1
        if order_index == (orders_len // chunk_size):
            last_chunk = True
        get_routes_and_process(chunk_df, domains, domain_type, start_date, end_date, start_time, last_chunk)
    if orders_len > 0:
        print('Market Orders fetched!')


def get_routes_and_process(chunk_df, domains, domain_type, start_date, end_date, start_time, last_chunk):
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

        merged_df = routes_df.merge(chunk_df, left_on="route_id", right_on="_id_oid", how="inner").drop_duplicates()
        print('Orders and routes merged.')
        reach_predictions = []
        depart_from_client_predictions = []

        if 'depart' in domains:
            if domain_type in (2, 6):
                chunk_df = chunk_df[chunk_df.domaintypes.isin([1, 3])]
                for domain in ['depart_from_merchant', 'depart_from_courier_warehouse']:
                    processed_depart_orders = depart_main(chunk_df, domain_type, domain, merged_df, start_time, last_chunk)
                    total_processed_routes_for_depart += (processed_depart_orders or 0)
            else:
                processed_depart_orders = depart_main(chunk_df, domain_type, '', merged_df, start_time, last_chunk)
                total_processed_routes_for_depart += (processed_depart_orders or 0)
            print("Total Processed Routes for domain type {} Depart: {}".format(domain_type,
                                                                                total_processed_routes_for_depart))

        if 'reach' in domains and domain_type not in (2, 6):
            processed_reach_orders = reach_main(chunk_df, domain_type, merged_df, start_time, last_chunk).get('routes')
            total_processed_routes_for_reach += processed_reach_orders
            reach_predictions = reach_main(chunk_df, domain_type, merged_df, start_time, last_chunk).get('preds')
            orders = chunk_df[['_id_oid', 'deliver_location__coordinates_lon', 'deliver_location__coordinates_lat']]
            reach_predictions = reach_predictions.merge(orders, left_on="_id_oid", right_on="_id_oid", how="inner")
            print("Total Processed Routes For Reach : ", total_processed_routes_for_reach)

        if 'depart_from_client' in domains and domain_type not in (2, 6):
            reach_predictions = reach_predictions[reach_predictions.time.notna()][['_id_oid', 'time', 'time_l']]. \
                rename(columns={'time': 'predicted_reach_date'}).copy()
            depart_from_client_dict = depart_from_client_main(chunk_df, routes_df, reach_predictions,
                                                              domain_type, merged_df, start_time, last_chunk)
            processed_depart_from_client_orders = depart_from_client_dict.get('routes')
            total_processed_routes_for_depart_from_client += processed_depart_from_client_orders
            print("Total Processed Routes for Depart from Client: ", total_processed_routes_for_depart_from_client)
            depart_from_client_predictions = depart_from_client_dict.get('preds')

        if 'deliver' in domains and domain_type not in (2, 6):
            reach_predictions = reach_predictions.rename(columns={'predicted_reach_date': 'time'}).copy()
            result_dict = deliver_main(reach_predictions, depart_from_client_predictions, domain_type, start_time, last_chunk)
            total_processed_orders_for_delivery += result_dict.get('orders')
            print("Total Processed Orders For Delivery : ", total_processed_orders_for_delivery)

        if 'reach_to_merchant' in domains and domain_type in (2, 6):
            processed_reach_to_merchant_orders = reach_to_merchant_main(chunk_df, domain_type, merged_df, start_time, last_chunk)
            total_processed_routes_for_reach_to_merchant += processed_reach_to_merchant_orders
            print("Total Processed Routes For Reach To Merchant : ", total_processed_routes_for_reach_to_merchant)


def reach_main(chunk_df: pd.DataFrame, domain_type: int, merged_df: pd.DataFrame, start_time: str, last_chunk: bool):
    order_ids = pd.DataFrame(chunk_df['_id_oid'], columns=['_id_oid'])
    processed_data = ReachDataProcessor(minimum_location_limit=config.MINIMUM_LOCATION_LIMIT,
                                        fibonacci_base=config.FIBONACCI_BASE,
                                        merged_df=merged_df,
                                        domain_type=domain_type).process(include_all=False)
    print('Reach data processed!')
    single_predictor = ReachLogisticReachSinglePredictor(config.REACH_INTERCEPT, config.REACH_COEFFICIENTS)
    bulk_predictor = ReachBulkPredictor(processed_data, single_predictor)
    predictions = bulk_predictor.predict_in_bulk()
    predictions = order_ids.merge(predictions, on='_id_oid', how='left')
    print('Reach data predicted for domain: ', domain_type)

    if domain_type in (1, 3):
        writer = Writer(predictions, WRITE_ENGINE, config.REACH_TABLE_NAME, config.SCHEMA_NAME,
                        config.REACH_TABLE_COLUMNS, start_time)
        writer.write()
        if last_chunk:
            writer.copy_to_redshift()
    elif domain_type == 2:
        writer = Writer(predictions, WRITE_ENGINE, config.FOOD_REACH_TO_CLIENT_TABLE, config.SCHEMA_NAME,
                        config.FOOD_REACH_TO_CLIENT_TABLE_COLUMNS, start_time)
        writer.write()
    elif domain_type == 6:
        writer = Writer(predictions, WRITE_ENGINE, config.ARTISAN_REACH_TO_CLIENT_TABLE, config.SCHEMA_NAME,
                        config.ARTISAN_REACH_TO_CLIENT_TABLE_COLUMNS, start_time)
        writer.write()
    else:
        writer_water = Writer(predictions, WRITE_ENGINE, config.WATER_REACH_TABLE_NAME, config.SCHEMA_NAME,
                              config.WATER_REACH_TABLE_COLUMNS, start_time)
        writer_water.write()
        if last_chunk:
            writer_water.copy_to_redshift()



    print('Reach predictions written!')
    dict = {
        'preds': predictions,
        'routes': chunk_df['delivery_route_oid'].nunique()
    }

    return dict


def reach_to_merchant_main(chunk_df: pd.DataFrame, domain_type, merged_df: pd.DataFrame, start_time: str, last_chunk: bool):
    print('Length:', len(chunk_df), 'NA column counts:', chunk_df.isna().sum()[chunk_df.isna().sum().gt(0)].to_dict())
    order_ids = pd.DataFrame(chunk_df['_id_oid'], columns=['_id_oid'])
    processed_data = ReachToMerchantDataProcessor(
        minimum_location_limit=config.MINIMUM_LOCATION_LIMIT,
        fibonacci_base=config.FIBONACCI_BASE, domain_type=domain_type, merged_df=merged_df).process(include_all=False)

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
                    table_columns, start_time)
    writer.write()

    if last_chunk:
        writer.copy_to_redshift()

    return chunk_df['delivery_route_oid'].nunique()


def depart_main(chunk_df: pd.DataFrame, domain_type: int, domain: str, merged_df: pd.DataFrame, start_time: str,
                last_chunk: bool):

    processed_data = DepartDataProcessor(
        minimum_location_limit=config.MINIMUM_LOCATION_LIMIT,
        domain=domain,
        merged_df=merged_df).process()
    print('Depart data processed!')

    t1 = Thread(target=depart_from_warehouse_main,
                args=[processed_data, chunk_df, domain_type, domain, last_chunk, start_time])
    t2 = Thread(target=depart_from_warehouse_new_model,
                args=[processed_data, chunk_df, domain_type, last_chunk, start_time])

    t1.start()
    t2.start()

    t1.join()
    t2.join()


    return chunk_df['delivery_route_oid'].nunique()


def depart_from_warehouse_main(processed_data:pd.DataFrame, chunk_df:pd.DataFrame, domain_type: int, domain: str,
                               last_chunk: bool, start_time: str):
    single_predictor = DepartLogisticReachSinglePredictor(config.DEPART_INTERCEPT, config.DEPART_COEFFICIENTS)
    bulk_predictor = DepartBulkPredictor(processed_data, single_predictor, config.MAX_DISTANCE_FOR_DEPART_PREDICTION,
                                         chunk_df, domain_type)
    predictions = bulk_predictor.predict_in_bulk()

    if predictions is not None and len(predictions) > 0:
        order_ids = pd.DataFrame(chunk_df['_id_oid'], columns=['_id_oid'])
        predictions = order_ids.merge(predictions, on='_id_oid', how='left')
        print('Depart data predicted for: ', domain_type.__str__() + '_' + domain)
        # 'depart_from_merchant', 'depart_from_courier_warehouse'
        if domain_type == 2:
            if domain == 'depart_from_merchant':
                TABLE_NAME = config.FOOD_DEPART_FROM_MERCHANT_TABLE_NAME
                TABLE_COLUMNS = config.FOOD_DEPART_FROM_MERCHANT_TABLE_COLUMNS
            else:
                TABLE_NAME = config.FOOD_DEPART_TABLE_NAME
                TABLE_COLUMNS = config.FOOD_DEPART_TABLE_COLUMNS

        elif domain_type == 6:
            if domain == 'depart_from_merchant':
                TABLE_NAME = config.ARTISAN_DEPART_FROM_MERCHANT_TABLE_NAME
                TABLE_COLUMNS = config.ARTISAN_DEPART_FROM_MERCHANT_TABLE_COLUMNS
            else:
                TABLE_NAME = config.ARTISAN_DEPART_TABLE_NAME
                TABLE_COLUMNS = config.ARTISAN_DEPART_TABLE_COLUMNS

        elif domain_type == 4:
            TABLE_NAME = config.WATER_DEPART_TABLE_NAME
            TABLE_COLUMNS = config.WATER_DEPART_TABLE_COLUMNS

        else:
            TABLE_NAME = config.DEPART_TABLE_NAME
            TABLE_COLUMNS = config.DEPART_TABLE_COLUMNS

        writer = Writer(predictions, WRITE_ENGINE, TABLE_NAME, config.SCHEMA_NAME, TABLE_COLUMNS, start_time)
        writer.write()

        if last_chunk:
            writer.copy_to_redshift()
        print('Depart data predictions written!')


def depart_from_warehouse_new_model(processed_data: pd.DataFrame, chunk_df:pd.DataFrame, domain_type: int,
                                    last_chunk: bool, start_time: str):
    processed_data['is_car'] = processed_data.apply(lambda x: 1 if x['vehicle_type'] == 6 else 0, axis=1)
    new_model_predictor = NewDepartModelPredictor(processed_data, config.MAX_DISTANCE_FOR_DEPART_PREDICTION,
                                                  chunk_df, domain_type)
    new_model_predictions = new_model_predictor.predict()
    if new_model_predictions is not None and len(new_model_predictions) > 0:
        order_ids = pd.DataFrame(chunk_df['_id_oid'], columns=['_id_oid'])
        predictions = order_ids.merge(new_model_predictions, on='_id_oid', how='left')
        predictions['domain_type'] = domain_type
        predictions = predictions[['_id_oid', 'domain_type', 'time', 'time_l', 'lat', 'lon']]
        print('New Model Depart data predicted for: ', domain_type.__str__() + '_' + str(domain_type))

        TABLE_NAME = config.DEPART_FROM_WAREHOUSE_NEW_MODEL_TABLE_NAME
        TABLE_COLUMNS = config.DEPART_FROM_WAREHOUSE_NEW_MODEL_TABLE_COLUMNS

        writer = Writer(predictions, WRITE_ENGINE, TABLE_NAME, config.SCHEMA_NAME, TABLE_COLUMNS, start_time)
        writer.write()

        if last_chunk:
            writer.copy_to_redshift()
    print('Depart data new model predictions written!')


def depart_from_client_main(chunk_df: pd.DataFrame, routes_df: pd.DataFrame, reach_predictions_df: pd.DataFrame,
                            domain_type: int, merged_df: pd.DataFrame, start_time: str, last_chunk: bool):
    order_ids = pd.DataFrame(chunk_df['_id_oid'], columns=['_id_oid'])
    processed_data = DepartFromClientDataProcessor(
        orders=chunk_df, routes=routes_df,
        minimum_location_limit=config.MINIMUM_LOCATION_LIMIT,
        domain_type=domain_type,
        merged_df=merged_df).process()
    print('Depart from client data processed!')
    single_predictor = DepartFromClientLogisticReachSinglePredictor(config.DEPART_FROM_CLIENT_INTERCEPT,
                                                                    config.DEPART_FROM_CLIENT_COEFFICIENTS)
    bulk_predictor = DepartFromClientBulkPredictor(processed_data, single_predictor,
                                                   config.MAX_DISTANCE_FOR_DEPART_FROM_CLIENT_PREDICTION,
                                                   reach_predictions_df=reach_predictions_df)
    predictions = bulk_predictor.predict_in_bulk()
    predictions = order_ids.merge(predictions, on='_id_oid', how='left')
    print('Depart from client data predicted!')
    if domain_type in (1, 3):
        writer = Writer(predictions, WRITE_ENGINE, config.DEPART_FROM_CLIENT_TABLE_NAME, config.SCHEMA_NAME,
                        config.DEPART_FROM_CLIENT_TABLE_COLUMNS, start_time)
        writer.write()
        if last_chunk:
            writer.copy_to_redshift()
    else:
        writer_water = Writer(predictions, WRITE_ENGINE, config.WATER_DEPART_FROM_CLIENT_TABLE_NAME, config.SCHEMA_NAME,
                              config.WATER_DEPART_FROM_CLIENT_TABLE_COLUMNS, start_time)
        writer_water.write()
        if last_chunk:
            writer_water.copy_to_redshift()
    print('Depart from client data predictions written!')
    dict = {
        'preds': predictions,
        'routes': chunk_df['delivery_route_oid'].nunique()
    }

    return dict


def deliver_main(reach_df: pd.DataFrame, depart_from_client_df: pd.DataFrame, domain_type: int, start_time: str, last_chunk: bool):
    reach_df = reach_df[reach_df.time.notna()]
    depart_from_client_df = depart_from_client_df[depart_from_client_df.time.notna()]
    delivery_predictions = reach_df.merge(depart_from_client_df, on="_id_oid", how="inner")
    orders = depart_from_client_df['_id_oid'].nunique()
    delivery_predictions['time'] = delivery_predictions['time_x'] + (
            delivery_predictions['time_y'] - delivery_predictions['time_x']) / 2
    delivery_predictions['time_l_x'] = pd.to_datetime(delivery_predictions['time_l_x'])
    delivery_predictions['time_l_y'] = pd.to_datetime(delivery_predictions['time_l_y'])
    delivery_predictions['time_l'] = delivery_predictions['time_l_x'] + (
            delivery_predictions['time_l_y'] - delivery_predictions['time_l_x']) / 2
    delivery_predictions = delivery_predictions[['_id_oid', 'time', 'time_l', 'lat', 'lon']]
    print('Deliver data predicted!')
    if domain_type in (1, 3):
        writer = Writer(delivery_predictions, WRITE_ENGINE, config.DELIVERY_TABLE_NAME, config.SCHEMA_NAME,
                        config.DELIVERY_TABLE_COLUMNS, start_time)
        writer.write()
        if last_chunk:
            writer.copy_to_redshift()
    else:
        writer_water = Writer(delivery_predictions, WRITE_ENGINE, config.WATER_DELIVERY_TABLE_NAME, config.SCHEMA_NAME,
                              config.WATER_DELIVERY_TABLE_COLUMNS, start_time)
        writer_water.write()
        if last_chunk:
            writer_water.copy_to_redshift()
    print('Deliver data predictions written!')
    dict = {
        'preds': delivery_predictions,
        'orders': orders
    }

    return dict


if __name__ == '__main__':
    main()
