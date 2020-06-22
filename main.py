from src import config, ROUTES_COLLECTION
from src.Order import Order
from src.Route import Route
from src.DataProcessor import DataProcessor
from src.Predictor import *
from src.Writer import Writer
from src import REDSHIFT_ETL, WRITE_ENGINE
from src.utils import timer, get_run_dates
from datetime import timedelta, datetime
import argparse

@timer()
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-sd", "--start_date",
                        help="Start Date of the time interval of the cron")
    parser.add_argument("-ed", "--end_date",
                        help="End Date of the time interval of the cron")
    args, leftovers = parser.parse_known_args()
    start_date = datetime.strptime(args.start_date, '%Y-%m-%d')
    end_date = datetime.strptime(args.end_date, '%Y-%m-%d')

    print(start_date, end_date)



    dates = get_run_dates(interval=timedelta(hours=1))



    for sd, ed in zip(dates, dates[1:]):
        print(sd, ed)

    parser = argparse.ArgumentParser()
    args, leftovers = parser.parse_known_args()
    print(args)
    print(leftovers)
    return



    if config.TEST:
        start_date = '2020-06-06'
        end_date = '2020-06-08'
        courier_ids = config.COURIER_IDS
    else:
        courier_ids = []


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


if __name__ == '__main__':
    main()
