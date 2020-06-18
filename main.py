from src import config
from src.Order import Order
from src.Route import Route
from src.DataProcessor import DataProcessor
from src.Predictor import *
from src.Writer import Writer


def main():
    if config.TEST:
        start_date = '2020-06-06'
        end_date = '2020-06-08'
        query = """
        SELECT _id_oid courier_id FROM etl_getir.couriers c
        WHERE c.person_oid in (
        '5bf2c0ff355c320018c800e5',
        '5df1612281efff6400692268',
        '5e0d768e117b3ee1885db2b1',
        '5bf2c0ff355c320018c800e5',
        '5e2edd0c295a758e61c05fea',
        '5e714a717e392a9ce765e945',
        '59b942f6d7b315b905ffbf80',
        '5e714a717e392a9ce765e945',
        '59b942f5d7b315b905ffbd0e',
        '5cfb7a5a6d84890001b16571',
        '5cfb7a5a6d84890001b16571',
        '59b942f6d7b315b905ffc0c5',
        '5e3adcc9a31b28400bd35e7b',
        '5e3581e4b9c71b84c1606adb',
        '59b942f6d7b315b905ffbf41',
        '5e3581e4b9c71b84c1606adb',
        '5dc500627aa58c549fe3582f',
        '5e8b9ec8eb09c711f7cd1351',
        '5e8b67735ac282c2780437c1' )
        """
        couriers_df = pd.read_sql(query, config.REDSHIFT_ETL)
        courier_ids = couriers_df['courier_id'].to_list()

    orders = Order(start_date, end_date, config.REDSHIFT_ETL, courier_ids, chunk_size=config.chunk_size)

    if config.chunk_size is None:
        route_ids = orders.orders_df['_id_oid'].unique()
        routes = Route(route_ids, config.MONGO_ENGINE, config.TEST, config.test_pickle_file)
        processed_data = DataProcessor(orders.orders_df, routes.routes_df).process(include_all=True)
        single_predictor = LogisticReachSinglePredictor(config.intercept, config.coefficients)
        bulk_predictor = BulkPredictor(processed_data, single_predictor)
        predictions = bulk_predictor.predict_in_bulk()
        writer = Writer(predictions)
        writer.write()

    else:
        for chunk_df in orders.orders_df:
            route_ids = chunk_df['_id_oid'].unique()
            routes = Route(route_ids, config.MONGO_ENGINE, config.TEST, config.test_pickle_file)
            processed_data = DataProcessor(chunk_df, routes.routes_df).process(include_all=True)
            single_predictor = LogisticReachSinglePredictor(config.intercept, config.coefficients)
            bulk_predictor = BulkPredictor(processed_data, single_predictor)
            predictions = bulk_predictor.predict_in_bulk()
            writer = Writer(predictions)
            writer.write()


if __name__ == '__main__':
    main()
