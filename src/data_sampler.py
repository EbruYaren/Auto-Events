import pandas as pd
from bson import ObjectId

class DataSampler():
    QUERY_TEMPLATE = """SELECT delivery_route_oid, _id_oid, courier_courier_oid,
   checkoutdatel,
   deliver_date ,
   reach_date,
   client_location__coordinates_lon, client_location__coordinates_lat,
   deliver_location__coordinates_lon, deliver_location__coordinates_lat,
   reach_location__coordinates_lon, reach_location__coordinates_lat,
   delivery_address_location__coordinates_lon, delivery_address_location__coordinates_lat
FROM etl_market_order.marketorders o
WHERE status in (900, 1000)
AND checkoutdatel BETWEEN  '{start_date}' AND  '{end_date}' AND domaintype = 1
{courier_filter}
ORDER BY random()
LIMIT {sample_size}
    """
    QUERY_TEMPLATE_ROUTES = """
SELECT rr.*
FROM routes_route rr
WHERE route_id in {routes}
ORDER BY route_id, "time"
    """

    def __init__(self, etl_engine, bitest_engine, mongo_client=None):
        self.etl_engine = etl_engine
        self.bitest_engine = bitest_engine
        self.mongo_client = mongo_client
        self.routes_collection = mongo_client.getir.routes
        self.orders_df = None

    @staticmethod
    def _convert_cursor_to_routes_df(cursor):
        all_datas = []
        for data in cursor:
            for i, location in enumerate(data['route']):
                temp_dict = {}
                temp_dict['route_id'] = str(data['_id'])
                temp_dict['index'] = i
                temp_dict['lon'] = location['coordinates'][0]
                temp_dict['lat'] = location['coordinates'][1]
                temp_dict['time'] = location['time']
                temp_dict['acc'] = location['acc']
                all_datas.append(temp_dict)
        return pd.DataFrame(all_datas)

    def _query_formatter(self, start_date, end_date, sample_size, courier_ids):
        if len(courier_ids) == 0:
            courier_filter = ''
        else:
            in_clause = self._create_in_clause(courier_ids)
            courier_filter = 'AND courier_courier_oid IN {}'.format(in_clause)

        return self.QUERY_TEMPLATE.format(start_date=start_date,
                                          end_date=end_date,
                                          sample_size=sample_size,
                                          courier_filter=courier_filter)

    def get_sample_orders_df(self, start_date, end_date, sample_size=10000, courier_ids=[]):

        query = self._query_formatter(start_date, end_date, sample_size, courier_ids)
        df = pd.read_sql(query, self.etl_engine)
        self.orders_df = df
        return df

    def _create_in_clause(self, id_s: list):
        if len(id_s) == 0:
            return ''
        ids = ["'" + str(id_) + "'" for id_ in id_s]
        return "(" + ','.join(ids) + ")"

    def get_routes_of_orders(self):
        assert isinstance(self.orders_df,
                          pd.DataFrame), 'There are no sample orders. Get some with get_sample_orders_df'
        query = self._route_query_formatter()
        self.routes_df = pd.read_sql(query, self.bitest_engine)
        return self.routes_df

    def get_routes_of_orders_mongo(self):
        assert isinstance(self.orders_df,
                          pd.DataFrame), 'There are no sample orders. Get some with get_sample_orders_df'
        route_ids = self.orders_df['delivery_route_oid'].to_list()
        route_ids = list(map(ObjectId, route_ids))
        match = {"_id": {"$in": route_ids}}
        return self._convert_cursor_to_routes_df(collection.find(match))

    def _concat_route_ids(self):
        assert isinstance(self.orders_df,
                          pd.DataFrame), 'There are no sample orders. Get some with get_sample_orders_df'
        return tuple(str(i) for i in self.orders_df['delivery_route_oid'])

    def _route_query_formatter(self):
        assert isinstance(self.orders_df,
                          pd.DataFrame), 'There are no sample orders. Get some with get_sample_orders_df'

        query = self.QUERY_TEMPLATE_ROUTES.format(routes=self._concat_route_ids())

        return query

