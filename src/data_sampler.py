import pandas as pd


class DataSampler():
    QUERY_TEMPLATE = """SELECT delivery_route_oid, _id_oid,
   checkoutdatel,
   deliver_date ,
   reach_date,
   client_location__coordinates_lon, client_location__coordinates_lat,
   deliver_location__coordinates_lon, deliver_location__coordinates_lat,
   reach_location__coordinates_lon, reach_location__coordinates_lat
FROM etl_market_order.marketorders o
WHERE status in (900, 1000)
AND checkoutdatel BETWEEN  '{start_date}' AND  '{end_date}' AND domaintype = 1
ORDER BY random()
LIMIT {sample_size}
    """
    QUERY_TEMPLATE_ROUTES = """
SELECT rr.*
FROM routes_route rr
WHERE route_id in {routes}
    """

    def __init__(self, etl_engine, bitest_engine):
        self.etl_engine = etl_engine
        self.bitest_engine = bitest_engine
        self.orders_df = None

    def _query_formatter(self, start_date, end_date, sample_size):
        return self.QUERY_TEMPLATE.format(start_date=start_date,
                                          end_date=end_date,
                                          sample_size=sample_size)

    def get_sample_orders_df(self, start_date, end_date, sample_size=10000):
        query = self._query_formatter(start_date, end_date, sample_size)
        print(query)
        df = pd.read_sql(query, self.etl_engine)
        self.orders_df = df
        return df

    def get_routes_of_orders(self):
        assert isinstance(self.orders_df,
                          pd.DataFrame), 'There are no sample orders. Get some with get_sample_orders_df'
        query = self._route_query_formatter()
        self.routes_df = pd.read_sql(query, self.bitest_engine)
        return self.routes_df

    def _concat_route_ids(self):
        assert isinstance(self.orders_df,
                          pd.DataFrame), 'There are no sample orders. Get some with get_sample_orders_df'
        return tuple(str(i) for i in self.orders_df['delivery_route_oid'])

    def _route_query_formatter(self):
        assert isinstance(self.orders_df,
                          pd.DataFrame), 'There are no sample orders. Get some with get_sample_orders_df'

        query = self.QUERY_TEMPLATE_ROUTES.format(routes=self._concat_route_ids())

        return query
