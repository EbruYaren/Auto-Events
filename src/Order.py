import pandas as pd
import sqlalchemy


class Order:
    QUERY_TEMPLATE = """SELECT delivery_route_oid, _id_oid, courier_courier_oid,
           checkoutdatel,
           deliver_date ,
           reach_date,
           client_location__coordinates_lon, client_location__coordinates_lat,
           deliver_location__coordinates_lon, deliver_location__coordinates_lat,
           reach_location__coordinates_lon, reach_location__coordinates_lat,
           delivery_address_location__coordinates_lon, delivery_address_location__coordinates_lat,
           warehouse_location__coordinates_lon, warehouse_location__coordinates_lat,
            first_value(handover_date) over (partition by delivery_job_oid order by delivery_batch_index desc rows unbounded preceding ) handover_date,
           delivery_job_oid,
           delivery_batch_index
        FROM etl_market_order.marketorders o
        WHERE status in (900, 1000)
        AND date_add('hour', 3, deliver_date) BETWEEN  '{start_date}' AND  '{end_date}' AND domaintype in (1,3)
        {courier_filter}
    """

    def __init__(self, start_date: str, end_date: str, etl_engine: sqlalchemy.engine.base.Engine, courier_ids=[],
                 chunk_size=1000):

        self.__start_date = start_date
        self.__end_date = end_date
        self.__etl_engine = etl_engine
        self.__courier_ids = courier_ids
        self.__chunk_size = chunk_size

    def _create_in_clause(self, id_s: list):
        if len(id_s) == 0:
            return ''
        ids = ["'" + str(id_) + "'" for id_ in id_s]

        return "(" + ','.join(ids) + ")"

    def _query_formatter(self):
        if len(self.__courier_ids) == 0:
            courier_filter = ''
        else:
            in_clause = self._create_in_clause(self.__courier_ids)
            courier_filter = 'AND courier_courier_oid IN {}'.format(in_clause)

        return self.QUERY_TEMPLATE.format(start_date=self.__start_date,
                                          end_date=self.__end_date,
                                          courier_filter=courier_filter)

    def fetch_orders_df(self):
        query = self._query_formatter()
        df_iterable = pd.read_sql(query, self.__etl_engine, chunksize=self.__chunk_size)
        if self.__chunk_size == None:
            return [df_iterable]
        return df_iterable
