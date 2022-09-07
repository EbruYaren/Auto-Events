import datetime
from datetime import datetime, timedelta

import pandas as pd


class Route:

    def __init__(self, start_date: str, end_date: str, route_id_list: list, is_test=False, engine_etl=None, ATHENA=None,
                 order_ids=list, domain_type=int):

        self.__route_id_list = route_id_list
        self.__is_test = is_test
        self.__engine_etl = engine_etl
        self.__start_date = start_date
        self.__end_date = end_date
        self.__ATHENA = ATHENA
        self.__order_ids = order_ids
        self.__domain_type = domain_type


    def __get_trajectories(self, order_filter=None, select_filter=None):

        if self.__domain_type in (1, 4):
            if len(self.__order_ids) == 0:
                order_filter = ''
            else:
                in_clause = self._create_in_clause(self.__order_ids)
                order_filter = 'market_order_id IN {} OR prev_market_order_id IN {}'.format(in_clause, in_clause)

            select_filter = """CASE WHEN orderid != 'null' THEN orderid END AS market_order_id,   \
                            COALESCE(CASE WHEN orderid != 'null' THEN orderid END, lag(CASE WHEN orderid != 'null' THEN 
                            orderid END, 1) IGNORE NULLS OVER(PARTITION BY courierid ORDER BY createdat)) AS prev_market_order_id"""

        elif self.__domain_type == 2:
            if len(self.__order_ids) == 0:
                order_filter = ''
            else:
                in_clause = self._create_in_clause(self.__order_ids)
                order_filter = 'food_order_id IN {} OR prev_food_order_id IN {}'.format(in_clause, in_clause)
            select_filter = """CASE WHEN foodorder != 'null' THEN foodorder END AS food_order_id,   \
                             COALESCE(CASE WHEN foodorder != 'null' THEN foodorder END, lag(CASE WHEN foodorder != 'null' THEN 
                             foodorder END, 1) IGNORE NULLS OVER(PARTITION BY courierid ORDER BY createdat)) AS prev_food_order_id"""

        elif self.__domain_type == 6:
            if len(self.__order_ids) == 0:
                order_filter = ''
            else:
                in_clause = self._create_in_clause(self.__order_ids)
                order_filter = 'artisan_order_id IN {} OR prev_artisan_order_id IN {}'.format(in_clause, in_clause)
            select_filter = """CASE WHEN artisanorder != 'null' THEN artisanorder END AS artisan_order_id,   \
                             COALESCE(CASE WHEN artisanorder != 'null' THEN artisanorder END, lag(CASE WHEN artisanorder != 'null' THEN 
                             artisanorder END, 1) IGNORE NULLS OVER(PARTITION BY courierid ORDER BY createdat)) AS prev_artisan_order_id"""


        # Getting routes from autoreach table.
        # Used nested query because of Athena does not support window operations or aggregations in WHERE clause
        query = f"""
         SELECT *
         FROM (
            SELECT courierid                   AS courier_oid,
                   acc,
                   status,
                   createdat                   AS "time",
                   lat,
                   lon,
                   {select_filter}
            FROM logs.event_getir_courier_autoreach
            WHERE dt BETWEEN %(start)s and %(end)s
            ORDER BY createdat
        ) a
        WHERE {order_filter} 
"""

        routes_df = pd.read_sql(query, self.__ATHENA, params={
            'select_filter': select_filter,
            'start': '{:%Y-%m-%dT%H-00-00Z}'.format(datetime.fromisoformat(self.__start_date)-timedelta(hours=2)),
            'end': '{:%Y-%m-%dT%H-00-00Z}'.format(datetime.fromisoformat(self.__end_date)+timedelta(hours=1)),
            'order_filter': order_filter
        }, parse_dates=['time'])

        routes_df['time'] = routes_df['time'].apply(lambda x: x.replace(tzinfo=None))
        routes_df['route_id'] = routes_df.bfill(axis=1).iloc[:, -2]
        routes_df['index'] = routes_df.sort_values(['route_id', 'time']).groupby(['route_id']).cumcount() + 1
        return routes_df




    def _create_in_clause(self, order_ids: list):
        if len(order_ids) == 0:
            return ''
        ids = ["'" + str(id_) + "'" for id_ in order_ids]

        return "(" + ','.join(ids) + ")"


    def fetch_routes_df(self):

        data = self.__get_trajectories()

        return data

