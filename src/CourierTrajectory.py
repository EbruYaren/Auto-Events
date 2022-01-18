import datetime
import traceback

import pandas as pd


class CourierTrajectory:

    def __init__(self, courier_ids: list, start_date: str, end_date: str):

        self.courier_ids = courier_ids
        self.start_date = start_date
        self.end_date = end_date

    def __get_trajectories(self):
        from src import ATHENA
        auto_df = pd.read_sql("""
        SELECT acc,
               "time",
               lat,
               lon,
               prev_order_id as _id_oid
        FROM (
            SELECT *,
                   COALESCE(order_id, lag(order_id, 1) IGNORE NULLS OVER(PARTITION BY courier_oid ORDER BY "time")) AS prev_order_id
        
            FROM (
                SELECT courierid                                    AS courier_oid,
                       acc,
                       status,
                       createdat                                    AS "time",
                       lat,
                       lon,
                       CASE WHEN orderid != 'null' THEN orderid END AS order_id
                FROM logs.event_getir_courier_autoreach
                WHERE dt BETWEEN '{start}' and '{end}'
                 and courierid in {courier_ids}
                ORDER BY createdat
            )
        )
        WHERE status = 900 and prev_order_id is not null
        """.format(**{
            'courier_ids': str(tuple(self.courier_ids)).replace(',)', ')'),
            'start': '{:%Y-%m-%dT%H-00-00Z}'.format(datetime.datetime.fromisoformat(self.start_date)),
            'end': '{:%Y-%m-%dT%H-00-00Z}'.format(datetime.datetime.fromisoformat(self.end_date)),
        }), ATHENA, parse_dates=['time'])
        auto_df['time'] = auto_df['time'].apply(lambda x: x.replace(tzinfo=None))
        return auto_df

    def fetch(self):
        try:
            data = self.__get_trajectories()
        except:
            print(traceback.format_exc())
            data = pd.DataFrame({'_id_oid': []})

        return data
