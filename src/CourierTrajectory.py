import datetime

import pandas as pd


class CourierTrajectory:

    def __init__(self, courier_ids: list, start_date: datetime.datetime, end_date: datetime.datetime):

        self.courier_ids = courier_ids
        self.start_date = start_date
        self.end_date = end_date

    def __get_trajectories(self):
        from src import ATHENA
        auto_df = pd.read_sql("""SELECT courierid as courier_id,
                                        acc,
                                        status,
                                        createdat as time,
                                        lat,
                                        lon,
                                        orderid as order_id
               FROM logs.event_getir_courier_autoreach
               WHERE dt BETWEEN %(start)s and %(end)s
                 and courierid in %(courier_ids)s
               order by createdat
            """, ATHENA, params={
            'courier_ids': self.courier_ids,
            'start': '{:%Y-%m-%dT%H-00-00Z}'.format(self.start_date),
            'end': '{:%Y-%m-%dT%H-00-00Z}'.format(self.end_date),
        })
        return auto_df

    def fetch(self):

        data = self.__get_trajectories()

        return data
