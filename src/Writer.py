import pandas as pd
from datetime import timedelta
import src.config

class Writer:
    table_columns = ['order_id',
                     'predicted_reach_date',
                     'predicted_reach_dateL',
                     'latitude', 'longitude']

    def __init__(self, predictions: pd.DataFrame):
        self.__predictions = predictions

    def prepare_columns(self):
        self.__predictions['time_l'] = self.__predictions['time'] + timedelta(hours=3)
        self.__predictions = self.__predictions[['_id_oid', 'time', 'time_l', 'lat', 'lon']]
        self.__predictions.columns = self.table_columns


    def write(self):
        self.prepare_columns()
        """
        self.__predictions.to_sql(
            name=src.config.WRITE_TABLE_NAME,
            schema=src.config.SCHEMA_NAME,
            index=False,
            if_exists='append',
            method='multi',
            con=None
        )
        """