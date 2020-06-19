import pandas as pd
from datetime import timedelta
import src.config

class Writer:
    table_columns = ['order_id',
                     'predicted_reach_date',
                     'predicted_reach_dateL',
                     'latitude', 'longitude']

    def __init__(self, predictions: pd.DataFrame, engine, table_name, schema_name):
        self.__predictions = predictions
        self.__engine = engine
        self.__table_name = table_name
        self.__schema_name = schema_name

    def __prepare_columns(self):
        self.__predictions['time_l'] = self.__predictions['time'] + timedelta(hours=3)
        self.__predictions = self.__predictions[['_id_oid', 'time', 'time_l', 'lat', 'lon']]
        self.__predictions.columns = self.table_columns


    def write(self):
        self.__prepare_columns()
        self.__predictions.to_sql(
            name=self.__table_name,
            schema=self.__schema_name,
            index=False,
            if_exists='append',
            method='multi',
            con=self.__engine
        )
