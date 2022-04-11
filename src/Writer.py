import pandas as pd
from datetime import timedelta
import src.config


class Writer:

    def __init__(self, predictions: pd.DataFrame, engine, table_name, schema_name, table_cols):
        self.__predictions = predictions
        self.__engine = engine
        self.__table_name = table_name
        self.__schema_name = schema_name
        self.__table_columns = table_cols

    def __prepare_columns(self):
        self.__predictions = self.__predictions[['_id_oid', 'time', 'time_l', 'lat', 'lon']]
        self.__predictions.columns = self.__table_columns

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
