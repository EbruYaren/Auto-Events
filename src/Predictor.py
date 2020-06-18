import abc
from math import log, e
import pandas as pd


class SinglePredictor(abc.ABC):

    @abc.abstractmethod
    def predict(self):
        pass


class LogisticReachSinglePredictor(SinglePredictor):

    def __init__(self, intercept: float, coefficients: list):
        self.__intercept = intercept
        self.__coefficients = coefficients

    def predict(self, distance_bin: int, time_in_bin: float):
        Z = self.__coefficients[0] * distance_bin + self.__coefficients[1] * log(time_in_bin + 1) + self.__intercept
        output = 1 / (1 + e ** (-Z))

        return True if output >= .5 else False


class BulkPredictor:

    def __init__(self, processed_data: pd.DataFrame, predictor: SinglePredictor):
        self.__processed_data = processed_data
        self.__predictor = predictor

    def predict_in_bulk(self):
        self.__processed_data['is_reached'] = self.__processed_data.apply(
            lambda row: self.__predictor.predict(row['distance_bin'], row['time_passed_in_bin']), axis='columns')
        reached = self.__processed_data[self.__processed_data['is_reached']]
        reached['row_number'] = reached.groupby('_id_oid')['time'].rank(method='min')
        predictions = reached[reached['row_number'] == 1][['_id_oid', 'time', 'lat', 'lon']]

        return predictions
