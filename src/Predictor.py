import abc
from math import log, e
import pandas as pd


class SinglePredictor(abc.ABC):

    @abc.abstractmethod
    def predict(self):
        raise NotImplemented


class ReachLogisticReachSinglePredictor(SinglePredictor):

    def __init__(self, intercept: float, coefficients: list):
        self.__intercept = intercept
        self.__coefficients = coefficients

    def predict(self, distance_bin: int, time_in_bin: float):
        Z = self.__coefficients[0] * distance_bin + self.__coefficients[1] * log(time_in_bin + 1) + self.__intercept
        output = 1 / (1 + e ** (-Z))

        return True if output >= .5 else False


class ReachBulkPredictor:

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


class DepartLogisticReachSinglePredictor(SinglePredictor):

    def __init__(self, intercept: float, coefficients: list):
        self.__intercept = intercept
        self.__coefficients = coefficients

    def predict(
            self, smooth_speed_to_warehouse: float, smooth_speed_to_prev_event: float,
            distance_to_warehouse_log: float):
        Z = self.__coefficients[0] * smooth_speed_to_warehouse + self.__coefficients[1] * smooth_speed_to_prev_event + \
            self.__coefficients[2] * distance_to_warehouse_log + self.__intercept
        output = 1 / (1 + e ** (-Z))

        return True if output >= .5 else False


class DepartBulkPredictor:

    def __init__(self, processed_data: pd.DataFrame, predictor: SinglePredictor):
        self.__processed_data = processed_data
        self.__predictor = predictor

    def predict_in_bulk(self):
        df = self.__processed_data.copy()

        df['predictions'] = df.appy(
            lambda row: self.__predictor.predict(
                row['smooth_speed_to_warehouse'],
                row['smooth_speed_to_prev_event'],
                row['dbw_warehouse_log']
            ))
        df['rn'] = df.groupby('_id_oid')['index'].rank(method='min')  # check if ture
        true_preds = df[(df['predictions']) & (df['time'] > df['handover_date'])]
        true_preds['true_rn'] = true_preds.groupby('_id_oid')['index'].rank(method='min')
        true_preds = true_preds[true_preds['true_rn'] == 1]
        pred_rows = true_preds[['_id_oid', 'rn']]
        pred_rows['last_false'] = pred_rows['rn'].apply(lambda r: r - 1 if r > 1 else r)
        pred_rows.drop('rn', axis='columns', inplace=True)
        df = df.merge(pred_rows, on='_id_oid')
        labeled_times = df[df['rn'] == df['last_false']][['_id_oid', 'time', 'lat', 'lon']] \
            .drop_duplicates()

        return labeled_times
