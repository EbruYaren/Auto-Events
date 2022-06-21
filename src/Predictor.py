import abc
from math import log, e
import pandas as pd
import pytz


class SinglePredictor(abc.ABC):

    @abc.abstractmethod
    def predict(self):
        pass


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
        predictions = reached[reached['row_number'] == 1][['_id_oid', 'time', 'lat', 'lon', 'time_zone']]
        if predictions.size > 0:
            predictions['time_l'] = predictions.apply(
               lambda row: row.time.replace(tzinfo=pytz.utc).astimezone(row.time_zone).strftime('%Y-%m-%dT%H:%M:%S.%f')
               , axis='columns')
        else:
            predictions.rename(columns={'time_zone': 'time_l'}, inplace=True)


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

        try:
            output = 1 / (1 + e ** (-Z))
        except OverflowError as err:
            return False

        return True if output >= .5  else False


def get_predicted_depart_rows(true_preds: pd.DataFrame, df: pd.DataFrame, domain_type: int):
    pred_rows = true_preds[['_id_oid', 'rn']]
    pred_rows['last_false'] = pred_rows['rn'].apply(lambda r: r - 1 if r > 1 else r)
    pred_rows.drop('rn', axis='columns', inplace=True)
    df = df.merge(pred_rows, on='_id_oid')
    labeled_times = df[(df['rn'] == df['last_false'])]
    if domain_type not in (2, 6):
        labeled_times = labeled_times[(labeled_times['time'] > labeled_times['onway_date'])]

    return labeled_times.sort_values(['_id_oid', 'time']).groupby(['_id_oid']).first().reset_index()


class DepartBulkPredictor:

    def __init__(self, processed_data: pd.DataFrame, predictor: SinglePredictor, max_distance_to_warehouse:float,
                 orders_df: pd.DataFrame, domain_type: int):
        self.__processed_data = processed_data
        self.__predictor = predictor
        self.__max_distance_to_warehouse = max_distance_to_warehouse
        self.__orders_df = orders_df
        self.__domain_type = domain_type

    def predict_in_bulk(self):
        if self.__processed_data.empty is False:
            df = self.__processed_data.copy()

            df['predictions'] = df.apply(
                lambda row: self.__predictor.predict(
                    row['smooth_speed_to_warehouse'],
                    row['smooth_speed_to_prev_event'],
                    row['dbw_warehouse_log']
                ), axis='columns')
            df['rn'] = df.groupby('_id_oid')['index'].rank(method='min')  # check if true
            #df['prev_time'] = df.groupby('_id_oid')['time'].shift(1)
            df['prev_distance_to_warehouse'] = df.groupby('_id_oid')['distance_to_warehouse'].shift(1)
            true_preds = df[(df['predictions']) &
                            (df['prev_distance_to_warehouse'] < self.__max_distance_to_warehouse)]
            if self.__domain_type not in (2, 6):
                true_preds = true_preds[true_preds['time'] > true_preds['onway_date']]

            true_preds['true_rn'] = true_preds.groupby('_id_oid')['index'].rank(method='min')

            labeled_times = get_predicted_depart_rows(true_preds, df, domain_type=self.__domain_type)

            if labeled_times.size > 0:
                labeled_times['time_l'] = labeled_times.apply(
                    lambda row: row.time.replace(tzinfo=pytz.utc).astimezone(row.time_zone).strftime('%Y-%m-%dT%H:%M:%S.%f')
                    , axis='columns')
            else:
                labeled_times.rename(columns={'time_zone': 'time_l'}, inplace=True)

            labeled_times = labeled_times[['_id_oid', 'time', 'lat', 'lon', 'time_l']].drop_duplicates()

            # Getting unpredicted batched orders data for market orders
            if self.__domain_type not in (2, 6):
                return self.get_unpredicted_batched_orders(labeled_times).reset_index(drop=True)
            else:
                return labeled_times.reset_index(drop=True)

    def get_unpredicted_batched_orders(self, predictions):
        # Getting processed data before prediction process and group by _id_oid to eliminate multiple routes
        data = self.__orders_df.groupby(['_id_oid']).max().reset_index()
        # Merging predicted and processed data on _id_oid  to get unpredicted batched orders
        df = data.merge(predictions, left_on="_id_oid", right_on="_id_oid", how="left")
        rows = []
        # For each delivery_job_oid in orders:
        for delivery_job_oid, row in df.groupby('delivery_job_oid'):
            # Getting first batch
            first_row = row.loc[row['delivery_batch_index'] == 1]
            if first_row.empty is False:
                rows.append({
                    '_id_oid': first_row._id_oid.values[0],
                    'time': first_row['time'].values[0],
                    'time_l': first_row['time_l'].values[0],
                    'lat': first_row['lat'].values[0],
                    'lon': first_row['lon'].values[0]
                })
                # If first batch was predicted:
                if any(first_row.time.notna()):
                    # Getting unpredicted batches after first batch
                    batches = row[(row['delivery_batch_index'] > 1)]
                    batches = batches.reset_index(drop=True)
                    if batches.size > 0:
                        # For each unpredicted batch, first batch data is being appended to rows.
                        for i, r in batches.iterrows():
                            rows.append({
                                '_id_oid': r._id_oid,
                                'time': first_row['time'].values[0],
                                'time_l': first_row['time_l'].values[0],
                                'lat': first_row['lat'].values[0],
                                'lon': first_row['lon'].values[0]
                            })

        rows = pd.DataFrame(rows)
        return rows





class DepartFromClientLogisticReachSinglePredictor(SinglePredictor):

    def __init__(self, intercept: float, coefficients: list):
        self.__intercept = intercept
        self.__coefficients = coefficients

    def predict(
            self, smooth_speed_to_client: float, smooth_speed_to_prev_event: float,
            distance_to_client_log: float):
        Z = self.__coefficients[0] * smooth_speed_to_client + self.__coefficients[1] * smooth_speed_to_prev_event + \
            self.__coefficients[2] * distance_to_client_log + self.__intercept

        try:
            output = 1 / (1 + e ** (-Z))
        except OverflowError as err:
            return False

        return True if output >= .5 else False




class DepartFromClientBulkPredictor:

    def __init__(self, processed_data: pd.DataFrame, predictor: SinglePredictor, max_distance_to_client:float,
                 reach_predictions_df: pd.DataFrame):
        self.__processed_data = processed_data.merge(reach_predictions_df, on='_id_oid', how='left')
        self.__predictor = predictor
        self.__max_distance_to_client = max_distance_to_client

    def predict_in_bulk(self):
        df = self.__processed_data.copy()

        df['predictions'] = df.apply(
            lambda row: self.__predictor.predict(
                row['smooth_speed_to_client'],
                row['smooth_speed_to_prev_event'],
                row['dbw_client_log']
            ), axis='columns')
        df['rn'] = df.groupby('_id_oid')['time'].rank(method='min')  # check if true
        df['prev_distance_to_client'] = df.groupby('_id_oid')['distance_to_client'].shift(1)
        true_preds = df[(df['predictions']) &
                        (df['time'] >= df['reach_date']) &
                        # (df['time'] >= df['predicted_reach_date']) &
                        (df['prev_distance_to_client'] < self.__max_distance_to_client)]
        true_preds['true_rn'] = true_preds.groupby('_id_oid')['time'].rank(method='min')
        true_preds = true_preds[true_preds['true_rn'] == 1]
        pred_rows = true_preds[['_id_oid', 'rn']]
        pred_rows['last_false'] = pred_rows['rn'].apply(lambda r: r - 1 if r > 1 else r)
        pred_rows.drop('rn', axis='columns', inplace=True)
        df = df.merge(pred_rows, on='_id_oid')

        labeled_times = df[((df['time'] >= df['reach_date']) & ((df['rn'] == df['last_false']) | (df['rn'] == (df['last_false'] + 1))))][['_id_oid', 'time', 'lat', 'lon', 'time_zone', 'rn']].drop_duplicates()
        labeled_times['row_number'] = labeled_times.sort_values(['_id_oid', 'rn']).groupby(['_id_oid']).cumcount() + 1
        labeled_times = labeled_times[labeled_times.row_number == 1][['_id_oid', 'time', 'lat', 'lon', 'time_zone']]
        if labeled_times.size > 0:
            labeled_times['time_l'] = labeled_times.apply(
               lambda row: row.time.replace(tzinfo=pytz.utc).astimezone(row.time_zone).strftime('%Y-%m-%dT%H:%M:%S.%f')
               , axis='columns')

        else:
            labeled_times.rename(columns={'time_zone': 'time_l'}, inplace=True)


        return labeled_times
