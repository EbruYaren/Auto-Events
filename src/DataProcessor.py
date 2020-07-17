import numpy as np
import pandas as pd
from math import radians, cos, sin, asin, sqrt, log, e


class DataProcessor():

    @staticmethod
    def haversine(lon1, lat1, lon2, lat2):
        """
        Calculate the great circle distance between two points
        on the earth (specified in decimal degrees)
        """
        # convert decimal degrees to radians
        lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

        # haversine formula
        dlon = lon2 - lon1
        dlat = lat2 - lat1
        a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
        c = 2 * asin(sqrt(a))
        r = 6371  # Radius of earth in kilometers. Use 3956 for miles
        return c * r

    def process(self):
        raise NotImplemented


class ReachDataProcessor(DataProcessor):
    fibonacci_series = [0, 1, 2, 3, 5, 8, 13, 21]
    returning_columns = ['_id_oid', 'delivery_route_oid', 'courier_courier_oid', 'reach_date',
                         'time', 'distance_bin', 'time_passed_in_bin', 'tbe',
                         'dbw_reach_client', 'dbw_reach_client_bin', 'lat', 'lon']

    def __init__(self, orders: pd.DataFrame, routes: pd.DataFrame, fibonacci_base, minimum_location_limit):

        self.minimum_location_limit = minimum_location_limit
        self.merged_df = routes.merge(orders, left_on="route_id", right_on="delivery_route_oid", how="inner")
        self.fibonacci_base = fibonacci_base

    @staticmethod
    def convert_to_seconds(x):
        try:
            return round(x.seconds)
        except:
            return np.nan

    def haversine_apply(self, row):
        dist = self.haversine(row['lon'], row['lat'],
                              row['delivery_address_location__coordinates_lon'],
                              row['delivery_address_location__coordinates_lat'])
        return round(dist * 1000)

    def find_distance_bin(self, distance):
        assert distance >= 0, 'distance should be positive.'
        for i, f in enumerate(self.fibonacci_series):
            if distance < f * self.fibonacci_base:
                return i - 1
        return len(self.fibonacci_series) - 1

    def process(self, include_all=False):

        m_df = self.merged_df
        counts = m_df.groupby('route_id')['time'].count()
        filtered_ids = counts[counts >= self.minimum_location_limit].index
        m_df = m_df[m_df['route_id'].isin(filtered_ids)]
        m_df['distance'] = m_df.apply(lambda r: self.haversine_apply(r), axis=1)
        m_df['distance_bin'] = m_df['distance'].apply(self.find_distance_bin)
        m_df['first_location_time'] = m_df.groupby(['delivery_route_oid'])['time'].transform(np.min)
        m_df['last_location_time'] = m_df.groupby(['delivery_route_oid'])['time'].transform(np.max)
        m_df['next_location_time'] = m_df.sort_values(['delivery_route_oid', 'time']).groupby(['delivery_route_oid'])[
            'time'].shift(-1)
        m_df['tbe'] = (m_df['next_location_time'] - m_df['time']).apply(self.convert_to_seconds).fillna(0)
        m_df['previous_distance_bin'] = \
            m_df.sort_values(['delivery_route_oid', 'time']).groupby(['delivery_route_oid'])['distance_bin'].shift(1)
        m_df['is_distance_bin_changed'] = m_df['distance_bin'] != m_df['previous_distance_bin']
        m_df['distance_bin_group'] = m_df.sort_values(['delivery_route_oid', 'time']).groupby(['delivery_route_oid'])[
            'is_distance_bin_changed'].cumsum()
        m_df['time_passed_in_bin'] = m_df.sort_values(['delivery_route_oid', 'distance_bin_group', 'time']).groupby(
            ['delivery_route_oid', 'distance_bin_group'])['tbe'].cumsum()
        m_df['dbw_reach_client'] = m_df.apply(lambda x: self.haversine(x['lon'], x['lat'],
                                                                       x['reach_location__coordinates_lon'],
                                                                       x['reach_location__coordinates_lat']) * 1000,
                                              axis=1)
        m_df['dbw_reach_client_bin'] = m_df['dbw_reach_client'].apply(self.find_distance_bin)
        if include_all:
            return m_df
        else:
            return m_df[self.returning_columns]


class DepartDataProcessor(DataProcessor):

    def __init__(self, orders: pd.DataFrame, routes: pd.DataFrame):
        self.merged_df = routes.merge(orders, left_on="route_id", right_on="delivery_route_oid", how="inner")

    @staticmethod
    def get_movement_info(data: pd.DataFrame):
        data.sort_values(['_id_oid', 'index'], inplace=True)

        data['tbe'] = data.groupby('_id_oid').apply(
            lambda route: (route['time'] - route['time'].shift(1)).dt.total_seconds()).droplevel(0)

        data['distance_to_warehouse'] = data.apply(
            lambda row: DataProcessor.haversine(
                row['lon'], row['lat'],
                row['warehouse_location__coordinates_lon'],
                row['warehouse_location__coordinates_lat']),
            axis='columns')

        data['dist_to_warehouse_diff'] = data.groupby('_id_oid').apply(
            lambda route: route['distance_to_warehouse'] - route['distance_to_warehouse'].shift(1)).droplevel(0)

        data['speed_to_warehouse'] = data['dist_to_warehouse_diff'] / data['tbe']

        data['prev_lat'] = data.groupby('_id_oid')['lat'].shift(1)
        data['prev_lon'] = data.groupby('_id_oid')['lon'].shift(1)

        data['distance_to_prev_event'] = data.apply(
            lambda row: DataProcessor.haversine(
                row['lon'], row['lat'],
                row['prev_lon'],
                row['prev_lat']),
            axis='columns')

        data['speed_to_prev_event'] = data['distance_to_prev_event'] / data['tbe']

        data['smooth_speed_to_warehouse'] = data.groupby('_id_oid')['speed_to_warehouse']\
            .rolling(3,center=True).mean().droplevel(0)
        data['smooth_speed_to_prev_event'] = data.groupby('_id_oid')['speed_to_prev_event']\
            .rolling(3, center=True).mean().droplevel(0)
        data['dbw_warehouse_log'] = data['distance_to_warehouse'].apply(log)

        return data.dropna()

    def process(self):
        return DepartDataProcessor.get_movement_info(self.merged_df)
