import numpy as np
import pandas as pd
from math import radians, cos, sin, asin, sqrt, log, e
import matplotlib.pyplot as plt
import abc


class DataProcessor(abc.ABC):

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

    @staticmethod
    def remove_outliers(df: pd.DataFrame, col_name: str, whisker=1.5):
        q = df[col_name].quantile([.25, .75])
        iqr = q[.75] - q[.25]
        lower_bound = q[.25] - whisker * iqr
        upper_bound = q[.75] + whisker * iqr

        return df[df[col_name].between(lower_bound, upper_bound)]

    @staticmethod
    def plot_story(
            route: pd.DataFrame, story_cols: list, vline_cols=[], figsize=(12, 6),
            story_linewidth=1, story_markersize=3, vlinewidth=3):

        colors = ['w', 'b', 'y', 'r', 'g', 'gray']

        fig, ax = plt.subplots(figsize=figsize)

        for i, col in enumerate(story_cols):
            route[col].plot(lw=story_linewidth, marker='o', markersize=story_markersize, c=colors[i], ax=ax, label=col)

        for i, col in enumerate(vline_cols):
            val = route[col].values[0]
            label = "{}: {}".format(col, val)
            ax.axvline(val, c=colors[i + 1], lw=vlinewidth, label=label)

        ax.legend()
        return fig, ax

    @abc.abstractmethod
    def process(self):
        pass


class ReachDataProcessor(DataProcessor):
    unique_fibonacci_numbers = [0, 1, 2, 3, 5, 8, 13, 21]
    returning_columns = ['_id_oid', 'delivery_route_oid', 'courier_courier_oid', 'reach_date',
                         'time', 'distance_bin', 'time_passed_in_bin', 'tbe',
                         'lat', 'lon', 'time_zone']

    def __init__(self, fibonacci_base, minimum_location_limit, merged_df: pd.DataFrame, domain_type: int):

        self.minimum_location_limit = minimum_location_limit
        self.merged_df = merged_df
        self.fibonacci_base = fibonacci_base
        self.domain_type = domain_type

    @staticmethod
    def convert_to_seconds(x):
        try:
            return round(x.seconds)
        except:
            return np.nan

    def haversine_apply(self, row):
        source_lon = 'delivery_address_location__coordinates_lon'
        source_lat = 'delivery_address_location__coordinates_lat'
        dist = self.haversine(row['lon'], row['lat'],
                              row[source_lon],
                              row[source_lat])
        return round(dist * 1000)

    def find_distance_bin(self, distance):
        assert distance >= 0, 'distance should be non-negative.'
        for i, f in enumerate(self.unique_fibonacci_numbers):
            if distance < f * self.fibonacci_base:
                return i - 1
        return len(self.unique_fibonacci_numbers) - 1

    def process(self, include_all=False):

        m_df = self.merged_df
        counts = m_df.groupby('route_id')['time'].count()
        filtered_ids = counts[counts >= self.minimum_location_limit].index
        m_df = m_df[m_df['route_id'].isin(filtered_ids)]
        m_df['distance'] = m_df.apply(lambda r: self.haversine_apply(r), axis=1)
        m_df['distance_bin'] = m_df['distance'].apply(self.find_distance_bin)
        m_df['first_location_time'] = m_df.groupby(['route_id'])['time'].transform(np.min)
        m_df['last_location_time'] = m_df.groupby(['route_id'])['time'].transform(np.max)
        m_df['next_location_time'] = m_df.sort_values(['route_id', 'time']).groupby(['route_id'])[
            'time'].shift(-1)
        m_df['tbe'] = (m_df['next_location_time'] - m_df['time']).apply(self.convert_to_seconds).fillna(0)
        m_df['previous_distance_bin'] = \
            m_df.sort_values(['route_id', 'time']).groupby(['route_id'])['distance_bin'].shift(1)
        m_df['is_distance_bin_changed'] = m_df['distance_bin'] != m_df['previous_distance_bin']
        m_df['distance_bin_group'] = m_df.sort_values(['route_id', 'time']).groupby(['route_id'])[
            'is_distance_bin_changed'].cumsum()
        m_df['time_passed_in_bin'] = m_df.sort_values(['route_id', 'distance_bin_group', 'time']).groupby(
            ['route_id', 'distance_bin_group'])['tbe'].cumsum()
        if include_all:
            return m_df
        else:
            return m_df[self.returning_columns]


class DepartDataProcessor(DataProcessor):

    def __init__(self, minimum_location_limit: int, domain: str, merged_df: pd.DataFrame):
        self.merged_df = merged_df
        self.minimum_location_limit = minimum_location_limit
        self.domain = domain

    @staticmethod
    def get_movement_info(data: pd.DataFrame, domain: str):
        data.sort_values(['_id_oid', 'index'], inplace=True)
        print('Depart data shape: ', data.shape)
        print('Depart data order count: ', data._id_oid.nunique())

        if len(data) > 0:
            if data._id_oid.nunique() == 1:
                    data['tbe'] = (data['time'] - data['time'].shift(1)).dt.total_seconds()
            else:
                data['tbe'] = data.groupby('_id_oid').apply(
                    lambda route: (route['time'] - route['time'].shift(1)).dt.total_seconds()).droplevel(0)

            if domain == 'depart_from_merchant':
                source_lat = 'restaurantloc_lat'
                source_lon = 'restaurantloc_lon'
            else:
                source_lat = 'warehouse_location__coordinates_lat'
                source_lon = 'warehouse_location__coordinates_lon'

            data['distance_to_warehouse'] = data.apply(
                lambda row: DataProcessor.haversine(
                    row['lon'], row['lat'],
                    row[source_lon],
                    row[source_lat]) * 1000,
                axis='columns')


            if data._id_oid.nunique() == 1:
                data['dist_to_warehouse_diff'] = data['distance_to_warehouse'] - data['distance_to_warehouse'].shift(1)
            else:
                data['dist_to_warehouse_diff'] = data.groupby('_id_oid').apply(
                    lambda route: route['distance_to_warehouse'] - route['distance_to_warehouse'].shift(1)).droplevel(0)

            data['speed_to_warehouse'] = data['dist_to_warehouse_diff'] / data['tbe']

            data['prev_lat'] = data.groupby('_id_oid')['lat'].shift(1)
            data['prev_lon'] = data.groupby('_id_oid')['lon'].shift(1)

            data['distance_to_prev_event'] = data.apply(
                lambda row: DataProcessor.haversine(
                    row['lon'], row['lat'],
                    row['prev_lon'],
                    row['prev_lat']) * 1000,
                axis='columns')

            data['speed_to_prev_event'] = data['distance_to_prev_event'] / data['tbe']

            data['smooth_speed_to_warehouse'] = data.groupby('_id_oid')['speed_to_warehouse'] \
                .rolling(3, center=True).mean().droplevel(0)
            data['smooth_speed_to_prev_event'] = data.groupby('_id_oid')['speed_to_prev_event'] \
                .rolling(3, center=True).mean().droplevel(0)
            data['dbw_warehouse_log'] = data['distance_to_warehouse'].apply(log)

            return data.dropna()
        else:
            return pd.DataFrame([])

    def process(self):
        m_df = self.merged_df
        counts = m_df.groupby('route_id')['time'].count()
        filtered_ids = counts[counts >= self.minimum_location_limit].index
        m_df = m_df[m_df['route_id'].isin(filtered_ids)]

        return DepartDataProcessor.get_movement_info(m_df, self.domain)


class DepartFromClientDataProcessor(DataProcessor):

    def __init__(self, orders: pd.DataFrame, routes: pd.DataFrame, minimum_location_limit: int, domain_type: int, merged_df: pd.DataFrame):
        self.routes = routes
        self.orders = orders
        self.minimum_location_limit = minimum_location_limit
        self.domain_type = domain_type
        self.merged_df = merged_df

    @staticmethod
    def get_movement_info(data: pd.DataFrame):
        data.sort_values(['_id_oid', 'time'], inplace=True)

        if data._id_oid.nunique() == 1:
            data['tbe'] = (data['time'] - data['time'].shift(1)).dt.total_seconds()
        else:
            data['tbe'] = data.groupby('_id_oid').apply(
                lambda route: (route['time'] - route['time'].shift(1)).dt.total_seconds()).droplevel(0)

        data['distance_to_client'] = data.apply(
            lambda row: DataProcessor.haversine(
                row['lon'], row['lat'],
                row['delivery_address_location__coordinates_lon'],
                row['delivery_address_location__coordinates_lat']) * 1000,
            axis='columns')

        if data._id_oid.nunique() == 1:
            data['dist_to_client_diff'] = data['distance_to_client'] - data['distance_to_client'].shift(1)
        else:
            data['dist_to_client_diff'] = data.groupby('_id_oid').apply(
                lambda route: route['distance_to_client'] - route['distance_to_client'].shift(1)).droplevel(0)

        data['speed_to_client'] = data['dist_to_client_diff'] / data['tbe']

        data['prev_lat'] = data.groupby('_id_oid')['lat'].shift(1)
        data['prev_lon'] = data.groupby('_id_oid')['lon'].shift(1)

        data['distance_to_prev_event'] = data.apply(
            lambda row: DataProcessor.haversine(
                row['lon'], row['lat'],
                row['prev_lon'],
                row['prev_lat']) * 1000,
            axis='columns')

        data['speed_to_prev_event'] = data['distance_to_prev_event'] / data['tbe']

        data['smooth_speed_to_client'] = data.groupby('_id_oid')['speed_to_client']\
            .rolling(3,center=True).mean().droplevel(0)
        data['smooth_speed_to_prev_event'] = data.groupby('_id_oid')['speed_to_prev_event']\
            .rolling(3, center=True).mean().droplevel(0)
        data['dbw_client_log'] = data['distance_to_client'].apply(log)

        return data

    def process(self):
        # Add returning logs
        m_df = self.merged_df
        # Add next route into tail
        if self.domain_type in (1, 3):
            job_routes = m_df.groupby(['delivery_job_oid', 'route_id'])['time'].max().reset_index().sort_values(['delivery_job_oid', 'time'])
            job_routes['prev_route_id'] = job_routes.groupby('delivery_job_oid')['route_id'].shift()
            job_routes = job_routes[['route_id', 'prev_route_id']].dropna()
            job_routes = job_routes.merge(self.routes, on='route_id').drop(columns='route_id') \
                .rename(columns={'prev_route_id': 'route_id'})
            job_routes = job_routes.merge(self.orders, left_on="route_id", right_on="_id_oid", how="inner")
            m_df = pd.concat([m_df, job_routes], sort=False)
            # m_df = m_df.drop(columns='index')

        # Filter by count
        counts = m_df.groupby('route_id')['time'].count()
        filtered_ids = counts[counts >= self.minimum_location_limit].index
        m_df = m_df[m_df['route_id'].isin(filtered_ids)].copy()
        print('Route len:', len(counts), 'Filtered:', len(filtered_ids))

        return DepartFromClientDataProcessor.get_movement_info(m_df)



class ReachToMerchantDataProcessor(DataProcessor):
    unique_fibonacci_numbers = [0, 1, 2, 3, 5, 8, 13, 21]
    returning_columns = ['_id_oid', 'delivery_route_oid', 'courier_courier_oid', 'reach_date',
                         'time', 'distance_bin', 'time_passed_in_bin', 'tbe',
                         'lat', 'lon', 'time_zone']

    def __init__(self, fibonacci_base, minimum_location_limit,
                 domain_type: int, merged_df: pd.DataFrame):

        self.minimum_location_limit = minimum_location_limit
        self.merged_df = merged_df
        self.fibonacci_base = fibonacci_base
        self.domain_type = domain_type

    @staticmethod
    def convert_to_seconds(x):
        try:
            return round(x.seconds)
        except:
            return np.nan

    def haversine_apply(self, row):
        dist = self.haversine(row['lon'], row['lat'],
                              row['restaurantloc_lon'],
                              row['restaurantloc_lat'])
        return round(dist * 1000)

    def find_distance_bin(self, distance):
        assert distance >= 0, 'distance should be non-negative.'
        for i, f in enumerate(self.unique_fibonacci_numbers):
            if distance < f * self.fibonacci_base:
                return i - 1
        return len(self.unique_fibonacci_numbers) - 1

    def process(self, include_all=False):

        m_df = self.merged_df
        counts = m_df.groupby('route_id')['time'].count()
        filtered_ids = counts[counts >= self.minimum_location_limit].index
        m_df = m_df[m_df['route_id'].isin(filtered_ids)]
        if m_df.empty:
            return None
        m_df['distance'] = m_df.apply(lambda r: self.haversine_apply(r), axis=1)
        m_df['distance_bin'] = m_df.distance.apply(self.find_distance_bin)
        m_df['first_location_time'] = m_df.groupby(['route_id'])['time'].transform(np.min)
        m_df['last_location_time'] = m_df.groupby(['route_id'])['time'].transform(np.max)
        m_df['next_location_time'] = m_df.sort_values(['route_id', 'time']).groupby(['route_id'])[
            'time'].shift(-1)
        m_df['tbe'] = (m_df['next_location_time'] - m_df['time']).apply(self.convert_to_seconds).fillna(0)
        m_df['previous_distance_bin'] = \
            m_df.sort_values(['route_id', 'time']).groupby(['route_id'])['distance_bin'].shift(1)
        m_df['is_distance_bin_changed'] = m_df['distance_bin'] != m_df['previous_distance_bin']
        m_df['distance_bin_group'] = m_df.sort_values(['route_id', 'time']).groupby(['route_id'])[
            'is_distance_bin_changed'].cumsum()
        m_df['time_passed_in_bin'] = m_df.sort_values(['route_id', 'distance_bin_group', 'time']).groupby(
            ['route_id', 'distance_bin_group'])['tbe'].cumsum()

        if include_all:
            return m_df
        else:
            return m_df[self.returning_columns]