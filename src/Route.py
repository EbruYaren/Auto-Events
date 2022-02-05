import datetime
import pandas as pd
from bson import ObjectId
import polyline
import pickle


class Route:

    def __init__(self, route_id_list: list, collection=None,
                 is_test=False, engine_etl=None):

        self.__route_id_list = route_id_list
        self.__is_test = is_test
        self.__collection = collection
        self.__engine_etl = engine_etl

    @staticmethod
    def _convert_cursor_to_routes_df(cursor):
        all_datas = []

        for data in cursor:
            if 'route' in data:
                for i, location in enumerate(data['route']):
                        temp_dict = {}
                        temp_dict['route_id'] = str(data['_id'])
                        temp_dict['index'] = i
                        temp_dict['lon'] = location['coordinates'][0]
                        temp_dict['lat'] = location['coordinates'][1]
                        temp_dict['time'] = location['time']
                        temp_dict['acc'] = location['acc']
                        all_datas.append(temp_dict)
            else:
                routes_data = decode(data)
                for dict in routes_data:
                    all_datas.append(dict)
        return pd.DataFrame(all_datas)

    def __get_routes_from_temp_table(self):
        query = \
            """
            create table #routes_route
            (
                route_id   varchar(26) not null,
                index      integer,
                lon        double precision,
                lat        double precision,
                time       timestamp,
                acc        double precision
            );
            copy #routes_route from 's3://getir-data-routes-etl/routes-etl/routes-route-20220116'
                iam_role 'arn:aws:iam::164762854291:role/data-cron-temp-files-role' CSV GZIP ignoreheader 1;
            SELECT *
            FROM #routes_route
            WHERE route_id in ('{routes}')
            """.format(routes="','".join(self.__route_id_list))

        return pd.read_sql(query, self.__engine_etl)

    def fetch_routes_df(self):

        if self.__is_test:
            data = self.__get_routes_from_temp_table()
        else:
            route_ids = self.__route_id_list
            route_ids = list(map(ObjectId, route_ids))
            match = {"_id": {"$in": route_ids}}
            cursor = self.__collection.find(match)
            data = self._convert_cursor_to_routes_df(cursor)

        return data


# decoding encoded route data
def decode(data):
    """
    :type data: dict
    :rtype: list of dict
    """
    route_id = str(data.get('_id'))
    start_time = data.get('startTime')
    decoded_routes = polyline.decode(data['encoded'], precision=7)
    time_list = [int(i, 36) for i in data['times'].split(";") if i != '']
    time_list = [start_time + datetime.timedelta(seconds=t / 20) for t in time_list if t is not None]

    rows = []
    # appending rows
    for t, r in zip(time_list, decoded_routes):
        rows.append({
            'route_id': route_id,
            'time': t,
            'lat': r[0],
            'lon': r[1],
        })
    # or simpler: [dict(route_id=route_id, time=t, lat=r[0], lon=r[1]) for t, r in zip(time_list, decoded_routes)]

    return rows
