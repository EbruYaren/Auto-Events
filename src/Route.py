import pandas as pd
from bson import ObjectId
import pickle


class Route:

    def __init__(self, route_id_list: list, collection=None, is_test=False, test_pickle_file=""):

        self.__route_id_list = route_id_list
        self.__is_test = is_test
        self.__test_pickle_file = test_pickle_file
        self.__collection = collection

    @staticmethod
    def _convert_cursor_to_routes_df(cursor):
        all_datas = []
        for data in cursor:
            for i, location in enumerate(data['route']):
                temp_dict = {}
                temp_dict['route_id'] = str(data['_id'])
                temp_dict['index'] = i
                temp_dict['lon'] = location['coordinates'][0]
                temp_dict['lat'] = location['coordinates'][1]
                temp_dict['time'] = location['time']
                temp_dict['acc'] = location['acc']
                all_datas.append(temp_dict)

        return pd.DataFrame(all_datas)

    def fetch_routes_df(self):
        if self.__is_test:
            pass
            with open(self.__test_pickle_file, 'rb') as test_file:
                cursor = pickle.load(test_file)
            data = self._convert_cursor_to_routes_df(cursor)
            return data

        route_ids = self.__route_id_list
        route_ids = list(map(ObjectId, route_ids))
        match = {"_id": {"$in": route_ids}}
        cursor = self.__collection.find(match)
        data = self._convert_cursor_to_routes_df(cursor)

        return data
