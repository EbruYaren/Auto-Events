import time
from utils import cache, timer


class DataAccess:
    def __init__(self):
        self.func_dict = {}

    @cache
    def get_data(self, a, b):
        print("Inputs are", a, b)
        time.sleep(3)
        return "Courier dict is ready!"

    @timer
    def get_another_data(self, x):
        time.sleep(x)
