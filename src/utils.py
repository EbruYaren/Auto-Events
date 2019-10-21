import logging
from datetime import datetime


def cache(func):
    def wrapper(self, *args, **kwargs):
        __hash_name__ = func.__name__ + str(kwargs) + str(args)
        if __hash_name__ not in self.func_table.keys():
            self.func_table[__hash_name__] = func(self, *args, **kwargs)
        return self.func_table[__hash_name__]

    return wrapper


def timer(function):
    def wrapper(*args, **kwargs):
        start = datetime.now()
        logging.info('{}, started at {}'.format(function.__name__, start))
        result = function(*args, **kwargs)
        end = datetime.now()
        logging.info('{}, ended at {}. Last for {} seconds'.format(function.__name__, start, end - start))
        return result

    return wrapper
