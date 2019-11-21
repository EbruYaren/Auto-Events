import time
from src.utils import timer, cache

#this version prints args
@timer()
def example_func_sleep(x, y):
    time.sleep(x)
    time.sleep(y)
    return x+y


#this version does note prints args
@timer(False)
def example_func_sleep(x, y):
    time.sleep(x)
    time.sleep(y)
    return x+y
