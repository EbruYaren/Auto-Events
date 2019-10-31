import time
from src.utils import timer, cache

@timer
def example_func_sleep(x, y):
    time.sleep(x)
    time.sleep(y)
    return x+y

