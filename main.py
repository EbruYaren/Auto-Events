from src.functions import example_func_sleep
from src.utils import get_run_dates
from datetime import timedelta
from src.data_access import DataAccess


def main():
    example_func_sleep(1, y=2)

    # example_func_sleep(1, 3)

    # print(get_run_dates())

    # print(get_run_dates(timedelta(hours=8)))

    # print(get_run_dates(timedelta(hours=2)))

    # print(get_run_dates(timedelta(minutes=12)))

    #data_access = DataAccess()
    #print(data_access.get_data(1, 2))
#
    #print(data_access.get_data(1, 2))
#
    #print()


main()
