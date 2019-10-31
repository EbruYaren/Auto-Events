import src.functions as functions
from src.functions import daily_analytics
from src.utils import get_run_dates, timer
from datetime import timedelta
import src.data_access as data_access


@timer
def main():
    run_dates = get_run_dates()

    for start, end in zip(run_dates, run_dates[1:]):
        commertial_df = daily_analytics(start, end)
        print('bitti')


if __name__ == '__main__':
    main()
