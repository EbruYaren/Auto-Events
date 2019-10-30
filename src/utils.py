import logging
from datetime import datetime
from dateutil import tz
from datetime import timedelta
import sys
import boto3
from botocore.exceptions import NoCredentialsError
from src import config
import os


def cache(func):
    def wrapper(self, *args, **kwargs):
        __hash_name__ = func.__name__ + str(kwargs) + str(args)
        if __hash_name__ not in self.func_table.keys():
            self.func_table[__hash_name__] = func(self, *args, **kwargs)
        return self.func_table[__hash_name__]

    return wrapper


def get_local_current_time(tz_name='Europe/Istanbul'):
    time_zone = tz.gettz(tz_name)
    return datetime.now(time_zone).replace(tzinfo=None)


def timer(function):
    def wrapper(*args, **kwargs):
        start = get_local_current_time()
        print('Function {}, started at {} with parameters args: {}, kwargs: {}'
              .format(function.__name__, start, args, kwargs))
        result = function(*args, **kwargs)
        end = get_local_current_time()
        print('Function {}, ended at {} and ran for {} seconds.'
              .format(function.__name__, end, (end - start).seconds))
        return result

    return wrapper


def get_local_time(utc_time, timezone='Europe/Istanbul'):
    local_tz = tz.gettz(timezone)
    local_offset = local_tz.utcoffset(utc_time)
    diff_seconds = local_offset.total_seconds()
    local_time = utc_time + timedelta(seconds=diff_seconds)
    return local_time


def read_query(file_path: str, skip_line_count=0):
    """

    :param file_path: file path of your query to read.
    :return: string object of your query
    """
    try:
        with open(file_path, 'r') as file:
            con_str = file.readlines()
    except:
        raise Exception('{} is not found'.format(file_path))
    return ' '.join(con_str[skip_line_count:])


def get_run_dates(interval=timedelta(hours=24)):
    total_seconds = interval.total_seconds()
    now = get_local_current_time()
    a_day_in_seconds = 60 * 60 * 24
    an_hour_in_seconds = 60 * 60
    a_minute_in_seconds = 60
    dates = []
    start = None
    if len(sys.argv) >= 1:
        try:
            start = datetime.strptime(sys.argv[1], '%Y-%m-%d')
            try:
                now = datetime.strptime(sys.argv[2], '%Y-%m-%d')
            except:
                pass
        except:
            logging.warning('Arguements {} are not convertible to datetime'.format(sys.argv))

    assert a_day_in_seconds >= total_seconds, 'Maximum Time Interval is in days'

    if total_seconds % a_day_in_seconds == 0:

        end = now.replace(hour=0, minute=0, second=0, microsecond=0)

    elif total_seconds % an_hour_in_seconds == 0:
        hours = int(total_seconds / an_hour_in_seconds)
        assert 24 % hours == 0, 'a day is indivisible to {} hours'.format(hours)
        replace_hours = int((now.hour // hours) * hours)
        end = now.replace(hour=replace_hours, minute=0, second=0, microsecond=0)

    elif total_seconds % a_minute_in_seconds == 0:
        minutes = int(total_seconds / a_minute_in_seconds)
        assert 60 % minutes == 0, 'an hour is indivisible to {} minutes'.format(minutes)
        replace_minute = int((now.minute // minutes) * minutes)
        end = now.replace(minute=replace_minute, second=0, microsecond=0)

    else:
        assert False, 'Minimum Time Interval is in minutes'

    if start:
        pass
    else:
        start = end - interval

    while end >= start:
        dates.append(start)
        start += interval
    logging.info("Start date: {}, end date: {}, time interval: {} ".format(dates[0], dates[-1], interval))
    return dates


def upload_to_s3(local_file, bucket, s3_file, s3_access_key=config.S3_ACCESS_KEY, s3_secret_key=config.S3_SECRET_KEY):
    s3 = boto3.client('s3', aws_access_key_id=s3_access_key,
                      aws_secret_access_key=s3_secret_key)

    try:
        s3.upload_file(local_file, bucket, s3_file)
        print("Upload Successful")
        return True
    except FileNotFoundError:
        print("The file was not found")
        return False
    except NoCredentialsError:
        print("Credentials not available")
        return False


def upsert_redshift(df, target, unique_id, engine, s3_bucket_name, s3_region,
                    s3_access_key=config.S3_ACCESS_KEY, s3_secret_key=config.S3_SECRET_KEY, ):
    """

    :param df:
    :param target:
    :param unique_id:
    :param engine:
    :param s3_access_key:
    :param s3_secret_key:
    :return:
    """

    s3_client = boto3.client('s3', aws_access_key_id=s3_access_key,
                             aws_secret_access_key=s3_secret_key,
                             region_name=s3_region)

    # write csv file to cache folder
    file_name = 'client-dates-%s.csv' % datetime.now().strftime("%Y%m%d")
    df.to_csv(file_name, index=False)
    print('DONE: write csv file to cache folder')
    # upload csv file to s3
    s3_client.upload_file(file_name, s3_bucket_name, file_name)
    print('DONE: upload csv file to s3')
    query_temp = read_query("./sql/upsert_redshift.sql")

    query = query_temp.format(target=target,
                              file_name=file_name,
                              s3_bucket_name=s3_bucket_name,
                              s3_access_key=s3_access_key,
                              s3_secret_key=s3_secret_key,
                              s3_region=s3_region,
                              unique_id=unique_id)

    with engine.begin() as connection:
        connection.execute(query, config.redshift_db)

    if os.path.exists(file_name):
        os.remove(file_name)
    s3_client.delete_object(Bucket=config.S3_BUCKET_NAME, Key=file_name)
