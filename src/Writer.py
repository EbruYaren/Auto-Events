import boto3
import pandas as pd
import datetime

from src import WRITE_ENGINE, REDSHIFT_S3_REGION, REDSHIFT_S3_BUCKET, REDSHIFT_IAM_ROLE


class Writer:

    def __init__(self, predictions: pd.DataFrame, engine, table_name, schema_name, table_cols, run_start: str):
        self.__predictions = predictions
        self.__engine = engine
        self.__table_name = table_name
        self.__schema_name = schema_name
        self.__table_columns = table_cols
        self.__run_start = run_start

        ts = datetime.datetime.now()

        self.__filename = f'{self.__run_start}-{self.__schema_name}-{self.__table_name}-{ts}.csv'

        self.__file_prefix = f'{self.__run_start}-{self.__schema_name}-{self.__table_name}'

    def __prepare_columns(self):
        if 'domain_type' in self.__predictions.columns:
            self.__predictions = self.__predictions[['_id_oid', 'domain_type', 'time', 'time_l', 'lat', 'lon']]
        else:
            self.__predictions = self.__predictions[['_id_oid', 'time', 'time_l', 'lat', 'lon']]
        self.__predictions.columns = self.__table_columns

    def __to_s3(self):
        filepath = '/tmp/' + self.__filename
        s3_file_name = 'auto-events/' + self.__filename
        print('File Name: ', s3_file_name)
        s3_client = boto3.client('s3', region_name=REDSHIFT_S3_REGION)
        s3_client.upload_file(filepath, Bucket=REDSHIFT_S3_BUCKET, Key=s3_file_name)

    def copy_to_redshift(self):
        s3_file_path = 's3://' + REDSHIFT_S3_BUCKET + '/auto-events/' + self.__file_prefix
        with WRITE_ENGINE.begin() as connection:
            try:
                connection.execute(f"""
                COPY {self.__schema_name}.{self.__table_name} ({",".join(self.__table_columns)})
                FROM '{s3_file_path}'
                iam_role '{REDSHIFT_IAM_ROLE}' delimiter '|' ignoreheader 1;
                """)


            except:
                df = connection.execute('select * from stl_load_errors order by starttime desc;')
                print(df[['starttime', 'err_reason', 'raw_line']].head())
                connection.rollback()


            finally:
                # Close the session
                connection.dispose()

    def write(self):
        self.__prepare_columns()
        self.__predictions.to_csv('/tmp/' + self.__filename, sep='|', index=False)
        self.__to_s3()
