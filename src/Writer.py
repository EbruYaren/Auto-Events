import boto3
import pandas as pd
import datetime

from src import WRITE_ENGINE, REDSHIFT_S3_REGION, REDSHIFT_S3_BUCKET, REDSHIFT_IAM_ROLE


class Writer:

    def __init__(self, predictions: pd.DataFrame, engine, table_name, schema_name, table_cols):
        self.__predictions = predictions
        self.__engine = engine
        self.__table_name = table_name
        self.__schema_name = schema_name
        self.__table_columns = table_cols
        ts = datetime.datetime.now().timestamp()
        self.__filename = f'{self.__schema_name}-{self.__table_name}-{ts}.csv'

    def __prepare_columns(self):
        self.__predictions = self.__predictions[['_id_oid', 'time', 'time_l', 'lat', 'lon']]
        self.__predictions.columns = self.__table_columns

    def __to_s3(self):
        filepath = '/tmp/' + self.__filename
        s3_file_name = 'auto-events/' + self.__filename
        s3_client = boto3.client('s3', region_name=REDSHIFT_S3_REGION)
        s3_client.upload_file(filepath, Bucket=REDSHIFT_S3_BUCKET, Key=s3_file_name)

    def __copy_to_redshift(self):
        s3_file_path = 's3://' + REDSHIFT_S3_BUCKET + '/auto-events/' + self.__filename
        with WRITE_ENGINE.begin() as connection:
            connection.execute(f"""
            CREATE TABLE "{self.__filename}"
            (
                _id_oid varchar(256),
                time    timestamp,
                time_l  timestamp,
                lat     double precision,
                lon     double precision
            );
            COPY "{self.__filename}" FROM '{s3_file_path}' iam_role '{REDSHIFT_IAM_ROLE}' delimiter '|' ignoreheader 1;
            INSERT INTO {self.__schema_name}.{self.__table_name}
                ({",".join(self.__table_columns)},predictedat)
                (SELECT *, getdate() FROM "{self.__filename}");
            """)

    def write(self):
        self.__prepare_columns()
        self.__predictions.to_csv('/tmp/' + self.__filename, sep='|', index=False)
        self.__to_s3()
        self.__copy_to_redshift()
