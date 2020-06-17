import boto3
from botocore.exceptions import NoCredentialsError


class AwsHelper:
    COPY_TEMPLATE = """
    copy {table_name} ({column_set})
    from 's3://{file_name}' 
    iam_role '{iam_role}'
    delimiter '{delimeter}' {explicit_ids};
    """

    def __init__(self,
                 s3_access_key,
                 s3_secret_key,
                 s3_region,
                 s3_bucket_name,
                 iam_role,
                 redshift_connection):

        self.S3_ACCESS_KEY = s3_access_key
        self.S3_SECRET_KEY = s3_secret_key
        self.S3_REGION = s3_region
        self.S3_BUCKET_NAME = s3_bucket_name
        self.IAM_ROLE = iam_role
        self.REDSHIFT_CONNECTION = redshift_connection
        self.S3_CLIENT = boto3.client('s3', aws_access_key_id=s3_access_key,
                                      aws_secret_access_key=s3_secret_key)

    def _copy_formatter(self, file_name: str, table_name: str,
                        column_set='', delimeter=',', explicity=False):
        if explicity:
            explicit_ids = 'explicit_ids'
        else:
            explicit_ids = ''

        if isinstance(column_set, str):
            pass
        else:
            column_set = ', '.join(column_set)

        return self.COPY_TEMPLATE.format(table_name=table_name,
                                         column_set=column_set,
                                         file_name=file_name,
                                         delimeter=delimeter,
                                         explicit_ids=explicit_ids)

    def upload_file(self, local_file, bucket: str, upload_file: str):
        try:
            self.S3_CLIENT.upload_file(local_file, bucket, upload_file)
            print("Upload Successful")
            return True, upload_file
        except FileNotFoundError:
            print("The file was not found")
            return False, upload_file
        except NoCredentialsError:
            print("Credentials not available")
            return False, upload_file

    def copy_to_redshift(self, connection, file_name: str, table_name: str,
                         column_set='', delimeter=',', explicit_ids=True):
        query = self._copy_formatter(file_name, table_name, column_set, delimeter, explicit_ids)
        connection.execute(query)
        return table_name

    def delete_file(self, file_name: str):
        self.S3_CLIENT.delete_object(Bucket=self.S3_BUCKET_NAME, Key=file_name)

    def upsert_to_redshift(self, connection, main_table,
                           local_file, column_set='',
                           delimeter=',', explicit_ids=True):
        ok, upload_file = self.upload_file(local_file,
                                           self.S3_BUCKET_NAME,
                                           local_file)
        if ok:
            temp_table = self.copy_to_redshift(connection, upload_file, '#temp_table',
                                  column_set, delimeter, explicit_ids)


        return


