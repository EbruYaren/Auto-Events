import time
from src.utils import cache, timer


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



def remove_duplicates(connection, table_name, unique_id, unique_fields, schema_name = 'public', qg=False):
    query_temp = """
    DELETE
FROM {table_name}
WHERE {unique_id} IN (
    SELECT {unique_id}
    FROM (
             SELECT {unique_id}, 
                    row_number()
                    over (partition by {unique_fields} order by {unique_id} desc) order_
             FROM {table_name}) iq1
    WHERE order_ != 1
)

    """
    table = '.'.join([schema_name, table_name])
    unique_fields = ','.join(unique_fields)
    query = query_temp.format(table_name=table, unique_id=unique_id, unique_fields=unique_fields)
    if qg:
        return query
    connection.execute(query)


def create_table(connection, query):
    connection.execute(query)


def drop_table(connection, table_name, schema_name='public'):
    drop_status = True
    query_temp = "DROP TABLE IF EXISTS {table};"
    table = '.'.join([schema_name, table_name])
    query = query_temp.format(table=table)
    try:
        connection.execute(query)
    except Exception as e:
        print(e)
        drop_status = False
    return drop_status


def grant_access(connection, table_name, schema_name='public'):
    grant_status = True
    query_temp = 'GRANT SELECT ON TABLE {table} TO PUBLIC;'
    table = '.'.join([schema_name, table_name])
    query = query_temp.format(table=table)
    try:
        connection.execute(query)
    except Exception as e:
        print(e)
        grant_status = False
    return grant_status

if __name__ == '__main__':
    print(remove_duplicates('', 'reah_date_prediction', 'prediction_id', ['order_id'], qg=True))