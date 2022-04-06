import numpy as np
import pandas as pd

from src import REDSHIFT_ETL


class FillUnpredictedDepartBatches:

    def __init__(self, start_date: str, end_date: str):

        self.__start_date = start_date
        self.__end_date = end_date


    def fill(self):


        query = """create table #job_oids as
        select mo.delivery_job_oid, listagg(mo.delivery_batch_index, ', ') as batches,
           COUNT(mo.delivery_batch_index) as batch_count,
           COUNT(p.predicted_depart_date) as pred_count
        from project_auto_events.depart_date_prediction p
        join etl_market_order.marketorders mo ON p.order_id = mo._id_oid
        where p.predictedat between \'{}\' and \'{}\' AND mo.status in (900, 1000) AND mo.domaintype in (1, 3)
        group by mo.delivery_job_oid
        having batch_count > 1 AND pred_count >= 1 AND pred_count < batch_count;
        """.format(self.__start_date, self.__end_date)

        # REDSHIFT_ETL.cursor().execute(query)
        REDSHIFT_ETL.execute(query)

        query = """
            select mo._id_oid, mo.delivery_job_oid, mo.delivery_batch_index, p.prediction_id,  p.order_id, p.predicted_depart_date, p.predicted_depart_datel,
           p.latitude, p.longitude,  p.predictedat
            FROM etl_market_order.marketorders mo
            LEFT JOIN #job_oids j ON mo.delivery_job_oid = j.delivery_job_oid
            LEFT JOIN project_auto_events.depart_date_prediction p ON mo._id_oid = p.order_id
            WHERE j.delivery_job_oid IS NOT NULL
        """


        df = pd.read_sql(query, REDSHIFT_ETL)
        df.columns = ['_id_oid', 'delivery_job_oid', 'delivery_batch_index', 'prediction_id', 'order_id',
                      'predicted_depart_date', 'predicted_depart_datel', 'latitude', 'longitude', 'predictedat']
        rows = []

        df['order_id'] = df._id_oid
        df = df[(df.delivery_job_oid.notna()) & (df.delivery_job_oid != 'NaN')]

        # df = df[df.delivery_job_oid == '623f615464bad9e0ea404f22']

        for delivery_job_oid, row in df.groupby('delivery_job_oid'):
            first_row = row.loc[row['delivery_batch_index'] == 1]
            if (first_row.predicted_depart_date.notna().bool()) & (np.isnat(first_row.predicted_depart_date).bool() == False):
                batches = row[(row['delivery_batch_index'] > 1) & (row.predicted_depart_date.isna())]
                batches = batches.reset_index(drop=True)
                if batches.size > 0:
                    # For each unpredicted batch, first batch data is being appended to rows.
                    for i, r in batches.iterrows():
                        order_id = r.order_id
                        predicted_depart_date = first_row['predicted_depart_date'].values[0]
                        predicted_depart_datel = first_row['predicted_depart_datel'].values[0]
                        latitude = first_row['latitude'].values[0]
                        longitude = first_row['longitude'].values[0]
                        query = """
                                UPDATE project_auto_events.depart_date_prediction
                                SET predicted_depart_date = \'{}\', predicted_depart_datel = \'{}\', latitude = {}, longitude = {}
                                WHERE order_id = \'{}\' ;
                        """.format(predicted_depart_date, predicted_depart_datel, latitude, longitude, order_id)

                        # pd.read_sql(query, REDSHIFT_ETL)
                        REDSHIFT_ETL.execute(query)

        predictions = pd.DataFrame(rows)



        return predictions
