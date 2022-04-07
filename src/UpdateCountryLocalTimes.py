

from src import REDSHIFT_ETL


class CountryLocalTimes:

    def __init__(self):
        print("!")



    def fill(self):


        query = """
                    create table #predicted_reach as
                    select r.order_id, r.predicted_reach_date, w.country_name, z.time_zone,       
                    convert_timezone('UTC', z.time_zone, r.predicted_reach_date) as predicted_reach_datel
                    from project_auto_events.reach_date_prediction r
                    inner join etl_market_order.marketorders mo ON r.order_id = mo._id_oid
                    left join market_analytics.warehouse_locations w ON mo.warehouse_warehouse_oid = w.warehouse
                    LEFT JOIN market_analytics.country_time_zones AS z ON mo.country_oid = z.country_id
                    where r.predictedat between '2022-01-01' AND '2022-04-07' 
                    AND r.predicted_reach_date is not null
                    AND w.country_name <> 'TURKEY';"""

        REDSHIFT_ETL.execute(query)

        query = """UPDATE project_auto_events.reach_date_prediction
                SET predicted_reach_datel = precalc.predicted_reach_datelFROM (
                    SELECT order_id, predicted_reach_date, country_name, time_zone, predicted_reach_datel    
                    FROM #predicted_reach
                    ) precalc
                WHERE project_auto_events.reach_date_prediction.order_id = precalc.order_id;"""

        REDSHIFT_ETL.execute(query)

        query = """create table #predicted_depart as
                    select r.order_id, r.predicted_depart_date, w.country_name, z.time_zone,       
                    convert_timezone('UTC', z.time_zone, r.predicted_depart_date) as predicted_depart_datel
                    from project_auto_events.depart_date_prediction r
                    inner join etl_market_order.marketorders mo ON r.order_id = mo._id_oid
                    left join market_analytics.warehouse_locations w ON mo.warehouse_warehouse_oid = w.warehouse
                    LEFT JOIN market_analytics.country_time_zones AS z ON mo.country_oid = z.country_id
                    where r.predictedat between '2022-01-01' AND '2022-04-07' 
                    AND r.predicted_depart_date is not null
                    AND w.country_name <> 'TURKEY';"""

        REDSHIFT_ETL.execute(query)

        query = """UPDATE project_auto_events.depart_date_prediction
                    SET predicted_depart_datel = precalc.predicted_depart_datel
                    FROM (
                        SELECT order_id, predicted_depart_date, country_name, time_zone, predicted_depart_datel
                        FROM #predicted_depart
                        ) precalc
                    WHERE project_auto_events.depart_date_prediction.order_id = precalc.order_id;"""

        REDSHIFT_ETL.execute(query)

        query = """create table #predicted_depart_from_client as
                    select r.order_id, r.predicted_depart_from_client_date, w.country_name, z.time_zone,
                    convert_timezone('UTC', z.time_zone, r.predicted_depart_from_client_datel) as predicted_depart_from_client_datel
                    from project_auto_events.depart_from_client_date_prediction r
                    inner join etl_market_order.marketorders mo ON r.order_id = mo._id_oid
                    left join market_analytics.warehouse_locations w ON mo.warehouse_warehouse_oid = w.warehouse
                    LEFT JOIN market_analytics.country_time_zones AS z ON mo.country_oid = z.country_id
                    where r.predictedat between '2022-01-01' AND '2022-04-07'
                    AND r.predicted_depart_from_client_date is not null
                    AND w.country_name <> 'TURKEY';"""

        REDSHIFT_ETL.execute(query)

        query = """UPDATE project_auto_events.depart_from_client_date_prediction
                SET predicted_depart_from_client_datel = precalc.predicted_depart_from_client_datel
                FROM (
                    SELECT order_id, predicted_depart_from_client_date, country_name, time_zone, predicted_depart_from_client_datel
                    FROM #predicted_depart_from_client
                    ) precalc
                WHERE project_auto_events.depart_from_client_date_prediction.order_id = precalc.order_id;
        """


        # REDSHIFT_ETL.cursor().execute(query)
        REDSHIFT_ETL.execute(query)
