import pandas as pd
import sqlalchemy


class Order:
    QUERY_TEMPLATE = """SELECT delivery_route_oid, _id_oid, courier_courier_oid,
           checkoutdatel,
           deliver_date ,
           reach_date,
           onway_date,
           -- client_location__coordinates_lon, client_location__coordinates_lat,
           deliver_location__coordinates_lon, deliver_location__coordinates_lat,
           reach_location__coordinates_lon, reach_location__coordinates_lat,
           delivery_address_location__coordinates_lon, delivery_address_location__coordinates_lat,
           warehouse_location__coordinates_lon, warehouse_location__coordinates_lat,
            first_value(handover_date) over (partition by delivery_job_oid order by delivery_batch_index desc rows unbounded preceding ) handover_date,
           delivery_job_oid,
           delivery_batch_index,
           z.time_zone
        FROM etl_market_order.marketorders o
        LEFT JOIN project_auto_events.{prediction_table} rdp ON rdp.order_id = o._id_oid
        LEFT JOIN market_analytics.country_time_zones AS z ON o.country_oid = z.country_id
        WHERE status in (900, 1000)
        {null_filter}
        AND deliver_date BETWEEN  '{start_date}' AND  '{end_date}' 
        AND domaintype in (1,3)
        {courier_filter}
        ORDER BY courier_courier_oid, deliver_date
    """

    # getting food orders
    QUERY_TEMPLATE_FOOD = """select f.route_oid as delivery_route_oid, f._id_oid, f.courier_oid as courier_courier_oid, 
                               f.checkoutdatel, f.deliverdate as deliver_date, f.reachdate as reach_date, f.handoverdate, 
                               f.deliveryaddress_location__coordinates_lon, f.deliveryaddress_location__coordinates_lat, f.restaurantloc_lon, f.restaurantloc_lat,
                               l.reached_to_restaurant_lat, l.reached_to_restaurant_lon, l.reached_to_restaurant_createdatl, l.reached_to_client_lat,
                               l.reached_to_client_lon, l.reached_to_client_createdatl, 2 as domaintype,
                               'Europe/Istanbul' as time_zone,
                               cd.domaintypes,
                               wl.lat as warehouse_location__coordinates_lat,
                               wl.lon as warehouse_location__coordinates_lon
                        from etl_food_order.foodorders f
                        LEFT JOIN (select  data_foodorder as artisan_order_id,
                                           MAX(case when data_method = 'courierReachedToRestaurant' then location__coordinates_lat end) as reached_to_restaurant_lat,
                                           MAX(case when data_method = 'courierReachedToRestaurant' then createdatl end) as reached_to_restaurant_createdatl,
                                           MAX(case when data_method = 'courierReachedToRestaurant' then location__coordinates_lon end) as reached_to_restaurant_lon,
                                           MAX(case when data_method = 'courierReachedToClient' then location__coordinates_lat end) as reached_to_client_lat,
                                           MAX(case when data_method = 'courierReachedToClient' then location__coordinates_lon end) as reached_to_client_lon,
                                           MAX(case when data_method = 'courierReachedToClient' then createdatl end) as reached_to_client_createdatl
                                    from etl_getir_logs.courierstatuslogs
                                    where data_foodorder is not null and data_foodorder <> 'null'
                                    group by data_foodorder) l ON f._id_oid = l.artisan_order_id
                        LEFT JOIN project_auto_events.reach_to_restaurant_date_prediction rdp ON rdp.order_id = f._id_oid
                        LEFT JOIN etl_getir.couriers__domaintypes cd ON f.courier_oid = cd._p_id_oid
                        LEFT JOIN etl_getir.couriers c ON f.courier_oid = c._id_oid
                        LEFT JOIN market_analytics.warehouse_locations wl ON c.warehouse_oid = wl.warehouse
                                WHERE f.status in (900, 1000)
                                AND f.deliverytype = 1 
                                AND (rdp.order_id is null OR rdp.predicted_reach_date is null)
                                AND f.deliverdate BETWEEN  '{start_date}' AND  '{end_date}'
                                 {courier_filter}
                """
    # getting artisan orders
    QUERY_TEMPLATE_ARTISAN = """select f.route_oid as delivery_route_oid, f._id_oid, f.courier_oid as courier_courier_oid,
                                    f.checkoutdatel, f.deliverdate as deliver_date, f.reachdate as reach_date, f.handoverdate,
                                    f.deliveryaddress_location__coordinates_lon, f.deliveryaddress_location__coordinates_lat,
                                    f.restaurantloc_lon, f.restaurantloc_lat,
                                    l.reached_to_restaurant_lat, l.reached_to_restaurant_lon, l.reached_to_restaurant_createdatl, l.reached_to_client_lat,
                                    l.reached_to_client_lon, l.reached_to_client_createdatl, 6 as domaintype,
                                    'Europe/Istanbul' as time_zone,
                                    cd.domaintypes,
                                   wl.lat as warehouse_location__coordinates_lat,
                                   wl.lon as warehouse_location__coordinates_lon
                                    from etl_artisan_order.foodorders f
                                    LEFT JOIN (select  data_foodorder as artisan_order_id,
                                                       MAX(case when data_method = 'courierReachedToRestaurant' then location__coordinates_lat end) as reached_to_restaurant_lat,
                                                       MAX(case when data_method = 'courierReachedToRestaurant' then createdatl end) as reached_to_restaurant_createdatl,
                                                       MAX(case when data_method = 'courierReachedToRestaurant' then location__coordinates_lon end) as reached_to_restaurant_lon,
                                                       MAX(case when data_method = 'courierReachedToClient' then location__coordinates_lat end) as reached_to_client_lat,
                                                       MAX(case when data_method = 'courierReachedToClient' then location__coordinates_lon end) as reached_to_client_lon,
                                                       MAX(case when data_method = 'courierReachedToClient' then createdatl end) as reached_to_client_createdatl
                                                from etl_getir_logs.courierstatuslogs
                                                where data_foodorder is not null and data_foodorder <> 'null'
                                                group by data_foodorder) l ON f._id_oid = l.artisan_order_id
                                    LEFT JOIN project_auto_events.reach_to_shop_date_prediction rdp ON rdp.order_id = f._id_oid
                                    LEFT JOIN etl_getir.couriers__domaintypes cd ON f.courier_oid = cd._p_id_oid
                                    LEFT JOIN etl_getir.couriers c ON f.courier_oid = c._id_oid
                                    LEFT JOIN market_analytics.warehouse_locations wl ON c.warehouse_oid = wl.warehouse
                                    WHERE f.status in (900, 1000)
                                    AND (rdp.order_id is null OR rdp.predicted_reach_date is null)
                                    AND f.deliverytype = 1
                                    AND f.deliverdate BETWEEN  '{start_date}' AND  '{end_date}'
                                             {courier_filter}
                """

    def __init__(self, start_date: str, end_date: str, etl_engine: sqlalchemy.engine.base.Engine, courier_ids=[],
                 chunk_size=1000, domains=None, domain_type=int):

        self.__start_date = start_date
        self.__end_date = end_date
        self.__etl_engine = etl_engine
        self.__courier_ids = courier_ids
        self.__chunk_size = chunk_size
        self.__domains = domains
        self.__domain_type = domain_type

    def _create_in_clause(self, id_s: list):
        if len(id_s) == 0:
            return ''
        ids = ["'" + str(id_) + "'" for id_ in id_s]

        return "(" + ','.join(ids) + ")"

    def _query_formatter(self):
        if self.__domains == ['depart_from_client']:
            # run for if no prediction
            prediction_table = 'depart_from_client_date_prediction'
            null_filter = "AND rdp.predicted_depart_from_client_date isnull"
        else:
            # run for first time rows
            prediction_table = 'depart_from_client_date_prediction'
            null_filter = "AND (rdp.order_id isnull or rdp.predicted_depart_from_client_date is null)"

        if len(self.__courier_ids) == 0:
            courier_filter = ''
        else:
            in_clause = self._create_in_clause(self.__courier_ids)
            courier_filter = 'AND courier_courier_oid IN {}'.format(in_clause)

        if self.__domain_type in (1, 3):
            return self.QUERY_TEMPLATE.format(start_date=self.__start_date,
                                              end_date=self.__end_date,
                                              courier_filter=courier_filter,
                                              prediction_table=prediction_table,
                                              null_filter=null_filter)
        elif self.__domain_type == 2:
            return self.QUERY_TEMPLATE_FOOD.format(start_date=self.__start_date,
                                                   end_date=self.__end_date,
                                                   courier_filter=courier_filter,
                                                   #                                       prediction_table=prediction_table,
                                                   null_filter=null_filter)
        elif self.__domain_type == 6:
            return self.QUERY_TEMPLATE_ARTISAN.format(start_date=self.__start_date,
                                                      end_date=self.__end_date,
                                                      courier_filter=courier_filter,
                                                      #                                          prediction_table=prediction_table,
                                                      null_filter=null_filter)

    def fetch_orders_df(self):
        query = self._query_formatter()
        df_iterable = pd.read_sql(query, self.__etl_engine, chunksize=self.__chunk_size)
        if self.__chunk_size == None:
            return [df_iterable]
        return df_iterable
