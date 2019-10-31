select unsucorders.*,
       checkout__tries_code,
       count(marketorders_checkout__tries._id_oid) as checkout_trycount_given_error,
       min(dateadd(hour, 3, checkout__tries_date)) as checkout__tries_date
    from marketorders_checkout__tries
         inner join
     (select _id_oid as order_id, client_client_oid as client_id, checkout_trycount as checkout_trycount_total
      from marketorders
      where status < 900
        and createdat >= dateadd(hour, -3, '{start_date}')
        and createdat < dateadd(day, 1, dateadd(hour, -3, '{start_date}'))
        and checkout_trycount > 0
     ) as unsucorders
     on unsucorders.order_id = _id_oid and checkout__tries_code in ('{given_error}')
    group by 1, 2, 3,4;