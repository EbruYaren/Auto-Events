DROP TABLE IF EXISTS {table_name};


SELECT *,
       CASE
           WHEN isRecent = 1 THEN
                       rClass::varchar(255) || fClass::varchar(255) || tClass::varchar(255) || mClass::varchar(255)
           ELSE '0000'
           END AS RFTM,
        CASE
           WHEN isRecent = 1 THEN
                       rClass::varchar(255) || ',' || fClass::varchar(255) || ',' || tClass::varchar(255) || ',' || mClass::varchar(255)
                   || ',' || gClass::varchar(255) || ',' || anm5Class::varchar(255) || ',' || deviceModel
           ELSE '0000'
           END AS unifiedclass,
        '{check_date}'::date AS LastIncludedOrderDate
        INTO {table_name}

FROM (
         SELECT *,
                NTILE(4) over (partition by isRecent ORDER BY rValue ASC)     rClass,
                NTILE(4) over (partition by isRecent ORDER BY fValue DESC )   fClass,
                NTILE(4) over (partition by isRecent ORDER BY tValue DESC)    tClass,
                NTILE(4) over (partition by isRecent ORDER BY mValue DESC)    mClass,
                NTILE(4) over (partition by isRecent ORDER BY gValue DESC)    gClass,
                NTILE(4) over (partition by isRecent ORDER BY anm5Value ASC) anm5Class,
                NTILE(4) over (partition by isRecent ORDER BY hValue ASC)     hClass


         FROM (
                  SELECT iq1.client,
                         iq1.rValue,
                         iq1.tValue,
                         CASE WHEN rValue <= {max_churn_recency} THEN iq1.Last{max_churn_recency}Monetary / iq1.Last{max_churn_recency}Frequance ELSE 0 END as mValue,
                         CASE WHEN rValue <= {max_churn_recency} THEN iq1.Last{max_churn_recency}GrossMargin / iq1.Last{max_churn_recency}Frequance ELSE 0 END as gValue,
                         CASE WHEN rValue <= {max_churn_recency} THEN iq1.fValue ELSE 0 END                               as fValue,
                         iq1.anm5Value,
                         iq1.hValue,
                         iq1.deviceType,
                         iq1.deviceBrand,
                         iq1.deviceModel,
                         iq1.bestStore,
                         CASE WHEN iq1.rValue <= {max_churn_recency} THEN 1 ELSE 0 END                                    AS isRecent
                  FROM (
                           SELECT o.client,
                                  COUNT(o.id)                                    as tValue,

                                  datediff(day, MAX(o.checkout_date), '{check_date}'::date) as rValue,

                                  SUM(
                                          CASE
                                              WHEN datediff(day, o.checkout_date, '{check_date}'::date) <= {max_churn_recency}
                                                  THEN o.net_revenue_tax_excluded
                                              ELSE 0
                                              END)                               AS Last{max_churn_recency}Monetary,
                                  SUM(
                                          CASE
                                              WHEN datediff(day, o.checkout_date, '{check_date}'::date) <= {max_churn_recency}
                                                  THEN o.gross_margin_tax_excluded
                                              ELSE 0
                                              END)                               AS Last{max_churn_recency}GrossMargin,


                                  SUM(
                                          CASE
                                              WHEN datediff(day, o.checkout_date, '{check_date}'::date) <= {max_churn_recency} THEN 1
                                              ELSE 0
                                              END)                               AS Last{max_churn_recency}Frequance,
                                  SUM(
                                          CASE
                                              WHEN datediff(day, o.checkout_date, '{check_date}'::date) <= {max_f_recency} THEN 1
                                              ELSE 0
                                              END)                               AS fValue,
                                  MAX(CASE
                                          WHEN m = 1 THEN o.deviceinfo_device_type
                                          ELSE NULL
                                      END)                                       AS deviceType,

                                  MAX(CASE
                                          WHEN m = 1 THEN o.deviceinfo_manufacturer
                                          ELSE NULL
                                      END)                                       AS deviceBrand,
                                  MAX(CASE
                                          WHEN m = 1 THEN o.deviceinfo_model
                                          ELSE NULL
                                      END)                                       AS deviceModel,

                                   MAX(CASE
                                          WHEN m2 = 1 THEN o.store
                                          ELSE NULL
                                      END)                                                AS bestStore,



                                  SUM(
                                          CASE
                                              WHEN t <={first_n} THEN O.a_andm_tax_excluded
                                              ELSE 0
                                              END)                               AS anm5Value,
                                  AVG(
                                          CASE
                                              WHEN datediff(day, o.checkout_date, '{check_date}'::date) <= {max_churn_recency} THEN h
                                              ELSE NULL
                                              END)                               AS hValue


                           FROM (
                                    SELECT *, row_number() over ( partition by client order by z desc) m,
                                    row_number() over ( partition by client order by st desc) m2

                                    FROM (
                                             SELECT *,
                                                    SUM(CASE WHEN y <= {last_n} THEN net_revenue_tax_excluded ELSE 0 END)
                                                    over (partition by client, deviceinfo_device_type, deviceinfo_manufacturer, deviceinfo_model ) as z,
                                                    SUM(CASE WHEN y <= 5 THEN net_revenue_tax_excluded ELSE 0 END)
                                                    over (partition by client, store ) as st
                                             FROM (

------------------------------------------------------------------------------------------------------------------------
                                                      SELECT id,
                                                             client,
                                                             checkout_date,
                                                             f.net_revenue_tax_excluded,
                                                             deviceinfo_device_type,
                                                             deviceinfo_manufacturer,
                                                             deviceinfo_model,
                                                             a_andm_tax_excluded,
                                                             store,
                                                             f.gross_margin_tax_excluded,
                                                             DATEDIFF(second, courier_verify_date, reach_date)              as h,
                                                             ROW_NUMBER()
                                                             over (partition by client order by checkout_date DESC)         as y,
                                                             row_number() over (partition by client order by checkout_date) as t
                                                      FROM orders
                                                               INNER JOIN orders_financials f on orders.id = f.order_id
                                                      WHERE status in {statusCodes}
                                                      AND checkout_date::date <= '{check_date}'::date
                                                  ) iq0) iq1
------------------------------------------------------------------------------------------------------------------------
                                ) o


                           group by O.client) iq1
              ) rfmValue) rfm1;

