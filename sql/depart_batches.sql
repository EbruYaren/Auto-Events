drop table if exists #delivery_job_oids;
create table #delivery_job_oids as
        select distinct mo._id_oid, mo.delivery_job_oid, max(onway_date) over (partition by delivery_job_oid) as max_onway_date,
               max(predicted_depart_date) over (partition by delivery_job_oid) AS max_predicted_depart_date,
               predicted_depart_date, predicted_depart_datel, p.latitude, p.longitude,
                max(delivery_batch_index) over (partition by delivery_job_oid) as max_index,
                 max(cancel_date) over (partition by delivery_job_oid) AS max_cancel_date
        from etl_market_order.marketorders mo
        left join project_auto_events.depart_date_prediction p ON  mo._id_oid = p.order_id
        where mo.deliver_date between '{start}' and '{end}' AND mo.status in (900, 1000) AND mo.domaintype in (1, 3);



drop table if exists #data;
create temp table #data as
select m._id_oid, max_predicted_depart_date as predicted_depart_date,
       max_predicted_depart_date as predicted_depart_datel,
       djo.longitude, djo.latitude
from etl_market_order.marketorders m
 join (select *
      from #delivery_job_oids
      where max_index > 1 and max_predicted_depart_date is not null
        and predicted_depart_date <> max_predicted_depart_date) djo on m._id_oid = djo._id_oid;


UPDATE project_auto_events.depart_date_prediction
SET predicted_depart_date = precalc.predicted_depart_date,
    predicted_depart_datel = precalc.predicted_depart_datel,
    latitude = precalc.latitude,
    longitude = precalc.longitude
FROM (
    SELECT _id_oid, predicted_depart_date, predicted_depart_datel,
           latitude, longitude
    FROM #data
    ) precalc
WHERE project_auto_events.depart_date_prediction.order_id = precalc._id_oid
  and project_auto_events.depart_date_prediction.predicted_depart_date <> precalc.predicted_depart_date;



