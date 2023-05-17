drop table if exists #delivery_job_oids;
create table #delivery_job_oids as
        select mo.delivery_job_oid,
               count(distinct p.predicted_depart_date),
               max(p.predicted_depart_date) as max_predicted_depart_date,
               max(p.predicted_depart_datel) as max_predicted_depart_datel
        from project_auto_events.depart_date_prediction p
        inner join etl_market_order.marketorders mo ON p.order_id = mo._id_oid
        where mo.deliver_date between '{start}' and '{end}' AND mo.status in (900, 1000) AND mo.domaintype in (1, 3)
        group by mo.delivery_job_oid
        having count(distinct p.predicted_depart_date) > 1;

drop table if exists #fill_pred_data;
create temp table #fill_pred_data as
select  mo._id_oid, mo.delivery_job_oid,
       max(dj.max_predicted_depart_date) over (partition by mo.delivery_job_oid) as predicted_depart_date,
       max(dj.max_predicted_depart_datel) over (partition by mo.delivery_job_oid) as predicted_depart_datel,
       p.longitude, p.latitude
from etl_market_order.marketorders mo
join project_auto_events.depart_date_prediction p on mo._id_oid = p.order_id
left join #delivery_job_oids dj on mo.delivery_job_oid = dj.delivery_job_oid and p.predicted_depart_date = dj.max_predicted_depart_date
where mo.deliver_date between '{start}' and '{end}';


UPDATE project_auto_events.depart_date_prediction
SET predicted_depart_date = precalc.predicted_depart_date,
    predicted_depart_datel = precalc.predicted_depart_datel,
    latitude = precalc.latitude,
    longitude = precalc.longitude
FROM (
    SELECT _id_oid, delivery_job_oid, predicted_depart_date, predicted_depart_datel,
           latitude, longitude
    FROM #fill_pred_data
    ) precalc
WHERE project_auto_events.depart_date_prediction.order_id = precalc._id_oid
  and project_auto_events.depart_date_prediction.predicted_depart_date <> precalc.predicted_depart_date;



