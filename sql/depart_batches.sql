
create table #delivery_job_oids as
        select mo.delivery_job_oid, listagg(mo.delivery_batch_index, ', ') as batches,
           COUNT(mo.delivery_batch_index) as batch_count,
           COUNT(p.predicted_depart_date) as pred_count
        from project_auto_events.depart_date_prediction p
        inner join etl_market_order.marketorders mo ON p.order_id = mo._id_oid
        where p.predictedat between '2022-04-01' and '2022-04-19' AND mo.status in (900, 1000) AND mo.domaintype in (1, 3)
        group by mo.delivery_job_oid
        having batch_count > 1 AND pred_count >= 1 AND pred_count < batch_count;

drop table if exists #pred_data;
create table #pred_data as
select mo.delivery_job_oid,
       min(p.predicted_depart_date) as min_predicted_depart_date,
       min(p.predicted_depart_datel) as min_predicted_depart_datel,
       min(p.latitude) as latitude,
       min(p.longitude) as longitude
from etl_market_order.marketorders mo
left join #delivery_job_oids j ON mo.delivery_job_oid = j.delivery_job_oid
left join project_auto_events.depart_date_prediction p ON mo._id_oid = p.order_id
where mo.deliver_date between '2022-03-31 23:00:00' and '2022-04-19' and mo.status in (900, 1000)
and mo.domaintype in (1, 3) and j.delivery_job_oid is not null and p.order_id is not null
group by mo.delivery_job_oid
having min_predicted_depart_date is not null;

drop table if exists #fill_pred_data;
create table #fill_pred_data as
select s.delivery_job_oid, s.min_predicted_depart_date, s.min_predicted_depart_datel, latitude, longitude,
       mo._id_oid, mo.delivery_batch_index
from #pred_data s
left join etl_market_order.marketorders mo ON s.delivery_job_oid = mo.delivery_job_oid
where mo._id_oid is not null;



UPDATE project_auto_events.depart_date_prediction
SET predicted_depart_date = precalc.min_predicted_depart_date,
    predicted_depart_datel = precalc.min_predicted_depart_datel,
    latitude = precalc.latitude,
    longitude = precalc.longitude
FROM (
    SELECT _id_oid, delivery_batch_index, delivery_job_oid, min_predicted_depart_date, min_predicted_depart_datel,
           latitude, longitude
    FROM #fill_pred_data
    ) precalc
WHERE project_auto_events.depart_date_prediction.order_id = precalc._id_oid
  and project_auto_events.depart_date_prediction.predicted_depart_date is null;
