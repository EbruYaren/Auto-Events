UPDATE project_auto_events.depart_date_prediction
SET predicted_depart_date = '{predicted_depart_date}',
    predicted_depart_datel = '{predicted_depart_datel}',
    latitude = {latitude},
    longitude = {longitude}
WHERE project_auto_events.depart_date_prediction.order_id = '{order_id}' ;