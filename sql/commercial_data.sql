select p.*, promo__applied_promo as promo
from marketorders_promo__applied
         right join
     (select c.*,
             datediff(minutes, c.first_checkout_try_date_l, c.checkout_date_l) as first_checkout_to_checkout_min,
             datediff(minutes, c.checkout_date_l, c.picker_assign_date)        as checkout_to_picker_assign_min,
             datediff(minutes, c.picker_assign_date, c.picker_verify_date_l)   as picker_assign_to_verify_min,
             datediff(minutes, c.picker_verify_date_l, c.preparing_date_l)     as picker_verify_to_preparing_min,
             datediff(minutes, c.preparing_date_l, c.prepare_date_l)           as preparing_to_prepared_min,
             datediff(minutes, c.prepare_date_l, c.handover_date_l)            as prepared_to_handover_min,
             datediff(minutes, c.handover_date_l, c.onway_date_l)              as handover_to_onway_min,
             datediff(minutes, c.onway_date_l, c.reach_date_l)                 as onway_to_reach_min,
             datediff(minutes, c.reach_date_l, c.deliver_date_l)               as reach_to_deliver_min,
             datediff(minutes, c.deliver_date_l, c.return_date_l)              as deliver_to_return_min
      from (
               select v.*,
                      prepare__products_picker_oid                   as picker_id,
                      min(dateadd(hour, 3, prepare__products_date))  as preparing_date_l,
                      count(distinct prepare__products_selected_oid) as unique_item_count,
                      sum(prepare__products_count)                   as total_item_count
               from marketorders_prepare__products
                        right join
                    (select e.*, min(dateadd(hour, 3, checkout__tries_date)) as first_checkout_try_date_l
                     from marketorders_checkout__tries
                              right join
                          (select o.*,
                                  dateadd(hour, 3, verify_date) as picker_verify_date_l
                           from marketorders__verify
                                    right join
                                (select trunc(dateadd(hour, 3, checkout_date))           as date,
                                        date_part(hour, dateadd(hour, 3, checkout_date)) as hour,
                                        _id_oid                                          as order_id,
                                        client_client_oid                                as client_id,
                                        dateadd(hour, 3, checkout_date)                  as checkout_date_l,
                                        dateadd(hour, 3, picking_date)                   as picker_assign_date,
                                        dateadd(hour, 3, prepare_date)                   as prepare_date_l,
                                        dateadd(hour, 3, handover_date)                  as handover_date_l,
                                        dateadd(hour, 3, onway_date)                     as onway_date_l,
                                        dateadd(hour, 3, reach_date)                     as reach_date_l,
                                        dateadd(hour, 3, deliver_date)                   as deliver_date_l,
                                        dateadd(hour, 3, return_date)                    as return_date_l,
                                        courier_courier_oid                              as courier_id,
                                        delivery_job_oid                                 as delivery_job_id,
                                        rate_rating                                      as rate,
                                        courier_fleetvehicle_oid                         as vehicle_oid,
                                        basket_calculation_longestedge                   as longest_edge,
                                        basket_calculation_totalweight                   as total_weight,
                                        basket_calculation_totalvolume                   as total_volume,
                                        financial_basketvalue                            as basket_value,
                                        financial_chargedamount                          as charged_amount,
                                        financial_netrevenue                             as net_revenue,
                                        financial_basketvaluetaxexcluded                 as basket_value_tax_excluded,
                                        financial_discountamounttaxexcluded              as discount_amount_tax_excluded,
                                        financial_chargedamounttaxexcluded               as charged_amount_tax_excluded,
                                        financial_netrevenuetaxexcluded                  as net_revenue_tax_excluded,
                                        financial_cogstaxexcluded                        as cogs_tax_excluded,
                                        financial_cogsofdiscounttaxexcluded              as cogs_of_discount_tax_excluded,
                                        financial_aandmtaxexcluded                       as a_and_m_tax_excluded,
                                        financial_suppliersupporttaxexcluded             as supplier_support_tax_excluded,
                                        financial_thirdpartysupporttaxexcluded           as third_party_support_tax_excluded,
                                        financial_grossmargintaxexcluded                 as gross_margin_tax_excluded
                                 from marketorders
                                 where checkout_date >= dateadd(hour, -3, '{query_start}')
                                   and checkout_date < dateadd(hour, -3, '{query_end}')
                                   and status in (900, 1000)
                                ) as o on _id_oid = o.order_id
                          ) as e on _id_oid = e.order_id and checkout__tries_code in ('{given_error}')
                     group by date, hour, order_id, client_id, checkout_date_l, picker_assign_date, prepare_date_l,
                              handover_date_l, onway_date_l, reach_date_l, deliver_date_l, return_date_l, courier_id,
                              delivery_job_id, rate, vehicle_oid, longest_edge, total_weight, total_volume,
                              basket_value, charged_amount, net_revenue, basket_value_tax_excluded,
                              discount_amount_tax_excluded, charged_amount_tax_excluded, net_revenue_tax_excluded,
                              cogs_tax_excluded, cogs_of_discount_tax_excluded, a_and_m_tax_excluded,
                              supplier_support_tax_excluded, third_party_support_tax_excluded,
                              gross_margin_tax_excluded, picker_verify_date_l
                    ) as v on _id_oid = v.order_id
               group by date, hour, order_id, client_id, checkout_date_l, picker_assign_date, prepare_date_l,
                        handover_date_l, onway_date_l, reach_date_l, deliver_date_l, return_date_l, courier_id,
                        delivery_job_id, rate, vehicle_oid, longest_edge, total_weight, total_volume,
                        basket_value, charged_amount, net_revenue, basket_value_tax_excluded,
                        discount_amount_tax_excluded, charged_amount_tax_excluded, net_revenue_tax_excluded,
                        cogs_tax_excluded, cogs_of_discount_tax_excluded, a_and_m_tax_excluded,
                        supplier_support_tax_excluded, third_party_support_tax_excluded, gross_margin_tax_excluded,
                        picker_verify_date_l,
                        first_checkout_try_date_l, picker_id
           ) as c
     ) as p on p.order_id = marketorders_promo__applied._id_oid;