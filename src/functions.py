import time
from src.utils import timer, cache
import src.data_access as data_access
import pandas as pd
import numpy as np
import src.constants as constants
import warnings

warnings.filterwarnings("ignore")


def wavg(group, avg_name, weight_name):
    d = group[avg_name]
    w = group[weight_name]
    try:
        return (d * w).sum() / w.sum()
    except ZeroDivisionError:
        return d.mean()


@timer
def daily_analytics(start_date, end_date):
    # Get Data

    # tab_click_df = data_access.get_tab_count(start_date, end_date)
    order_detail = data_access.get_order_detail(query_start=start_date, query_end=end_date, given_error=constants.unavailable_courier_error)
    # product_sales_df = data_access.get_product_sales(query_start=start_date, query_end=end_date)

    # Manipulations

    # Unique Visit Count From Tab

    # tab_click_count = tab_click_df.groupby(['date'])['client'].nunique().reset_index().rename(columns={'client': 'unique_visitor'})

    # Batch Part
    if len(order_detail['delivery_job_id'].unique()) > 1:
        is_batched_order = order_detail.groupby(['delivery_job_id'])['order_id'].count().reset_index()
        is_batched_order.columns = ['delivery_job_id', 'is_batched']
        order_detail_batched = pd.merge(order_detail, is_batched_order, on=['delivery_job_id'], how='left')
        order_detail_batched['is_batched'] = order_detail_batched['is_batched'].apply(lambda x: True if x > 1 else False)
        batched_df = order_detail_batched[order_detail_batched['is_batched'] == True].sort_values(['delivery_job_id', 'deliver_date_l'])

        batched_num_df = pd.DataFrame()
        batched_orders = pd.DataFrame()
        for i in list(batched_df['delivery_job_id'].unique()):
            selected_batch = batched_df[batched_df['delivery_job_id'] == i].reset_index(drop=True)
            selected_batch['batch_no'] = selected_batch.index + 1

            for j in list(selected_batch['batch_no']):
                main_order_df = selected_batch[selected_batch['batch_no'] == j][['order_id', 'batch_no']].reset_index(drop=True)
                batched_list = list(selected_batch[selected_batch['batch_no'] != j]['order_id'])
                batched_str = ','.join(str(e) for e in batched_list)
                main_order_df['batched_with'] = pd.Series(batched_str)

                batched_df_v2 = main_order_df[['order_id', 'batched_with']]
                batched_orders = pd.concat([batched_orders, batched_df_v2]).reset_index(drop=True)

            for k in list(selected_batch['batch_no']):
                if k == 1:  # For first batch
                    selected_batch['realized_onway_date_l_first'] = selected_batch['onway_date_l'].max()
                else:
                    selected_batch['realized_onway_date_l_other'] = selected_batch['deliver_date_l'].shift(1)

            selected_batch['realized_onway_date_l'] = selected_batch.apply(lambda row:
                                                                           row['realized_onway_date_l_first'] if row['batch_no'] == 1 else
                                                                           row['realized_onway_date_l_other']
                                                                           , axis=1)
            del selected_batch['realized_onway_date_l_first'], selected_batch['realized_onway_date_l_other']

            batched_num_df = pd.concat([batched_num_df, selected_batch])
            batched_final = pd.merge(batched_num_df, batched_orders, on=['order_id'], how='left')

        batched_final['realized_onway_to_reach_min'] = (batched_final.reach_date_l - batched_final.realized_onway_date_l) / np.timedelta64(1, 'm')
        batched_final['handover_to_realized_onway_min'] = (batched_final.realized_onway_date_l - batched_final.handover_date_l) / np.timedelta64(1, 'm')
        unbatched_df = order_detail_batched[order_detail_batched['is_batched'] == False]
        unbatched_df['batch_no'] = 0
        unbatched_df['handover_to_realized_onway_min'] = unbatched_df['handover_to_onway_min']
        unbatched_df['realized_onway_to_reach_min'] = unbatched_df['onway_to_reach_min']
        unbatched_df['realized_onway_date_l'] = unbatched_df['onway_date_l']
        order_detail_df = pd.concat([batched_final, unbatched_df], sort=False)
    else:
        order_detail_df = order_detail
        order_detail_df['batch_no'] = 0

    print(order_detail_df.info())

    # Order Durations

    if len(order_detail['delivery_job_id'].unique()) > 1:

        order_durations = order_detail_df.groupby(['date']).agg({'first_checkout_to_checkout_min': ['mean'],
                                                                 'checkout_to_picker_assign_min': ['mean'], 'picker_assign_to_verify_min': ['mean'],
                                                                 'picker_verify_to_preparing_min': ['mean'], 'preparing_to_prepared_min': ['mean'],
                                                                 'prepared_to_handover_min': ['mean'], 'handover_to_onway_min': ['mean'],
                                                                 'handover_to_realized_onway_min': ['mean'],
                                                                 'realized_onway_to_reach_min': ['mean'], 'onway_to_reach_min': ['mean'], 'reach_to_deliver_min': ['mean'], 'deliver_to_return_min': ['mean']
                                                                 }).reset_index()
    else:
        order_durations = order_detail_df.groupby(['date']).agg({'first_checkout_to_checkout_min': ['mean'],
                                                                 'checkout_to_picker_assign_min': ['mean'], 'picker_assign_to_verify_min': ['mean'],
                                                                 'picker_verify_to_preparing_min': ['mean'], 'preparing_to_prepared_min': ['mean'],
                                                                 'prepared_to_handover_min': ['mean'], 'handover_to_onway_min': ['mean'],
                                                                 'onway_to_reach_min': ['mean'], 'reach_to_deliver_min': ['mean'], 'deliver_to_return_min': ['mean']
                                                                 }).reset_index()

    order_durations.columns = order_durations.columns.droplevel(1)

    # Margin

    order_detail_df['gross_margin'] = order_detail_df['gross_margin_tax_excluded'] / order_detail_df['net_revenue_tax_excluded']
    order_detail_df['net_margin'] = (order_detail_df['gross_margin_tax_excluded'] - order_detail_df['a_and_m_tax_excluded']) / order_detail_df['net_revenue_tax_excluded']

    gross_margin = pd.DataFrame(order_detail_df.groupby(['date']).apply(wavg, 'gross_margin', 'basket_value_tax_excluded')).reset_index()
    gross_margin.columns = ['date', 'gross_margin']

    net_margin = pd.DataFrame(order_detail_df.groupby(['date']).apply(wavg, 'net_margin', 'net_revenue_tax_excluded')).reset_index()
    net_margin.columns = ['date', 'net_margin']
    margin_df = pd.merge(gross_margin, net_margin, on=['date'], how='outer')

    # Order Financials

    order_financials = order_detail_df.groupby(['date']).agg({'order_id': ['count'], 'client_id': ['nunique'], 'total_item_count': ['mean'],
                                                              'unique_item_count': ['mean'],
                                                              'basket_value': ['sum'],
                                                              'charged_amount': ['sum'],
                                                              'net_revenue': ['sum'],
                                                              'net_revenue_tax_excluded': ['sum'],
                                                              'basket_value_tax_excluded': ['mean'], 'discount_amount_tax_excluded': ['mean'],
                                                              'charged_amount_tax_excluded': ['mean'], 'cogs_tax_excluded': ['mean'],
                                                              'cogs_of_discount_tax_excluded': ['mean'], 'a_and_m_tax_excluded': ['mean'],
                                                              'supplier_support_tax_excluded': ['mean'], 'third_party_support_tax_excluded': ['mean']

                                                              }).reset_index()

    order_financials.columns = order_financials.columns.droplevel(1)

    # Promo Count and Basket Value

    order_detail_df['order_with_promo'] = order_detail_df['promo'].apply(lambda x: True if x is not None else False)
    promo_count = pd.DataFrame(order_detail_df[order_detail_df['order_with_promo'] == True].groupby(['date']).agg({'order_with_promo': ['count']})).reset_index()
    promo_count.columns = promo_count.columns.droplevel(1)
    promo_count.columns = ['date', 'order_with_promo_count']

    basket_value_by_promo_status = order_detail_df.pivot_table(values=['basket_value_tax_excluded'], index=['date'], columns=['order_with_promo'],
                                                               aggfunc={'basket_value_tax_excluded': 'mean'}, fill_value=0).reset_index()

    basket_value_by_promo_status.columns = basket_value_by_promo_status.columns.droplevel(1)
    basket_value_by_promo_status.columns = ['date', 'avg_basket_value_tax_exc_in_organic_baskets', 'avg_basket_value_tax_exc_with_promo']

    # tab_click_count['date'] = pd.to_datetime(tab_click_count['date'])
    # commertial_df = pd.merge(tab_click_count, order_financials, on=['date'], how='left')
    # commertial_df['Conversion(Order/Visitor)'] = commertial_df['order_id'] / commertial_df['unique_visitor']
    # commertial_df['Conversion(Client/Visitor)'] = commertial_df['client_id'] / commertial_df['unique_visitor']

    # order_detail_df['date'] = pd.to_datetime(order_detail_df['date'])
    # promo_count['date'] = pd.to_datetime(promo_count['date'])
    # order_durations['date'] = pd.to_datetime(order_durations['date'])
    # order_financials['date'] = pd.to_datetime(order_financials['date'])
    # margin_df['date'] = pd.to_datetime(margin_df['date'])

    commertial_df = pd.merge(order_financials, order_durations, on=['date'], how='outer')
    commertial_df = pd.merge(commertial_df, margin_df, on=['date'], how='outer')
    commertial_df = pd.merge(commertial_df, promo_count, on=['date'], how='outer')

    commertial_df = pd.merge(commertial_df, basket_value_by_promo_status, on=['date'], how='outer')

    print(order_detail_df.info())
    print(commertial_df.info())


