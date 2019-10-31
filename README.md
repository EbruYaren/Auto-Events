# cron_template
## Cron Information

* Frequency: Daily
* Parameters: Ex: 2019-10-30 2019-10-31
* Description: Calculates daily Getir Buyuk metrics

### Databases & Environment Variables

| Database                               | Permission        | Collections                                                      | Environment Name              |
| -----------------                      |:----------------: | ------------------                                               | ------------------------------|
| G30_REDSHIFT                           |      read         | <collection_name>                                                | G30_REDSHIFT                  |
| MONGO_GETIREVENTS_LIVE                 |      read         | <collection_name>                                                | MONGO_GETIREVENTS_LIVE        |
| REDSHIFT_MARKET_ANALYTICS_LIVE         |      write        | daily_stats, missed_orders, order_details                        | REDSHIFT_MARKET_ANALYTICS_LIVE|
| MONGO_MARKET_ANALYSIS_LIVE             |      write        | marketdailysummaries                                             |

### Optional Environment Variables
There are no optional environment variables.





