#AUTO-EVENT

##Dependent Crons:
There are no dependency
## Cron Information

* Frequency: Hourly
* Description: This cron generates predicted reach time and locations for orders with status 900 or 1000 and domaintype 1.

### Databases & Environment Variables

| Database          | Permission        | Collections,Schema       | Environment Name            
| ----------------- |:----------------: | ------------------ | ---------------------------   
| ETL_CLUSTER       |      (r/w)        | project_auto_events| REDSHIFT_ETL_URI       
| MAIN_DB           |         r         | orders, clients    |          MAIN_DB_URI           








