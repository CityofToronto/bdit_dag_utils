- [Intro](#intro)
  - [airflow\_health\_monitoring.py](#airflow_health_monitoringpy)
  - [log\_cleanup.py](#log_cleanuppy)
  - [monitor\_db\_sessions.py](#monitor_db_sessionspy)
  - [monitor\_db\_size.py](#monitor_db_sizepy)
  - [monthly\_vacuum.py](#monthly_vacuumpy)
  - [sirius\_bancroft\_check.py](#sirius_bancroft_checkpy)

# Intro

This repo holds Airflow DAGs which can be deployed across different servers to aim in regular database and server administration tasks.

## [airflow_health_monitoring.py](airflow_health_monitoring.py)
A daily DAG that runs on Morbius (checks EC2/Bancroft) and Bancroft (checks Morbius) to see if all Airflow services are "Healthy". Sends a daily slack message whether healthy or not, so the absence of the message should also be alarming. 

## [log_cleanup.py](log_cleanup.py)
A daily DAG that runs on various servers to 

## [monitor_db_sessions.py](monitor_db_sessions.py)
A daily DAG that runs on EC2 which pokes every 10 minutes to record any long running queries to `public.session_log` along the the user who ran them. Because the check runs only every 10 minutes, it is possible to miss many queries that are between 10-20 minutes long, but no queries of more than 20 minutes should be missed.
This DAG is intended for admins to be able to keep a pulse on slow queries in order to offer query tips to their authors, or potential architecture improvements to reduce run times.

## [monitor_db_size.py](monitor_db_size.py)
A daily DAG that runs on various servers which logs the size of tables and schemas. Sends slack notifications when the database is approaching full.
The max size of the database is taken from a manually maintained airflow variable (see `airflow_var` variable in the DAG for identifying this variable on each server).

## [monthly_vacuum.py](monthly_vacuum.py)
A monthly DAG intended to run on various servers which generates `VACUUM FULL` statements for all tables with the previous month (`YYYYMM`) in the name. This is because once the month is finished, we are unlikely to update these tables and can save significant disc space through vacuuming. The Auto-Vacuum process in Postgres will never propose `VACUUM FULL` operation which locks the table but frees up a lot more space. 
A schedule of the end of the 3rd of the month at the end of the day was chosen because most tables should be done with inserts by then, and the script should be run after hours if possible to avoid locking up tables used for analysis.

## [sirius_bancroft_check.py](sirius_bancroft_check.py)
A currently paused DAG which compares row counts between Bancroft and Sirius for VFH data to ensure logical replication is working.
