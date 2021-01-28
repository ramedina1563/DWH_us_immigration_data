import datetime
from datetime import datetime, timedelta
from airflow import DAG

from airflow.operators import (
    PostgresOperator,
    LoadFactOperator,
    LoadDimensionOperator,
    StageToRedshiftOperator,
    DataQualityOperator
)
from airflow.operators.dummy_operator import DummyOperator
from helpers import SqlQueries
import create_tables

start_date = datetime.utcnow()

dag = DAG("capstone",
         description="Capstone project load and transform US immigration data into Redshift",
         start_date=start_date,
         schedule_interval=None
         )

# The data pipeline was oganized in the following way:
# start_operator: It has the purpose of initializing the pipeline. It's there for visualization purposes
# create_immigration_staging_table: This task creates staging table 'staging_immigration'
# create_demographics_staging_table: This task creates staging table 'staging_usdemographics'
# create_climate_staging_table: This task creates staging table 'staging_climate'
# create_airportcodes_staging_table: This task creates staging table 'staging_airportcodes'
# copy_migration_task: This task loads data from S3 into Redshift into table staging_immigration
# copy_demographics_task: This task loads data from S3 into Redshift into table staging_usdemographics
# copy_climate_task: This task loads data from S3 into Redshift into table staging_climate
# copy_airportcodes_task: This task loads data from S3 into Redshift into table staging_airportcodes
# create_immigration_table: This task creates table 'immigration'
# create_persons_table: This task creates table 'persons'
# create_dates_table: This task creates table 'dates'
# create_city_table: This task creates table 'city'
# load_fact_table_task: This task loads data to fact table immigration from staging tables
# load_persons_table_task: This task loads data to table persons from staging tables
# load_dates_table_task: This task loads data to table dates from staging tables
# load_city_table_task: This task loads data to table city from staging tables
# run_quality_checks: This operator executes a quality check on the resulting tables of the data warehouse. For this, you need to input a SQL query and its expected outcome
# finish_operator: Ti finalizes the data pipeline. It's there for visualization purposes

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

# For this task you have to enter following fields:
#   - task_id: Name of the task
#   - postgres_conn_id: Name of the connection to a Postgres database. In this case 'redshift' which is configurable on Airflow
#   - sql: SQL query to create the table
create_immigration_staging_table = PostgresOperator(
    task_id="create_immigration_staging_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=create_tables.staging_immigration_table_create
)

# For this task you have to enter following fields:
#   - task_id: Name of the task
#   - postgres_conn_id: Name of the connection to a Postgres database. In this case 'redshift' which is configurable on Airflow
#   - sql: SQL query to create the table
create_demographics_staging_table = PostgresOperator(
    task_id="create_demographics_staging_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=create_tables.staging_demographics_table_create
)

# For this task you have to enter following fields:
#   - task_id: Name of the task
#   - postgres_conn_id: Name of the connection to a Postgres database. In this case 'redshift' which is configurable on Airflow
#   - sql: SQL query to create the table
create_climate_staging_table = PostgresOperator(
    task_id="create_climate_staging_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=create_tables.staging_climate_table_create
)

# For this task you have to enter following fields:
#   - task_id: Name of the task
#   - postgres_conn_id: Name of the connection to a Postgres database. In this case 'redshift' which is configurable on Airflow
#   - sql: SQL query to create the table
create_airportcodes_staging_table = PostgresOperator(
    task_id="create_airportcodes_staging_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=create_tables.staging_airportcodes_table_create
)

# For this task you have to enter following fields:
#   - task_id: Name of the task
#   - postgres_conn_id: Name of the connection to a Postgres database. In this case 'redshift' which is configurable on Airflow
#   - aws_credentials_id: Access key and Secret Key to access the aws. This is configurable on Airflow
#   - table: Name of the table where data will be copied from S3 to Redshift to
#   - s3 bucket. In this case 'udacity-dend'
#   - s3 key. In this case 'log_data'
copy_migration_task = StageToRedshiftOperator(
    task_id="copy_immigration",
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_immigration",
    s3_bucket="capstone-usimmigration",
    s3_key="immigration.csv",
    delimiter=",",
    provide_context=True
)

# For this task you have to enter following fields:
#   - task_id: Name of the task
#   - postgres_conn_id: Name of the connection to a Postgres database. In this case 'redshift' which is configurable on Airflow
#   - aws_credentials_id: Access key and Secret Key to access the aws. This is configurable on Airflow
#   - table: Name of the table where data will be copied from S3 to Redshift to
#   - s3 bucket. In this case 'udacity-dend'
#   - s3 key. In this case 'log_data'
copy_demographics_task = StageToRedshiftOperator(
    task_id="copy_usdemographics",
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_usdemographics",
    s3_bucket="capstone-usimmigration",
    s3_key="us_demographics.csv",
    delimiter=",",
    provide_context=True
)

# For this task you have to enter following fields:
#   - task_id: Name of the task
#   - postgres_conn_id: Name of the connection to a Postgres database. In this case 'redshift' which is configurable on Airflow
#   - aws_credentials_id: Access key and Secret Key to access the aws. This is configurable on Airflow
#   - table: Name of the table where data will be copied from S3 to Redshift to
#   - s3 bucket. In this case 'udacity-dend'
#   - s3 key. In this case 'log_data'
copy_climate_task = StageToRedshiftOperator(
    task_id="copy_climate",
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_climate",
    s3_bucket="capstone-usimmigration",
    s3_key="temperaturesCity.csv",
    delimiter=",",
    provide_context=True
)

# For this task you have to enter following fields:
#   - task_id: Name of the task
#   - postgres_conn_id: Name of the connection to a Postgres database. In this case 'redshift' which is configurable on Airflow
#   - aws_credentials_id: Access key and Secret Key to access the aws. This is configurable on Airflow
#   - table: Name of the table where data will be copied from S3 to Redshift to
#   - s3 bucket. In this case 'udacity-dend'
#   - s3 key. In this case 'log_data'
copy_airportcodes_task = StageToRedshiftOperator(
    task_id="copy_airportcodes",
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_airportcodes",
    s3_bucket="capstone-usimmigration",
    s3_key="airport_codes.csv",
    delimiter=";",
    provide_context=True
)

# For this task you have to enter following fields:
#   - task_id: Name of the task
#   - postgres_conn_id: Name of the connection to a Postgres database. In this case 'redshift' which is configurable on Airflow
#   - sql: SQL query to create the table
create_immigration_table = PostgresOperator(
    task_id="create_immigration_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=create_tables.immigration_table_create
)

# For this task you have to enter following fields:
#   - task_id: Name of the task
#   - postgres_conn_id: Name of the connection to a Postgres database. In this case 'redshift' which is configurable on Airflow
#   - sql: SQL query to create the table
create_persons_table = PostgresOperator(
    task_id="create_persons_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=create_tables.persons_table_create
)

# For this task you have to enter following fields:
#   - task_id: Name of the task
#   - postgres_conn_id: Name of the connection to a Postgres database. In this case 'redshift' which is configurable on Airflow
#   - sql: SQL query to create the table
create_dates_table = PostgresOperator(
    task_id="create_dates_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=create_tables.dates_table_create
)

# For this task you have to enter following fields:
#   - task_id: Name of the task
#   - postgres_conn_id: Name of the connection to a Postgres database. In this case 'redshift' which is configurable on Airflow
#   - sql: SQL query to create the table
create_city_table = PostgresOperator(
    task_id="create_city_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=create_tables.city_table_create
)

# For this task you have to enter following fields:
#   - task_id: Name of the task
#   - postgres_conn_id: Name of the connection to a Postgres database. In this case 'redshift' which is configurable on Airflow
#   - schema: Enter schema of the table. In this case 'public'
#   - table: Name of the table where data will be inserted into
#   - sql: SQL query to insert data into the table
load_fact_table_task = LoadFactOperator(
    task_id="load_fact_table_immigration",
    dag=dag,
    redshift_conn_id="redshift",
    schema="public",
    table="immigration",
    sql_query=SqlQueries.immigration_table_insert
)

# For this task you have to enter following fields:
#   - task_id: Name of the task
#   - postgres_conn_id: Name of the connection to a Postgres database. In this case 'redshift' which is configurable on Airflow
#   - schema: Enter schema of the table. In this case 'public'
#   - table: Name of the table where data will be inserted into
#   - sql: SQL query to insert data into the table
#   - truncate: Enter True or False
load_persons_table_task = LoadDimensionOperator(
    task_id="load_dimension_table_persons",
    dag=dag,
    redshift_conn_id="redshift",
    schema="public",
    table="persons",
    sql_query=SqlQueries.persons_table_insert,
    truncate=True
)

# For this task you have to enter following fields:
#   - task_id: Name of the task
#   - postgres_conn_id: Name of the connection to a Postgres database. In this case 'redshift' which is configurable on Airflow
#   - schema: Enter schema of the table. In this case 'public'
#   - table: Name of the table where data will be inserted into
#   - sql: SQL query to insert data into the table
#   - truncate: Enter True or False
load_dates_table_task = LoadDimensionOperator(
    task_id="load_dimension_table_dates",
    dag=dag,
    redshift_conn_id="redshift",
    schema="public",
    table="dates",
    sql_query=SqlQueries.dates_table_insert,
    truncate=True
)

# For this task you have to enter following fields:
#   - task_id: Name of the task
#   - postgres_conn_id: Name of the connection to a Postgres database. In this case 'redshift' which is configurable on Airflow
#   - schema: Enter schema of the table. In this case 'public'
#   - table: Name of the table where data will be inserted into
#   - sql: SQL query to insert data into the table
#   - truncate: Enter True or False
load_city_table_task = LoadDimensionOperator(
    task_id="load_dimension_table_city",
    dag=dag,
    redshift_conn_id="redshift",
    schema="public",
    table="city",
    sql_query=SqlQueries.city_table_insert,
    truncate=True
)

# For this task you have to enter following fields:
#   - task_id: Name of the task
#   - postgres_conn_id: Name of the connection to a Postgres database. In this case 'redshift' which is configurable on Airflow
#   - sql: Enter a list of SQL queries
#   - operation: Enter a list with the operations you'd like to perform between the SQL query entered and the expected outcome
#   - outcome: Enter a list with the expected outcomes of the queries
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",    
    sql=["select count(*) from public.persons where persons_id is null", "select count(*) from public.immigration where migration_id is null", "select count(distinct state) from public.city", "select distinct year from public.dates", "select count(distinct date_id) from public.dates", "select count(*) from public.persons where age > 120"],
    operation=["equal","equal","less","equal","less","equal"],
    outcome=[0,0,50,2016,365,0]
)
    
finish_operator = DummyOperator(task_id='Finalizing_execution',  dag=dag)

start_operator >> [create_immigration_staging_table, create_demographics_staging_table, create_climate_staging_table, create_airportcodes_staging_table]
create_immigration_staging_table >> copy_migration_task
create_demographics_staging_table >> copy_demographics_task
create_climate_staging_table >> copy_climate_task
create_airportcodes_staging_table >> copy_airportcodes_task
[copy_migration_task, copy_demographics_task, copy_climate_task, copy_airportcodes_task] >> create_immigration_table
create_immigration_table >> [create_persons_table, create_dates_table, create_city_table]
[create_persons_table, create_dates_table, create_city_table] >> load_fact_table_task
load_fact_table_task >> [load_persons_table_task, load_dates_table_task, load_city_table_task]
[load_persons_table_task, load_dates_table_task, load_city_table_task] >> run_quality_checks
run_quality_checks >> finish_operator