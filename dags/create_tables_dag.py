
from datetime import datetime
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from helpers import SqlQueries
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from helpers import SqlQueries
import logging

def create_tables(*args, **kwargs):

    # Acesso ao meu Redshift do airflow
    logging.info("Connecting to Redshift")
    redshift_hook = PostgresHook("redshift")

    logging.info("Creating tables")
    redshift_hook.run(SqlQueries.staging_events_create)
    redshift_hook.run(SqlQueries.staging_songs_create)
    redshift_hook.run(SqlQueries.fact_songplays_create)
    redshift_hook.run(SqlQueries.user_table_create)
    redshift_hook.run(SqlQueries.song_table_create)
    redshift_hook.run(SqlQueries.artist_table_create)
    redshift_hook.run(SqlQueries.time_table_create)


dag = DAG('create_tables_sparkify',
          schedule_interval='@once',
          start_date=datetime.now(),
          description='Creates tables in redshift',
        )


creates_tables_task = PythonOperator(
    task_id='creates_tables',
    dag=dag,
    # Load data to S3 From trips tables in redshift
    python_callable=create_tables
)