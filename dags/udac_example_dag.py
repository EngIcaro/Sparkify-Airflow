from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2018, 11, 1),
    # task can run idenpendent if the previous run of the task in the previous DAG Run succeeded.
    'depends_on_past':False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    # Airflow starts processing from the current interval
    'catchup':False,
    'email_on_retry': False,
}

dag = DAG('sparkify_airflow',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)



stage_events_to_redshift = StageToRedshiftOperator(
    task_id            ='Stage_events'     ,
    postgres_conn_id   = "redshift"        ,
    aws_credentials_id = "aws_credentials" ,
    s3_bucket          = "udacity-dend"    ,
    s3_json_data       = "log_data/{execution_date.year}/{execution_date.month}/",
    table_output       = "staging_events"    ,
    json_path          ="s3://udacity-dend/log_json_path.json", 
    dag                =dag
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id            = 'Stage_songs',
    postgres_conn_id   = "redshift"        ,
    aws_credentials_id = "aws_credentials" ,
    s3_bucket          = "udacity-dend"    ,
    s3_json_data       = "song_data/A/A/"  ,
    table_output       = "staging_songs"   ,
    json_path          ="auto", 
    dag=dag
)


load_songplays_table = LoadFactOperator(
    task_id            = 'Load_songplays_fact_table'  ,
    postgres_conn_id   = "redshift"                   ,
    fact_table         = "fact_songplays"             ,
    dag=dag
)



load_user_dimension_table = LoadDimensionOperator(
    task_id            = 'Load_user_dim_table'        ,
    postgres_conn_id   = "redshift"                   ,
    dim_table          = "dim_users"                  ,
    sql_query          = SqlQueries.user_table_insert ,
    dag=dag

)


load_song_dimension_table = LoadDimensionOperator(
    task_id            = 'Load_song_dim_table'        ,
    postgres_conn_id   = "redshift"                   ,
    dim_table          = "dim_songs"                  ,
    sql_query          = SqlQueries.song_table_insert ,
    dag=dag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id            = 'Load_artist_dim_table'      ,
    postgres_conn_id   = "redshift"                   ,
    dim_table          = "dim_artists"                ,
    sql_query          = SqlQueries.artist_table_insert,
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id            = 'Load_time_dim_table'        ,
    postgres_conn_id   = "redshift"                   ,
    dim_table          = "dim_time"                   ,
    sql_query          =  SqlQueries.time_table_insert,
    dag=dag
)


run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks'                ,
    postgres_conn_id   = "redshift"                  ,
    tables_quality = ["fact_songplays","dim_users","dim_songs","dim_artists","dim_time" ],
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> [stage_events_to_redshift,stage_songs_to_redshift]
[stage_events_to_redshift,stage_songs_to_redshift] >> load_songplays_table
load_songplays_table >> [load_user_dimension_table, load_song_dimension_table,
                        load_artist_dimension_table, load_time_dimension_table]
[load_user_dimension_table, load_song_dimension_table,
 load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks
run_quality_checks >> end_operator

