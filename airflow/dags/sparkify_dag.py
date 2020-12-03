from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries
from sparkify_subdag import load_dim_dag

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'danielarevalo',
    'start_date': datetime(2020, 12, 1),
    'end_date': datetime(2020, 12, 2),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

dag = DAG('sparkify_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          max_active_runs=1
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    provide_context=True,
    table="staging_events",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    region="us-west-2",
    file_format="JSON"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    provide_context=True,
    table="staging_songs",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    region="us-west-2",
    data_format="JSON"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    provide_context=True,
    aws_credentials_id="aws_credentials",
    redshift_conn_id='redshift',
    sql_query=SqlQueries.songplay_table_insert
)

load_user_dimension_table = SubDagOperator(
    subdag=load_dim_dag(
        parent_dag_name='sparkify_dag',
        task_id='Load_user_dim_table',
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        start_date= datetime(2020, 12, 1),
        table="users",
        sql_query=SqlQueries.user_table_insert,
    ),
    task_id='Load_user_dim_table',
    dag=dag
)

load_song_dimension_table = SubDagOperator(
    subdag=load_dim_dag(
        parent_dag_name='sparkify_dag',
        task_id='Load_song_dim_table',
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        start_date= datetime(2020, 12, 1),
        table="songs",
        sql_query=SqlQueries.song_table_insert,
    ),
    task_id='Load_song_dim_table',
    dag=dag
)

load_artist_dimension_table = SubDagOperator(
    subdag=load_dim_dag(
        parent_dag_name='sparkify_dag',
        task_id='Load_artist_dim_table',
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="artists",
        start_date= datetime(2020, 12, 1),
        sql_query=SqlQueries.artist_table_insert,
    ),
    task_id='Load_artist_dim_table',
    dag=dag
)

load_time_dimension_table = SubDagOperator(
    subdag=load_dim_dag(
        parent_dag_name='sparkify_dag',
        task_id='Load_time_dim_table',
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="time",
        start_date= datetime(2020, 12, 1),
        sql_query=SqlQueries.time_table_insert,
    ),
    task_id='Load_time_dim_table',
    dag=dag
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    provide_context=True,
    aws_credentials_id="aws_credentials",
    redshift_conn_id='redshift',
    dq_checks=[
                    {'check_sql':'SELECT COUNT(*) FROM songplay WHERE playid is null' , 'expected_result': 0},
                    {'check_sql':'SELECT COUNT(*) FROM users WHERE userid is null' , 'expected_result': 0},
                    {'check_sql':'SELECT COUNT(*) FROM songs WHERE songid is null' , 'expected_result': 0},
                    {'check_sql':'SELECT COUNT(*) FROM artists WHERE artistid is null' , 'expected_result': 0},
                    {'check_sql':'SELECT COUNT(*) FROM time WHERE start_time is null' , 'expected_result': 0},
                ]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> [stage_songs_to_redshift, stage_events_to_redshift]

[stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table

load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks

run_quality_checks >> end_operator
