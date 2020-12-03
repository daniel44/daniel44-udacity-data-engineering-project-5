from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

def load_dim_dag(
    parent_dag_name,
    task_id,
    redshift_conn_id,
    aws_credentials_id,
    table,
    sql_query,
    *args, **kwargs):
    dag = DAG(
        f"{parent_dag_name}.{task_id}",
        **kwargs
    )
    
    load_dimension = LoadDimensionOperator(
        task_id=f"load_{table}_dim_table",
        dag=dag,
        table=table,
        redshift_conn_id=redshift_conn_id,
        aws_credentials_id=aws_credentials_id,
        sql_query=sql_query
    )

    load_dimension

    return dag
