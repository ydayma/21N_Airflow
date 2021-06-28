from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.models.connection import Connection
import os
import mysql.connector
from mysql.connector import Error
from sqlalchemy import create_engine
from sqlalchemy.types import TIME
import sys
import pandas as pd
from airflow.models import Variable



REPLICA_DB_HOST =Variable.get("replica_db_host") #'3.0.136.197'
REPLICA_DB_USER = Variable.get("replica_db_user")#'yantra'
REPLICA_DB_PASSWORD =Variable.get("replica_db_password") #'0FRZ*j7,>M>C,&tJ'
REPLICA_DB_PORT =Variable.get("replica_db_port") #'3306'
REPLICA_DB =Variable.get("replica_db") #'21north'


SUMMARY_DB_HOST= Variable.get("summary_db_host")#'asia-database-instance-1.cdiynpmpjjje.ap-southeast-1.rds.amazonaws.com'
SUMMARY_DB_USER= Variable.get("summary_db_user")#'analytics'
SUMMARY_DB_PASSWORD= Variable.get("summary_db_password")#'k9%^88*Lf5EDC7KP'
SUMMARY_DB_PORT = Variable.get("summary_db_port")#'3306'
SUMMARY_DB = Variable.get("summary_db")#'analytics'

def ambassador_channel_partner_comission():
    df_ambassador_channel_partner_comission = pd.read_sql_query(
        sql="SELECT * FROM 21N_ambassador_channel_partner_comission where creationdate > NOW()-INTERVAL 1 DAY;",
        con='mysql+mysqldb://'+REPLICA_DB_USER+':'+REPLICA_DB_PASSWORD+'@'+REPLICA_DB_HOST+':'+REPLICA_DB_PORT+'/'+REPLICA_DB+'?ssl_mode=DISABLED')

    print("----------------Connected to Replica DB-------------------------")
    print(df_ambassador_channel_partner_comission.head())
    
    df_ambassador_channel_partner_comission_1 = pd.read_sql_query(
        sql="SELECT * FROM 21N_ambassador_channel_partner_comission where creationdate > NOW()-INTERVAL 1 DAY;",
        con='mysql+mysqldb://'+SUMMARY_DB_USER+':'+SUMMARY_DB_PASSWORD+'@'+SUMMARY_DB_HOST+':'+SUMMARY_DB_PORT+'/'+SUMMARY_DB+'?ssl_mode=DISABLED',)

    
    print('---------------------Connected to Replica db-----------------------')
    print(df_ambassador_channel_partner_comission_1.head())

default_args={
            "owner": "airflow",
            "retries": 3,
            "retry_delay": timedelta(minutes=5),
            "start_date": datetime(2021, 6, 22),
}

# Schedule Interval set to UTC time zone. TO be run everyday @ 1AM IST which is 5:30PM UTC
dag = DAG(
        dag_id='test',
        schedule_interval="@daily",  #'45 23 * * *',
        default_args= default_args,
        catchup=False,)


ambassador_channel_partner_comission = PythonOperator(
    task_id="ambassador_channel_partner_comission",
    python_callable = ambassador_channel_partner_comission,
    provide_context = True,
    dag=dag,
)
