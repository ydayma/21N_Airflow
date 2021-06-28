
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

REPLICA_DB_HOST =Variable.get("replica_db_host") 
REPLICA_DB_USER = Variable.get("replica_db_user")
REPLICA_DB_PASSWORD =Variable.get("replica_db_password") 
REPLICA_DB_PORT =Variable.get("replica_db_port") 
REPLICA_DB =Variable.get("replica_db") 


SUMMARY_DB_HOST= Variable.get("summary_db_host")
SUMMARY_DB_USER= Variable.get("summary_db_user")
SUMMARY_DB_PASSWORD= Variable.get("summary_db_password")
SUMMARY_DB_PORT = Variable.get("summary_db_port")
SUMMARY_DB = Variable.get("summary_db")


# #Localhost details - con="mysql://"+LOCAL_DB_USER+":"+LOCAL_DB_PASSWORD+"@"+LOCAL_DB_HOST+":"+LOCAL_DB_PORT+"/"+LOCAL_DB
# LOCAL_DB_HOST = 'host.docker.internal'
# LOCAL_DB_USER = 'root'
# LOCAL_DB_PASSWORD = 'password'
# LOCAL_DB = '21north'
# LOCAL_DB_PORT = '3306'




   
def ambassador_channel_partner_comission(*args, **context):
    print("------------------------------------Inside Function-----------------------------")

    df_ambassador_channel_partner_comission = pd.read_sql_query(
        sql="SELECT * FROM 21N_ambassador_channel_partner_comission where creationdate > NOW()-INTERVAL 1 DAY;",
        con='mysql+mysqldb://'+REPLICA_DB_USER+':'+REPLICA_DB_PASSWORD+'@'+REPLICA_DB_HOST+':'+REPLICA_DB_PORT+'/'+REPLICA_DB+'?ssl_mode=DISABLED')

    print("-------------------------------Data read into dataframe---------------------------")

    df_ambassador_channel_partner_comission.to_sql(
        name="21N_ambassador_channel_partner_comission",
        con='mysql+mysqldb://'+SUMMARY_DB_USER+':'+SUMMARY_DB_PASSWORD+'@'+SUMMARY_DB_HOST+':'+SUMMARY_DB_PORT+'/'+SUMMARY_DB+'?ssl_mode=DISABLED',
        schema=SUMMARY_DB,
        if_exists="append",
        index=False)

    print('----------------------------------Data written into summary DB----------------------')


def ambassador_channel_partner(*args, **context):
    print("------------------------------------Inside Function-----------------------------")

    df_ambassador_channel_partner = pd.read_sql_query(
        sql="SELECT * FROM 21N_ambassador_channel_partner where creationdatetime > NOW()-INTERVAL 1 DAY;",
        con='mysql+mysqldb://'+REPLICA_DB_USER+':'+REPLICA_DB_PASSWORD+'@'+REPLICA_DB_HOST+':'+REPLICA_DB_PORT+'/'+REPLICA_DB+'?ssl_mode=DISABLED')

    print("-------------------------------Data read into dataframe---------------------------")

    df_ambassador_channel_partner.to_sql(
        name="21N_ambassador_channel_partner",
        con='mysql+mysqldb://'+SUMMARY_DB_USER+':'+SUMMARY_DB_PASSWORD+'@'+SUMMARY_DB_HOST+':'+SUMMARY_DB_PORT+'/'+SUMMARY_DB+'?ssl_mode=DISABLED',
        schema=SUMMARY_DB,
        if_exists="append",
        index=False)

    print('----------------------------------Data written into summary DB----------------------')


def ambassador_channel_partner_tied(*args, **context):
    print("------------------------------------Inside Function-----------------------------")

    df_ambassador_channel_partner_tied = pd.read_sql_query(
        sql="SELECT * FROM 21N_ambassador_channel_partner_tied;",
        con='mysql+mysqldb://'+REPLICA_DB_USER+':'+REPLICA_DB_PASSWORD+'@'+REPLICA_DB_HOST+':'+REPLICA_DB_PORT+'/'+REPLICA_DB+'?ssl_mode=DISABLED')

    print("-------------------------------Data read into dataframe---------------------------")

    df_ambassador_channel_partner_tied.to_sql(
        name="21n_ambassador_channel_partner_tied",
        con='mysql+mysqldb://'+SUMMARY_DB_USER+':'+SUMMARY_DB_PASSWORD+'@'+SUMMARY_DB_HOST+':'+SUMMARY_DB_PORT+'/'+SUMMARY_DB+'?ssl_mode=DISABLED',
        schema=SUMMARY_DB,
        if_exists="replace",
        index=False)

    print('----------------------------------Data written into summary DB----------------------')


def ambassador_info(*args, **context):
    print("------------------------------------Inside Function-----------------------------")

    df_ambassador_info = pd.read_sql_query(
        sql="SELECT * FROM 21N_ambassador_info;",
        con='mysql+mysqldb://'+REPLICA_DB_USER+':'+REPLICA_DB_PASSWORD+'@'+REPLICA_DB_HOST+':'+REPLICA_DB_PORT+'/'+REPLICA_DB+'?ssl_mode=DISABLED')

    print("-------------------------------Data read into dataframe---------------------------")

    df_ambassador_info.to_sql(
        name="21n_ambassador_info",
        con='mysql+mysqldb://'+SUMMARY_DB_USER+':'+SUMMARY_DB_PASSWORD+'@'+SUMMARY_DB_HOST+':'+SUMMARY_DB_PORT+'/'+SUMMARY_DB+'?ssl_mode=DISABLED',
        schema=SUMMARY_DB,
        if_exists="replace",
        index=False)

    print('----------------------------------Data written into summary DB----------------------')


def ambassador_locationbk(*args, **context):
    print("------------------------------------Inside Function-----------------------------")

    df_ambassador_locationbk = pd.read_sql_query(
        sql="SELECT * FROM 21N_ambassador_locationbk where presenttime > NOW()-INTERVAL 1 DAY;",
        con='mysql+mysqldb://'+REPLICA_DB_USER+':'+REPLICA_DB_PASSWORD+'@'+REPLICA_DB_HOST+':'+REPLICA_DB_PORT+'/'+REPLICA_DB+'?ssl_mode=DISABLED')

    print("-------------------------------Data read into dataframe---------------------------")

    df_ambassador_locationbk.to_sql(
        name="21N_ambassador_locationbk",
        con='mysql+mysqldb://'+SUMMARY_DB_USER+':'+SUMMARY_DB_PASSWORD+'@'+SUMMARY_DB_HOST+':'+SUMMARY_DB_PORT+'/'+SUMMARY_DB+'?ssl_mode=DISABLED',
        schema=SUMMARY_DB,
        if_exists="append",
        index=False)

    print('----------------------------------Data written into summary DB----------------------')


def ambassador_login(*args, **context):
    print("------------------------------------Inside Function-----------------------------")

    df_ambassador_login = pd.read_sql_query(
        sql="SELECT * FROM 21N_ambassador_login where creationdate > NOW()-INTERVAL 1 DAY;",
        con='mysql+mysqldb://'+REPLICA_DB_USER+':'+REPLICA_DB_PASSWORD+'@'+REPLICA_DB_HOST+':'+REPLICA_DB_PORT+'/'+REPLICA_DB+'?ssl_mode=DISABLED')

    print("-------------------------------Data read into dataframe---------------------------")

    df_ambassador_login.to_sql(
        name="21N_ambassador_login",
        con='mysql+mysqldb://'+SUMMARY_DB_USER+':'+SUMMARY_DB_PASSWORD+'@'+SUMMARY_DB_HOST+':'+SUMMARY_DB_PORT+'/'+SUMMARY_DB+'?ssl_mode=DISABLED',
        schema=SUMMARY_DB,
        if_exists="append",
        index=False)

    print('----------------------------------Data written into summary DB----------------------')


def ambassador_mishaps(*args, **context):
    print("------------------------------------Inside Function-----------------------------")

    df_ambassador_mishaps = pd.read_sql_query(
        sql="SELECT * FROM 21N_ambassador_mishaps;",
        con='mysql+mysqldb://'+REPLICA_DB_USER+':'+REPLICA_DB_PASSWORD+'@'+REPLICA_DB_HOST+':'+REPLICA_DB_PORT+'/'+REPLICA_DB+'?ssl_mode=DISABLED')

    print("-------------------------------Data read into dataframe---------------------------")

    df_ambassador_mishaps.to_sql(
        name="21n_ambassador_mishaps",
        con='mysql+mysqldb://'+SUMMARY_DB_USER+':'+SUMMARY_DB_PASSWORD+'@'+SUMMARY_DB_HOST+':'+SUMMARY_DB_PORT+'/'+SUMMARY_DB+'?ssl_mode=DISABLED',
        schema=SUMMARY_DB,
        if_exists="replace",
        index=False)

    print('----------------------------------Data written into summary DB----------------------')

def ambassador_ontime_history(*args, **context):
    print("------------------------------------Inside Function-----------------------------")

    df_ambassador_ontime_history = pd.read_sql_query(
        sql="SELECT * FROM 21N_ambassador_ontime_history where added_at > NOW()-INTERVAL 1 DAY;",
        con='mysql+mysqldb://'+REPLICA_DB_USER+':'+REPLICA_DB_PASSWORD+'@'+REPLICA_DB_HOST+':'+REPLICA_DB_PORT+'/'+REPLICA_DB+'?ssl_mode=DISABLED')

    print("-------------------------------Data read into dataframe---------------------------")

    df_ambassador_ontime_history.to_sql(
        name="21N_ambassador_ontime_history",
        con='mysql+mysqldb://'+SUMMARY_DB_USER+':'+SUMMARY_DB_PASSWORD+'@'+SUMMARY_DB_HOST+':'+SUMMARY_DB_PORT+'/'+SUMMARY_DB+'?ssl_mode=DISABLED',
        schema=SUMMARY_DB,
        if_exists="append",
        index=False)

    print('----------------------------------Data written into summary DB----------------------')


def ambassador_refunds_type(*args, **context):
    print("------------------------------------Inside Function-----------------------------")

    df_ambassador_refunds_type = pd.read_sql_query(
        sql="SELECT * FROM 21N_ambassador_refunds_type;",
        con='mysql+mysqldb://'+REPLICA_DB_USER+':'+REPLICA_DB_PASSWORD+'@'+REPLICA_DB_HOST+':'+REPLICA_DB_PORT+'/'+REPLICA_DB+'?ssl_mode=DISABLED')

    print("-------------------------------Data read into dataframe---------------------------")

    df_ambassador_refunds_type.to_sql(
        name="21n_ambassador_refunds_type",
        con='mysql+mysqldb://'+SUMMARY_DB_USER+':'+SUMMARY_DB_PASSWORD+'@'+SUMMARY_DB_HOST+':'+SUMMARY_DB_PORT+'/'+SUMMARY_DB+'?ssl_mode=DISABLED',
        schema=SUMMARY_DB,
        if_exists="replace",
        index=False)

    print('----------------------------------Data written into summary DB----------------------')


def ambassador_refunds(*args, **context):
    print("------------------------------------Inside Function-----------------------------")

    df_ambassador_refunds = pd.read_sql_query(
        sql="SELECT * FROM 21N_ambassador_refunds where creationdate > NOW()-INTERVAL 1 DAY;",
        con='mysql+mysqldb://'+REPLICA_DB_USER+':'+REPLICA_DB_PASSWORD+'@'+REPLICA_DB_HOST+':'+REPLICA_DB_PORT+'/'+REPLICA_DB+'?ssl_mode=DISABLED')

    print("-------------------------------Data read into dataframe---------------------------")

    df_ambassador_refunds.to_sql(
        name="21N_ambassador_refunds",
        con='mysql+mysqldb://'+SUMMARY_DB_USER+':'+SUMMARY_DB_PASSWORD+'@'+SUMMARY_DB_HOST+':'+SUMMARY_DB_PORT+'/'+SUMMARY_DB+'?ssl_mode=DISABLED',
        schema=SUMMARY_DB,
        if_exists="append",
        index=False)

    print('----------------------------------Data written into summary DB----------------------')


def customer_servicecentre_logins(*args, **context):
    print("------------------------------------Inside Function-----------------------------")

    df_customer_servicecentre_logins = pd.read_sql_query(
        sql="SELECT * FROM 21N_customer_servicecentre_logins where createddatetime > NOW()-INTERVAL 1 DAY;",
        con='mysql+mysqldb://'+REPLICA_DB_USER+':'+REPLICA_DB_PASSWORD+'@'+REPLICA_DB_HOST+':'+REPLICA_DB_PORT+'/'+REPLICA_DB+'?ssl_mode=DISABLED')

    print("-------------------------------Data read into dataframe---------------------------")

    df_customer_servicecentre_logins.to_sql(
        name="21N_customer_servicecentre_logins",
        con='mysql+mysqldb://'+SUMMARY_DB_USER+':'+SUMMARY_DB_PASSWORD+'@'+SUMMARY_DB_HOST+':'+SUMMARY_DB_PORT+'/'+SUMMARY_DB+'?ssl_mode=DISABLED',
        schema=SUMMARY_DB,
        if_exists="append",
        index=False)

    print('----------------------------------Data written into summary DB----------------------')


def dealer_master(*args, **context):
    print("------------------------------------Inside Function-----------------------------")

    df_dealer_master = pd.read_sql_query(
        sql="SELECT * FROM 21N_dealer_master;",
        con='mysql+mysqldb://'+REPLICA_DB_USER+':'+REPLICA_DB_PASSWORD+'@'+REPLICA_DB_HOST+':'+REPLICA_DB_PORT+'/'+REPLICA_DB+'?ssl_mode=DISABLED')

    print("-------------------------------Data read into dataframe---------------------------")

    df_dealer_master.to_sql(
        name="21n_dealer_master",
        con='mysql+mysqldb://'+SUMMARY_DB_USER+':'+SUMMARY_DB_PASSWORD+'@'+SUMMARY_DB_HOST+':'+SUMMARY_DB_PORT+'/'+SUMMARY_DB+'?ssl_mode=DISABLED',
        schema=SUMMARY_DB,
        if_exists="replace",
        index=False)

    print('----------------------------------Data written into summary DB----------------------')


def queue_bills_due(*args, **context):
    print("------------------------------------Inside Function-----------------------------")

    df_queue_bills_due = pd.read_sql_query(
        sql="SELECT * FROM 21N_queue_bills_due where paydatetime > NOW()-INTERVAL 1 DAY;",
        con='mysql+mysqldb://'+REPLICA_DB_USER+':'+REPLICA_DB_PASSWORD+'@'+REPLICA_DB_HOST+':'+REPLICA_DB_PORT+'/'+REPLICA_DB+'?ssl_mode=DISABLED')

    print("-------------------------------Data read into dataframe---------------------------")

    df_queue_bills_due.to_sql(
        name="21N_queue_bills_due",
        con='mysql+mysqldb://'+SUMMARY_DB_USER+':'+SUMMARY_DB_PASSWORD+'@'+SUMMARY_DB_HOST+':'+SUMMARY_DB_PORT+'/'+SUMMARY_DB+'?ssl_mode=DISABLED',
        schema=SUMMARY_DB,
        if_exists="append",
        index=False)

    print('----------------------------------Data written into summary DB----------------------')


def queue_cash_collected_multiple_queueid(*args, **context):
    print("------------------------------------Inside Function-----------------------------")

    df_queue_cash_collected_multiple_queueid = pd.read_sql_query(
        sql="SELECT * FROM 21N_queue_cash_collected_multiple_queueid where creationdate > NOW()-INTERVAL 1 DAY;",
        con='mysql+mysqldb://'+REPLICA_DB_USER+':'+REPLICA_DB_PASSWORD+'@'+REPLICA_DB_HOST+':'+REPLICA_DB_PORT+'/'+REPLICA_DB+'?ssl_mode=DISABLED')

    print("-------------------------------Data read into dataframe---------------------------")

    df_queue_cash_collected_multiple_queueid.to_sql(
        name="21N_queue_cash_collected_multiple_queueid",
        con='mysql+mysqldb://'+SUMMARY_DB_USER+':'+SUMMARY_DB_PASSWORD+'@'+SUMMARY_DB_HOST+':'+SUMMARY_DB_PORT+'/'+SUMMARY_DB+'?ssl_mode=DISABLED',
        schema=SUMMARY_DB,
        if_exists="append",
        index=False)

    print('----------------------------------Data written into summary DB----------------------')


def queue_completedby(*args, **context):
    print("------------------------------------Inside Function-----------------------------")

    df_queue_completedby = pd.read_sql_query(
        sql="SELECT * FROM 21N_queue_completedby where creationdate > NOW()-INTERVAL 1 DAY;",
        con='mysql+mysqldb://'+REPLICA_DB_USER+':'+REPLICA_DB_PASSWORD+'@'+REPLICA_DB_HOST+':'+REPLICA_DB_PORT+'/'+REPLICA_DB+'?ssl_mode=DISABLED')

    print("-------------------------------Data read into dataframe---------------------------")

    df_queue_completedby.to_sql(
        name="21N_queue_completedby",
        con='mysql+mysqldb://'+SUMMARY_DB_USER+':'+SUMMARY_DB_PASSWORD+'@'+SUMMARY_DB_HOST+':'+SUMMARY_DB_PORT+'/'+SUMMARY_DB+'?ssl_mode=DISABLED',
        schema=SUMMARY_DB,
        if_exists="append",
        index=False)

    print('----------------------------------Data written into summary DB----------------------')


def queue_distance_eta(*args, **context):
    print("------------------------------------Inside Function-----------------------------")

    df_queue_distance_eta = pd.read_sql_query(
        sql="SELECT * FROM 21N_queue_distance_eta where creationdatetime > NOW()-INTERVAL 1 DAY;",
        con='mysql+mysqldb://'+REPLICA_DB_USER+':'+REPLICA_DB_PASSWORD+'@'+REPLICA_DB_HOST+':'+REPLICA_DB_PORT+'/'+REPLICA_DB+'?ssl_mode=DISABLED')

    print("-------------------------------Data read into dataframe---------------------------")

    df_queue_distance_eta.to_sql(
        name="21N_queue_distance_eta",
        con='mysql+mysqldb://'+SUMMARY_DB_USER+':'+SUMMARY_DB_PASSWORD+'@'+SUMMARY_DB_HOST+':'+SUMMARY_DB_PORT+'/'+SUMMARY_DB+'?ssl_mode=DISABLED',
        schema=SUMMARY_DB,
        if_exists="append",
        index=False)

    print('----------------------------------Data written into summary DB----------------------')


def queue_is_trip_based(*args, **context):
    print("------------------------------------Inside Function-----------------------------")

    df_queue_is_trip_based = pd.read_sql_query(
        sql="SELECT * FROM 21N_queue_is_trip_based where creationdatetime > NOW()-INTERVAL 1 DAY;",
        con='mysql+mysqldb://'+REPLICA_DB_USER+':'+REPLICA_DB_PASSWORD+'@'+REPLICA_DB_HOST+':'+REPLICA_DB_PORT+'/'+REPLICA_DB+'?ssl_mode=DISABLED')

    print("-------------------------------Data read into dataframe---------------------------")

    df_queue_is_trip_based.to_sql(
        name="21N_queue_is_trip_based",
        con='mysql+mysqldb://'+SUMMARY_DB_USER+':'+SUMMARY_DB_PASSWORD+'@'+SUMMARY_DB_HOST+':'+SUMMARY_DB_PORT+'/'+SUMMARY_DB+'?ssl_mode=DISABLED',
        schema=SUMMARY_DB,
        if_exists="append",
        index=False)

    print('----------------------------------Data written into summary DB----------------------')


def queue_prepaid_list(*args, **context):
    print("------------------------------------Inside Function-----------------------------")

    df_queue_prepaid_list = pd.read_sql_query(
        sql="SELECT * FROM 21N_queue_prepaid_list where creationdatetime > NOW()-INTERVAL 1 DAY;",
        con='mysql+mysqldb://'+REPLICA_DB_USER+':'+REPLICA_DB_PASSWORD+'@'+REPLICA_DB_HOST+':'+REPLICA_DB_PORT+'/'+REPLICA_DB+'?ssl_mode=DISABLED')

    print("-------------------------------Data read into dataframe---------------------------")

    df_queue_prepaid_list.to_sql(
        name="21N_queue_prepaid_list",
        con='mysql+mysqldb://'+SUMMARY_DB_USER+':'+SUMMARY_DB_PASSWORD+'@'+SUMMARY_DB_HOST+':'+SUMMARY_DB_PORT+'/'+SUMMARY_DB+'?ssl_mode=DISABLED',
        schema=SUMMARY_DB,
        if_exists="append",
        index=False)

    print('----------------------------------Data written into summary DB----------------------')


def queue_repeat(*args, **context):
    print("------------------------------------Inside Function-----------------------------")

    df_queue_repeat = pd.read_sql_query(
        sql="SELECT * FROM 21N_queue_repeat where creationdatetime > NOW()-INTERVAL 1 DAY;",
        con='mysql+mysqldb://'+REPLICA_DB_USER+':'+REPLICA_DB_PASSWORD+'@'+REPLICA_DB_HOST+':'+REPLICA_DB_PORT+'/'+REPLICA_DB+'?ssl_mode=DISABLED')

    print("-------------------------------Data read into dataframe---------------------------")

    df_queue_repeat.to_sql(
        name="21N_queue_repeat",
        con='mysql+mysqldb://'+SUMMARY_DB_USER+':'+SUMMARY_DB_PASSWORD+'@'+SUMMARY_DB_HOST+':'+SUMMARY_DB_PORT+'/'+SUMMARY_DB+'?ssl_mode=DISABLED',
        schema=SUMMARY_DB,
        if_exists="append",
        index=False)

    print('----------------------------------Data written into summary DB----------------------')


def queue_rescheduled(*args, **context):
    print("------------------------------------Inside Function-----------------------------")

    df_queue_rescheduled = pd.read_sql_query(
        sql="SELECT * FROM 21N_queue_rescheduled where creationdate > NOW()-INTERVAL 1 DAY;",
        con='mysql+mysqldb://'+REPLICA_DB_USER+':'+REPLICA_DB_PASSWORD+'@'+REPLICA_DB_HOST+':'+REPLICA_DB_PORT+'/'+REPLICA_DB+'?ssl_mode=DISABLED')

    print("-------------------------------Data read into dataframe---------------------------")

    df_queue_rescheduled.to_sql(
        name="21N_queue_rescheduled",
        con='mysql+mysqldb://'+SUMMARY_DB_USER+':'+SUMMARY_DB_PASSWORD+'@'+SUMMARY_DB_HOST+':'+SUMMARY_DB_PORT+'/'+SUMMARY_DB+'?ssl_mode=DISABLED',
        schema=SUMMARY_DB,
        if_exists="append",
        index=False)

    print('----------------------------------Data written into summary DB----------------------')


def queue_state(*args, **context):
    print("------------------------------------Inside Function-----------------------------")

    df_queue_state = pd.read_sql_query(
        sql="SELECT * FROM 21N_queue_state where creationdate > NOW()-INTERVAL 1 DAY;",
        con='mysql+mysqldb://'+REPLICA_DB_USER+':'+REPLICA_DB_PASSWORD+'@'+REPLICA_DB_HOST+':'+REPLICA_DB_PORT+'/'+REPLICA_DB+'?ssl_mode=DISABLED')

    print("-------------------------------Data read into dataframe---------------------------")

    df_queue_state.to_sql(
        name="21N_queue_state",
        con='mysql+mysqldb://'+SUMMARY_DB_USER+':'+SUMMARY_DB_PASSWORD+'@'+SUMMARY_DB_HOST+':'+SUMMARY_DB_PORT+'/'+SUMMARY_DB+'?ssl_mode=DISABLED',
        schema=SUMMARY_DB,
        if_exists="append",
        index=False)

    print('----------------------------------Data written into summary DB----------------------')


def queue(*args, **context):
    print("------------------------------------Inside Function-----------------------------")

    df_queue = pd.read_sql_query(
        sql="SELECT * FROM 21N_queue where creationdate > NOW()-INTERVAL 1 DAY;",
        con='mysql+mysqldb://'+REPLICA_DB_USER+':'+REPLICA_DB_PASSWORD+'@'+REPLICA_DB_HOST+':'+REPLICA_DB_PORT+'/'+REPLICA_DB+'?ssl_mode=DISABLED')

    print("-------------------------------Data read into dataframe---------------------------")

    df_queue.to_sql(
        name="21N_queue",
        con='mysql+mysqldb://'+SUMMARY_DB_USER+':'+SUMMARY_DB_PASSWORD+'@'+SUMMARY_DB_HOST+':'+SUMMARY_DB_PORT+'/'+SUMMARY_DB+'?ssl_mode=DISABLED',
        schema=SUMMARY_DB,
        if_exists="append",
        index=False)

    print('----------------------------------Data written into summary DB----------------------')


def servicecentre_invoices(*args, **context):
    print("------------------------------------Inside Function-----------------------------")

    df_servicecentre_invoices = pd.read_sql_query(
        sql="SELECT * FROM 21N_servicecentre_invoices where creationdate > NOW()-INTERVAL 1 DAY;",
        con='mysql+mysqldb://'+REPLICA_DB_USER+':'+REPLICA_DB_PASSWORD+'@'+REPLICA_DB_HOST+':'+REPLICA_DB_PORT+'/'+REPLICA_DB+'?ssl_mode=DISABLED')

    print("-------------------------------Data read into dataframe---------------------------")

    df_servicecentre_invoices.to_sql(
        name="21N_servicecentre_invoices",
        con='mysql+mysqldb://'+SUMMARY_DB_USER+':'+SUMMARY_DB_PASSWORD+'@'+SUMMARY_DB_HOST+':'+SUMMARY_DB_PORT+'/'+SUMMARY_DB+'?ssl_mode=DISABLED',
        schema=SUMMARY_DB,
        if_exists="append",
        index=False)

    print('----------------------------------Data written into summary DB----------------------')


def servicecentre(*args, **context):
    print("------------------------------------Inside Function-----------------------------")

    df_servicecentre = pd.read_sql_query(
        sql="SELECT * FROM 21N_servicecentre where creationdate > NOW()-INTERVAL 1 DAY;",
        con='mysql+mysqldb://'+REPLICA_DB_USER+':'+REPLICA_DB_PASSWORD+'@'+REPLICA_DB_HOST+':'+REPLICA_DB_PORT+'/'+REPLICA_DB+'?ssl_mode=DISABLED')

    print("-------------------------------Data read into dataframe---------------------------")

    df_servicecentre.to_sql(
        name="21N_servicecentre",
        con='mysql+mysqldb://'+SUMMARY_DB_USER+':'+SUMMARY_DB_PASSWORD+'@'+SUMMARY_DB_HOST+':'+SUMMARY_DB_PORT+'/'+SUMMARY_DB+'?ssl_mode=DISABLED',
        schema=SUMMARY_DB,
        if_exists="append",
        index=False)

    print('----------------------------------Data written into summary DB----------------------')


def slot_tracker(*args, **context):
    print("------------------------------------Inside Function-----------------------------")

    df_slot_tracker = pd.read_sql_query(
        sql="SELECT * FROM 21N_slot_tracker where creationdatetime > NOW()-INTERVAL 1 DAY;",
        con='mysql+mysqldb://'+REPLICA_DB_USER+':'+REPLICA_DB_PASSWORD+'@'+REPLICA_DB_HOST+':'+REPLICA_DB_PORT+'/'+REPLICA_DB+'?ssl_mode=DISABLED')

    print("-------------------------------Data read into dataframe---------------------------")

    df_slot_tracker['slot_hour'] = (pd.Timestamp('now').normalize() + df_slot_tracker['slot_hour']).dt.time

    df_slot_tracker.to_sql(
        name="21N_slot_tracker",
        con='mysql+mysqldb://'+SUMMARY_DB_USER+':'+SUMMARY_DB_PASSWORD+'@'+SUMMARY_DB_HOST+':'+SUMMARY_DB_PORT+'/'+SUMMARY_DB+'?ssl_mode=DISABLED',
        schema=SUMMARY_DB,
        if_exists="append",
        index=False,
        )

    print('----------------------------------Data written into summary DB----------------------')


def vola_list(*args, **context):
    print("------------------------------------Inside Function-----------------------------")

    df_vola_list = pd.read_sql_query(
        sql="SELECT * FROM 21N_vola_list where creationdatetime > NOW()-INTERVAL 1 DAY;",
        con='mysql+mysqldb://'+REPLICA_DB_USER+':'+REPLICA_DB_PASSWORD+'@'+REPLICA_DB_HOST+':'+REPLICA_DB_PORT+'/'+REPLICA_DB+'?ssl_mode=DISABLED')

    print("-------------------------------Data read into dataframe---------------------------")

    df_vola_list.to_sql(
        name="21N_vola_list",
        con='mysql+mysqldb://'+SUMMARY_DB_USER+':'+SUMMARY_DB_PASSWORD+'@'+SUMMARY_DB_HOST+':'+SUMMARY_DB_PORT+'/'+SUMMARY_DB+'?ssl_mode=DISABLED',
        schema=SUMMARY_DB,
        if_exists="append",
        index=False)

    print('----------------------------------Data written into summary DB----------------------')




default_args={
            "owner": "airflow",
            "retries": 3,
            "retry_delay": timedelta(minutes=5),
            "start_date": datetime(2021, 6, 23),
}

# Schedule Interval set to UTC time zone. TO be run everyday @ 11:45PM IST 
dag = DAG(
        dag_id='summary_db',
        schedule_interval='45 23 * * *',
        default_args= default_args,
        concurrency=4,
        max_active_runs = 2,
        catchup=False,)

    #Tasks under a Dag
ambassador_channel_partner_comission = PythonOperator(
    task_id="ambassador_channel_partner_comission",
    python_callable = ambassador_channel_partner_comission,
    provide_context = True,
    dag=dag,
)


ambassador_channel_partner = PythonOperator(
    task_id="ambassador_channel_partner",
    python_callable = ambassador_channel_partner,
    provide_context = True,
    dag=dag,
)

ambassador_channel_partner_tied = PythonOperator(
    task_id="ambassador_channel_partner_tied",
    python_callable = ambassador_channel_partner_tied,
    provide_context = True,
    dag=dag,
)

ambassador_info = PythonOperator(
    task_id="ambassador_info",
    python_callable = ambassador_info,
    provide_context = True,
    dag=dag,
)

ambassador_locationbk = PythonOperator(
    task_id="ambassador_locationbk",
    python_callable = ambassador_locationbk,
    provide_context = True,
    dag=dag,
)

ambassador_login = PythonOperator(
    task_id="ambassador_login",
    python_callable = ambassador_login,
    provide_context = True,
    dag=dag,
)

ambassador_mishaps = PythonOperator(
    task_id="ambassador_mishaps",
    python_callable = ambassador_mishaps,
    provide_context = True,
    dag=dag,
)

ambassador_ontime_history = PythonOperator(
    task_id="ambassador_ontime_history",
    python_callable = ambassador_ontime_history,
    provide_context = True,
    dag=dag,
)

ambassador_refunds_type = PythonOperator(
    task_id="ambassador_refunds_type",
    python_callable = ambassador_refunds_type,
    provide_context = True,
    dag=dag,
)

ambassador_refunds = PythonOperator(
    task_id="ambassador_refunds",
    python_callable = ambassador_refunds,
    provide_context = True,
    dag=dag,
)

customer_servicecentre_logins = PythonOperator(
    task_id="customer_servicecentre_logins",
    python_callable = customer_servicecentre_logins,
    provide_context = True,
    dag=dag,
)

dealer_master = PythonOperator(
    task_id="dealer_master",
    python_callable = dealer_master,
    provide_context = True,
    dag=dag,
)

queue_bills_due = PythonOperator(
    task_id="queue_bills_due",
    python_callable = queue_bills_due,
    provide_context = True,
    dag=dag,
)

queue_cash_collected_multiple_queueid = PythonOperator(
    task_id="queue_cash_collected_multiple_queueid",
    python_callable = queue_cash_collected_multiple_queueid,
    provide_context = True,
    dag=dag,
)

queue_completedby = PythonOperator(
    task_id="queue_completedby",
    python_callable = queue_completedby,
    provide_context = True,
    dag=dag,
)

queue_distance_eta = PythonOperator(
    task_id="queue_distance_eta",
    python_callable = queue_distance_eta,
    provide_context = True,
    dag=dag,
)

queue_is_trip_based = PythonOperator(
    task_id="queue_is_trip_based",
    python_callable = queue_is_trip_based,
    provide_context = True,
    dag=dag,
)

queue_prepaid_list = PythonOperator(
    task_id="queue_prepaid_list",
    python_callable = queue_prepaid_list,
    provide_context = True,
    dag=dag,
)

queue_repeat = PythonOperator(
    task_id="queue_repeat",
    python_callable = queue_repeat,
    provide_context = True,
    dag=dag,
)

queue_rescheduled = PythonOperator(
    task_id="queue_rescheduled",
    python_callable = queue_rescheduled,
    provide_context = True,
    dag=dag,
)

queue_state = PythonOperator(
    task_id="queue_state",
    python_callable = queue_state,
    provide_context = True,
    dag=dag,
)

queue = PythonOperator(
    task_id="queue",
    python_callable = queue,
    provide_context = True,
    dag=dag,
)

servicecentre_invoices = PythonOperator(
    task_id="servicecentre_invoices",
    python_callable = servicecentre_invoices,
    provide_context = True,
    dag=dag,
)

servicecentre = PythonOperator(
    task_id="servicecentre",
    python_callable = servicecentre,
    provide_context = True,
    dag=dag,
)

slot_tracker = PythonOperator(
    task_id="slot_tracker",
    python_callable = slot_tracker,
    provide_context = True,
    dag=dag,
)

vola_list = PythonOperator(
    task_id="vola_list",
    python_callable = vola_list,
    provide_context = True,
    dag=dag,
)











# '''
# #Summary DB Details

# SUMMARY_DB_HOST=asia-database-instance-1.cdiynpmpjjje.ap-southeast-1.rds.amazonaws.com
# SUMMARY_DB_USER=analytics
# SUMMARY_DB_PASSWORD=k9%^88*Lf5EDC7KP
# SUMMARY_DB_PORT = 3306
# SUMMARY_DB = analytics


# #21NORTH PRODUCTION REPLICA DETAILS

# REPLICA_DB_HOST = 3.0.136.197
# REPLICA_DB_USER = yantra
# REPLICA_DB_PASSWORD = 0FRZ*j7,>M>C,&tJ
# REPLICA_DB_PORT = 3306
# REPLICA_DB = 21north
# '''