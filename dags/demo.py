from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow import models, settings
from airflow.models import Connection
from datetime import datetime
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models import TaskInstance
import pandas as pd
import os
import random as r
import csv
import time 
list = []

def create_sql_file(**kwargs):
    output_file = kwargs['output_file']
    # Kiểm tra xem tệp đã tồn tại chưa
    if not os.path.exists(output_file):
        # Nếu không tồn tại, tạo tệp rỗng
        with open(output_file, 'w') as file:
            file.write("")

def get_data_wiki(**kwargs):
    input_file = kwargs['input_file']
    output_file = kwargs['output_file']
    store_path = kwargs['store']
    df = pd.read_csv(input_file)
    bien = r.randint(0, 10)
    time_error = 0
    list_success = []
    try:
        with open(store_path, 'r') as file:
            csv_reader = csv.reader(file)
            for row in csv_reader:
                list_success.append(row[0].strip()) 
    except:
        list_success = []
    with open(output_file, 'w') as file:
        for index, row in df.iterrows():
            kwargs['ti'].xcom_push(key='prepare', value='chuan bi them du lieu vao he thong')
            if (index == bien and row['id'] not in list_success) or len(list_success) == 0:
                # i+=1
                with open(store_path, 'a') as file_store:
                    file_store.write(f"{row['id']}\n")
                file.write( 
                    f"INSERT INTO employees VALUES ('{row['id']}','{row['name']}','{row['age']}','{row['city']}','{row['salary']}');\n"
                )
                kwargs['ti'].xcom_push(key=row['id'], value='chen du lieu thanh cong')
                break
            if (row['id'] in list_success):
                if time_error < 4:
                    time_error += 1
                    kwargs['ti'].xcom_push(key=row['id'], value='da ton tai bang ghi trong he thong')
                    time.sleep(180)
                    continue
                if time_error >= 4:
                    kwargs['ti'].xcom_push(key=row['id'], value='ket thuc sau 3 lan thu')  
                    break
            # else:



def read_sql_file(**kwargs):
    output_file = kwargs['output_file']
    with open(output_file, 'r') as file:
        return file.read()

def create_postgres_connection(**kwargs):
    conn_id = kwargs['conn_id']
    host = kwargs['host']
    schema = kwargs['schema']
    login = kwargs['login']
    password = kwargs['password']
    port = 5432
    
    session = settings.Session()

    # Kiểm tra xem kết nối đã tồn tại chưa
    if session.query(Connection).filter(Connection.conn_id == conn_id).first() is None:
        # Tạo kết nối PostgreSQL mới
        conn = Connection(
            conn_id=conn_id,
            conn_type='postgres',
            host=host,
            schema=schema,
            login=login,
            password=password,
            port=port
        )
        session.add(conn)
        session.commit()
        print(f"PostgreSQL connection '{conn_id}' created successfully.")
    else:
        print(f"PostgreSQL connection '{conn_id}' already exists.")

with DAG(dag_id='get_data_wiki', 
         start_date=datetime(2024, 3, 10),
         schedule_interval='@daily',   
         template_searchpath='/var/tmp/',
         ) as dag:
    
    add_postgres_conn_id = PythonOperator(
        task_id='add_postgres_conn_id',
        python_callable=create_postgres_connection,
        op_kwargs={
            'conn_id':'postgres_conn_id',
            'host':'host.docker.internal',
            'schema':'truong_itt',
            'login':'postgres',
            'password':'postgres'
        }
    )

    create_sql_file_task = PythonOperator(
        task_id='create_sql_file',
        python_callable=create_sql_file,
        op_kwargs={
            'output_file':'/var/tmp/data.sql'
        }
    )

    data_local = PythonOperator(
        task_id='data_local',
        python_callable=get_data_wiki,
        op_kwargs={
            'input_file':'/var/tmp/data-local/employees.csv',
            'output_file':'/var/tmp/data.sql',
            'store':'/var/tmp/storeid.csv'
        }
    )
    
    create_table_postgres = PostgresOperator(
        task_id='create_table_postgres',
        postgres_conn_id='postgres_conn_id',
        sql= 
            '''
            CREATE TABLE IF NOT EXISTS employees (
                id UUID,
                name VARCHAR(255),
                age INTEGER,
                city VARCHAR(255),
                salary INTEGER
            );
            '''
    )
    
    read_sql_file_task = PythonOperator(
        task_id='read_sql_file',
        python_callable=read_sql_file,
        op_kwargs={
            'output_file':'/var/tmp/data.sql'
        }
    )
    
    write_to_table_postgres = PostgresOperator(
        task_id = 'write_to_table_postgres',
        postgres_conn_id='postgres_conn_id',
        sql = '{{ task_instance.xcom_pull(task_ids="read_sql_file") }}',  
        trigger_rule='all_done'  
    )
add_postgres_conn_id
create_sql_file_task >> data_local
data_local >> create_table_postgres
create_sql_file_task >> create_table_postgres 
create_table_postgres >> read_sql_file_task >> write_to_table_postgres
