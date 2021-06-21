from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

default_args = {'owner': 'airflow',
                'start_date': datetime(2021, 1, 1)
                }

dag = DAG('dag_file_dynamic_test_2',
            schedule_interval='@daily',
            default_args=default_args,
            catchup=False)

def python_func():
    return "Processing..."


with dag:
    pass
    start = BashOperator(
        task_id='start_bash_operator',
        bash_command="echo 'Started succesfully.'")


    process = PythonOperator(
        task_id='process_python_operator',
        python_callable=python_func)



    end = BashOperator(
        task_id='end_bash_operator',
        bash_command="echo 'Completed succesfully.'")


start >> process >> end