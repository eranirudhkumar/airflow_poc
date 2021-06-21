from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {'owner': 'airflow',
                'start_date': datetime(2021, 1, 1)
                }

dag = DAG('dag_file_dynamic_test',
            schedule_interval='@daily',
            default_args=default_args,
            catchup=False)

with dag:
    t1 = BashOperator(
        task_id='bash_task',
        bash_command="echo 'Task Completed Successfully!'")