from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
import random


# dag = DAG(
#     dag_id='Choose_Best_Model_Sample',
#     schedule_interval='0 0 * * *',
#     dagrun_timeout=timedelta(minutes=60),
#     tags=['Best_Model']
# )

def training_model():
    return random.randint(0, 100)

def choose_best_model_ml(ti):
    model_accuracy = ti.xcom_pull(task_ids=[
        "Model_A",
        "Model_B",
        "Model_C"
    ])
    if max(model_accuracy) > 80 :
        return "accurate_model"
    return "inaccurate_model"

with DAG(
    dag_id="Choose_Best_Model_Sample",
    schedule_interval="@daily",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    # end_date=,
    # full_filepath=,
    # template_searchpath=,
    # template_undefined=,
    # user_defined_macros=,
    # user_defined_filters=,
    # default_args=,
    # concurrency=,
    # max_active_runs=
    ) as dag:
    
    # modelA = PythonOperator(
    #     task_id="Model_A",
    #     python_callable=training_model
    # )

    # modelB = PythonOperator(
    #     task_id="Model_B",
    #     python_callable=training_model
    # )

    # modelC = PythonOperator(
    #     task_id="Model_C",
    #     python_callable=training_model
    # )

       
    models = [PythonOperator(
        task_id=f"Model_{model}",
        python_callable=training_model
    ) for model in "ABC" ]

    best_model = BranchPythonOperator(
        task_id="choose_best_model",
        python_callable=choose_best_model_ml
    )

    accurate = BashOperator(
        task_id="accurate_model",
        bash_command="echo 'Model is Accurate!!!'"
    )

    inaccurate = BashOperator(
        task_id="inaccurate_model",
        bash_command="echo 'Model is Failed!!!'"
    )

    # [modelA, modelB, modelC] >> best_model  >> [accurate, inaccurate]

    models  >> best_model  >> [accurate, inaccurate]