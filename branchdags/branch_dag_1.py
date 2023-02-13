from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator

from random import randint
from datetime import datetime

def _training_model() -> int:
    return randint(1, 10)

def _choose_best_model(ti):
    # pull the xcom db result from airflow db to get the output from previous tasks
    # which returns a list
    accuracies = ti.xcom_pull(task_ids=[
        'training_model_A',
        'training_model_B',
        'training_model_C',
    ])
    best_accuracy = max(accuracies)
    if (best_accuracy > 8):
        # use BranchPythonOperator to return the task_id for the successor task
        return 'accurate'
    else:
        return 'inaccurate'    

with DAG ("example_branch_dag", 
    start_date=datetime(2023, 2, 12), # schedule time + schedule_interval is the first schedule
    schedule_interval="@daily",
    catchup=False, # always use catchup to be False, otherwise all the missing schedule from start_date will be refilled
) as dag:
    '''Declare Tasks in DAG'''
    training_model_A = PythonOperator(
        task_id="training_model_A",
        python_callable=_training_model
    ) 

    training_model_B = PythonOperator(
        task_id="training_model_B",
        python_callable=_training_model
    )

    training_model_C = PythonOperator(
        task_id="training_model_C",
        python_callable=_training_model
    )

    choose_best_model = BranchPythonOperator(
        task_id="choose_best_model",
        python_callable=_choose_best_model
    )

    accurate = BashOperator(
        task_id="accurate",
        bash_command="echo 'accurate'"
    )

    inaccurate = BashOperator(
        task_id="inaccurate",
        bash_command="echo 'inaccurate'"
    )
    '''Declare Tasks dependencies with bit shift operators'''
    [training_model_A, training_model_B, training_model_C] >> choose_best_model >> [accurate, inaccurate]





