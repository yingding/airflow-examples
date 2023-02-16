from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.decorators import task

from random import randint
from datetime import datetime

with DAG ("example_flow_dag", 
    start_date=datetime(2023, 2, 14),
    schedule="@daily",
    description="flow api pipeline example",
    tags=["Yingding", "Scivias"],
    catchup=False,
) as dag:
    @task
    def training_model(accuracy):
        return accuracy
 
    @task.branch
    def choose_best_model(accuracies):
        best_accuracy = max(accuracies)
        if (best_accuracy > 8):
            # use BranchPythonOperator to return the task_id for the successor task
            return 'accurate'
        else:
            return 'inaccurate'   


    accurate = BashOperator(
        task_id="accurate",
        bash_command="echo 'accurate'"
    )

    inaccurate = BashOperator(
        task_id="inaccurate",
        bash_command="echo 'inaccurate'"
    )
    '''Declare Tasks dependencies with bit shift operators'''
    choose_best_model(training_model.expand(accuracy=[5, 10, 6])) >> [accurate, inaccurate]





