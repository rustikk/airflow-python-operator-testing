from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator

from datetime import timedelta, datetime
from pprint import pprint


def kw_parameter_check(**kwargs):
    pprint(kwargs)
    return kwargs


def ar_parameter_check(*data):
    pprint(data)
    return data


with DAG(
    dag_id="python_operator_tests_mods",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=["Python_Operator_Test"],
) as dag:
    # this says it cant deserialize json
    # error code:
    # TypeError: Object of type AirflowConfigParser is not JSON serializable
    t1 = PythonOperator(
        task_id="parameter_test",
        python_callable=kw_parameter_check,
        op_kwargs={"kwarg1": "kwarg_one", "kwarg2": "kwarg_two"},
    )

    # this works
    t2 = PythonOperator(
        task_id="args_test",
        python_callable=ar_parameter_check,
        op_args=["Hello", "QA", "Team", "!"],
    )


t2 >> t1
