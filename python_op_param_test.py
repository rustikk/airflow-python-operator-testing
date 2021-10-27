from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from datetime import timedelta, datetime


def kw_parameter_check(**kwargs):
    kw = kwargs
    return kw


def ar_parameter_check(*data):
    ar = data
    return ar


default_args = {
    "owner": "airflow",
}

with DAG(
    dag_id="python_operator_tests",
    owner="airflow",
    start_date=datetime(2021, 10, 24),
    schedule_interval=timedelta(minutes=10),
    catchup=False,
    default_args=default_args,
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
