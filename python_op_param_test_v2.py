from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from datetime import timedelta, datetime

from pprint import pprint

import sys

yesterday = datetime.now() - timedelta(days=1)


def kw_parameter_check(**kwargs):
    pprint(kwargs)


def ar_parameter_check(*data):
    pprint(data)


def templated_dict_check(templates_dict):
    # pprint(templates_dict)
    return templates_dict


with DAG(
    dag_id="python_operator_tests",
    start_date=yesterday,
    schedule_interval=None,
    catchup=False,
    template_searchpath="/home/t-dawg/Documents/astronomer_tut/templates",
    tags=["Python_Operator_Test"],
) as dag:

    t1 = PythonOperator(
        task_id="kwargs_test",
        python_callable=kw_parameter_check,
        op_kwargs={"kwarg1": "kwarg_one", "kwarg2": "kwarg_two"},
    )

    t2 = PythonOperator(
        task_id="args_test",
        python_callable=ar_parameter_check,
        op_args=["Hello", "QA", "Team", "!"],
    )

    # shows as both a xcom with a value of return value or
    # can be seen in the rendered Template tab next to XComs
    t3 = PythonOperator(
        task_id="template_dict_test",
        python_callable=templated_dict_check,
        # converts key values to str in xcoms
        # templates_dict={"Hello": "There", 12: 14, 14: "string"},
        templates_dict={
            "sql": "sqller.sql",
            "website_boilerplt": "texty.html",
            "python": "pypy.py",
        },
        # templates_exts=[".sql", ".html", ".py"],
    )

t1 >> t2 >> t3
