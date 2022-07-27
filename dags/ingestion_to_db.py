import datetime

from airflow.models import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago

# with DAG("db_ingestion", start_date=datetime.date(2022, 1, 1)) as dag:
#    start_workflow = DummyOperator(task_id="start_workflow")
#    validate = DummyOperator(task_id="validate")
#    prepare = DummyOperator(task_id="prepare")
#    load = DummyOperator(task_id="load")
#    end_workflow = DummyOperator(task_id="end_workflow")

#    start_workflow >> validate >> prepare >> load >> end_workflow


def ingest_data():
    hook = PostgresHook(postgres_conn_id="ml_conn")
    hook.insert_rows(
        table="monthly_charts_data",
        rows=[["Jan 200", 1, "The Weknd", "Out Of Time", 1, 2, 3, 4, 5, 6]],
    )


with DAG(
    "db_ingestion", start_date=days_ago(1), schedule_interval="@once"
) as dag:
    start_workflow = DummyOperator(task_id="start_workflow")
    validate = DummyOperator(task_id="validate")
    prepare = PostgresOperator(
        task_id="prepare",
        postgres_conn_id="ml_conn",
        sql="""
            CREATE TABLE IF NOT EXISTS monthly_charts_data (
                month VARCHAR(10) NOT NULL,
                position INTEGER NOT NULL,
                artist VARCHAR(100) NOT NULL,
                song VARCHAR(100) NOT NULL,
                indicative_revenue NUMERIC NOT NULL,
                us INTEGER,
                uk INTEGER,
                de INTEGER,
                fr INTEGER,
                ca INTEGER,
                au INTEGER
            )
        """,
    )
    load = PythonOperator(task_id="load", python_callable=ingest_data)
    end_workflow = DummyOperator(task_id="end_workflow")

    start_workflow >> validate >> prepare >> load >> end_workflow
