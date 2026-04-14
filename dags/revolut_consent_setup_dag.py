from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from utils.revolut_clients import RevolutClient

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 0,
}

dag = DAG(
    dag_id="revolut_consent_setup",
    default_args=default_args,
    description="One-time Revolut Consent and Token Setup",
    schedule_interval=None,
    catchup=False,
)


def create_consent_and_print_url():
    client = RevolutClient()
    consent = client.create_consent()
    consent_id = consent["Data"]["ConsentId"]

    url = client.get_authorization_url(consent_id)
    print(url)


def exchange_code(**context):
    code = input("Enter authorization code: ").strip()
    client = RevolutClient()
    client.exchange_code(code)

    Variable.set("REVOLUT_REFRESH_TOKEN", client.refresh_token)


create_consent_task = PythonOperator(
    task_id="create_consent_and_url",
    python_callable=create_consent_and_print_url,
    dag=dag,
)

exchange_task = PythonOperator(
    task_id="exchange_code_for_tokens",
    python_callable=exchange_code,
    dag=dag,
)

create_consent_task >> exchange_task
