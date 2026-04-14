import io
import json
from datetime import datetime, timedelta

from minio import Minio

from airflow import DAG
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator

from utils.revolut_clients import RevolutClient
from constants.constants import (
    BUCKET_RAW,
)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='revolut_extract',
    default_args=default_args,
    description='Extract Revolut data to MinIO (with proper consent)',
    schedule_interval=None,
    catchup=False,
)


def get_minio_client():
    """Get MinIO client from connection"""
    connection = BaseHook.get_connection('minio_default')
    return Minio(
        connection.host,
        access_key=connection.login,
        secret_key=connection.password,
        secure=False
    )


def extract_accounts(**context):
    """Извлекает счета и сохраняет в MinIO"""
    refresh_token = Variable.get("REVOLUT_REFRESH_TOKEN")
    if not refresh_token:
        raise ValueError("REVOLUT_REFRESH_TOKEN not found in Variables")

    client = RevolutClient()
    client.refresh_token = refresh_token

    accounts = client.get_accounts()

    minio_client = get_minio_client()
    date_str = datetime.now().strftime('%Y-%m-%d_%H-%M')
    object_name = f"accounts/{date_str}/accounts.json"

    data_bytes = json.dumps(
        accounts, indent=2, ensure_ascii=False
    ).encode('utf-8')
    data_stream = io.BytesIO(data_bytes)

    minio_client.put_object(
        bucket_name=BUCKET_RAW,
        object_name=object_name,
        data=data_stream,
        length=len(data_bytes),
        content_type='application/json'
    )

    context['task_instance'].xcom_push(key='extract_date', value=date_str)

    account_list = accounts.get("Data", {}).get("Account", [])
    context['task_instance'].xcom_push(key='account_list', value=account_list)

    return len(account_list)


def extract_transactions(**context):
    """Извлекает транзакции для всех счетов"""

    refresh_token = Variable.get("REVOLUT_REFRESH_TOKEN")
    if not refresh_token:
        raise ValueError("REVOLUT_REFRESH_TOKEN not found in Variables")

    client = RevolutClient()
    client.refresh_token = refresh_token

    accounts = accounts = context[
        'task_instance'
    ].xcom_pull(key='account_list', task_ids='extract_accounts')

    if not accounts:
        accounts_data = client.get_accounts()
        accounts = accounts_data.get("Data", {}).get("Account", [])

    if not accounts:
        print("No accounts found")
        return 0

    minio_client = get_minio_client()
    date_str = datetime.now().strftime("%Y-%m-%d_%H-%M")

    saved_count = 0
    for account in accounts:
        account_id = account.get("AccountId")

        try:
            transactions = client.get_transactions(account_id)

            object_name = f"transactions/{date_str}/{account_id}.json"
            data_bytes = json.dumps(
                transactions, indent=2, ensure_ascii=False
            ).encode('utf-8')
            data_stream = io.BytesIO(data_bytes)

            minio_client.put_object(
                bucket_name=BUCKET_RAW,
                object_name=object_name,
                data=data_stream,
                length=len(data_bytes),
                content_type='application/json'
            )

            saved_count += 1

        except Exception as e:
            print(f"Failed for {account_id}: {e}")
            continue

    print(f"Successfull saved transactions for {saved_count} accounts")
    return saved_count


extract_accounts_task = PythonOperator(
    task_id='extract_accounts',
    python_callable=extract_accounts,
    dag=dag,
)

extract_transactions_task = PythonOperator(
    task_id='extract_transactions',
    python_callable=extract_transactions,
    dag=dag,
)

extract_accounts_task >> extract_transactions_task
