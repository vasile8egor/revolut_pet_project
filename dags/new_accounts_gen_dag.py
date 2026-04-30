from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id='revolut_generate_new_accounts_v2',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['generation', 'accounts']
) as dag:

    def generate_and_insert(**context):
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        from revolut_app.generators.new_accounts_gen import NewAccountGenerator
        
        pg_hook = PostgresHook(postgres_conn_id='postgres_main')
        target_date = context['dag_run'].logical_date.date()
        
        generator = NewAccountGenerator()
        n_new = generator.get_daily_count(target_date)
        
        if n_new == 0:
            dag.log.info(f"No new accounts to generate for {target_date}")
            return

        accounts = []
        transactions = []

        base_acc_fields = [
            'account_id', 'first_name', 'last_name', 'email', 'phone',
            'date_of_birth', 'currency', 'account_type', 'account_sub_type',
            'acquisition_channel', 'acquisition_channel_name',
            'initial_deposit', 'registration_datetime', 'churn_risk', 
            'lifetime_value'
        ]

        db_fields = base_acc_fields + [
            'churn_risk', 'lifetime_value', 
            'churn_risk_score', 'ltv_amount'
        ]

        tx_fields = [
            'transaction_id', 'account_id', 'booking_datetime', 'value_datetime',
            'amount', 'currency', 'credit_debit_indicator', 'status',
            'transaction_information', 'merchant_name'
        ]

        risk_map = {'low': 0.10, 'medium': 0.50, 'high': 0.90}
        ltv_map = {'low': 100.0, 'medium': 500.0, 'high': 1000.0}

        for _ in range(n_new):
            acc, tx = generator.generate_new_client(target_date)

            raw_risk = str(acc.get('churn_risk', 'low')).lower()
            raw_ltv = str(acc.get('lifetime_value', 'low')).lower()
            
            risk_score = risk_map.get(raw_risk, 0.0)
            ltv_amount = ltv_map.get(raw_ltv, 0.0)

            row = [acc[f] for f in base_acc_fields]
            row.extend([raw_risk, raw_ltv, risk_score, ltv_amount])
            accounts.append(row)
            transactions.append([tx[f] for f in tx_fields])
            
        pg_hook.insert_rows(
            table='silver.dim_accounts',
            rows=accounts,
            target_fields=db_fields,
            replace=True,
            replace_index=['account_id']
        )

        pg_hook.insert_rows(
            table='silver.fact_transactions',
            rows=transactions,
            target_fields=tx_fields,
            replace=True,
            replace_index=['transaction_id']
        )

    generate_task = PythonOperator(
        task_id='generate_new_accounts_task',
        python_callable=generate_and_insert
    )
