import io
import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
}

def upload_via_copy(df, table_name, pg_hook):
    """Исправленный метод загрузки через COPY"""
    if df.empty:
        return
    
    buffer = io.StringIO()
    # Записываем CSV без заголовков
    df.to_csv(buffer, index=False, header=False, sep='\t')
    buffer.seek(0)
    
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    try:
        # Устанавливаем путь поиска, чтобы Postgres точно видел схему silver
        cursor.execute("SET search_path TO silver, public;")
        
        # Если имя таблицы пришло как 'silver.dim_accounts', берем только вторую часть
        only_table_name = table_name.split('.')[-1]
        
        cursor.copy_from(
            buffer, 
            only_table_name, 
            sep='\t', 
            columns=list(df.columns)
        )
        conn.commit()
        print(f"Successfully loaded {len(df)} rows into {table_name}")
    except Exception as e:
        conn.rollback()
        print(f"Error loading into {table_name}: {e}")
        raise e
    finally:
        cursor.close()
        conn.close()

def bootstrap_history(**context):
    from revolut_app.generators.new_accounts_gen import NewAccountGenerator
    from revolut_app.generators.transactions_gen import MetropolisTransactionGenerator
    from dateutil.relativedelta import relativedelta
    import pandas as pd
    
    pg_hook = PostgresHook(postgres_conn_id='postgres_main')

    # Очищаем таблицы перед заливкой
    pg_hook.run("TRUNCATE silver.fact_transactions, silver.dim_accounts CASCADE;", autocommit=True)

    acc_gen = NewAccountGenerator()
    tx_gen = MetropolisTransactionGenerator()

    current_date = (datetime.now() - relativedelta(months=6)).date()
    end_date = datetime.now().date()
    
    local_account_ids = []
    # --- Единые имена буферов ---
    acc_buffer = []
    tx_buffer = []

    print(f"Starting bootstrap from {current_date}...")

    while current_date <= end_date:
        # 1. Генерация аккаунтов
        n_new = acc_gen.get_daily_count(current_date)
        for _ in range(n_new):
            acc, _ = acc_gen.generate_new_client(current_date)
            local_account_ids.append(acc['account_id'])
            
            risk = {'low': 0.1, 'medium': 0.4, 'high': 0.8}.get(str(acc.get('churn_risk')).lower(), 0.0)
            ltv = {'low': 100.0, 'medium': 500.0, 'high': 1000.0}.get(str(acc.get('lifetime_value')).lower(), 0.0)

            acc_buffer.append({
                'account_id': acc['account_id'], 'first_name': acc['first_name'],
                'last_name': acc['last_name'], 'email': acc['email'], 'phone': acc['phone'],
                'date_of_birth': acc['date_of_birth'], 'currency': acc['currency'],
                'account_type': acc['account_type'], 'account_sub_type': acc['account_sub_type'],
                'acquisition_channel': acc['acquisition_channel'],
                'acquisition_channel_name': acc['acquisition_channel_name'],
                'initial_deposit': acc['initial_deposit'],
                'registration_datetime': acc['registration_datetime'],
                'churn_risk': risk, 'lifetime_value': ltv
            })

        # 2. Генерация транзакций
        if local_account_ids:
            tx_gen.run_mcmc()
            # Берем хвост списка для скорости
            sample_ids = local_account_ids[-500:] 
            for acc_id in sample_ids:
                for tx in tx_gen.generate_for_account(acc_id, current_date):
                    tx_buffer.append({
                        'transaction_id': tx['transaction_id'],
                        'account_id': acc_id,
                        'merchant_name': tx.get('merchant_name', 'Unknown'),
                        'amount': tx['amount'],
                        'currency': tx['currency'],
                        'booking_datetime': tx['booking_datetime'],
                        'source': 'bootstrap'
                    })

        # 3. Промежуточный сброс раз в месяц (защита от OOM и Heartbeat timeout)
        if current_date.day == 1:
            if acc_buffer:
                upload_via_copy(pd.DataFrame(acc_buffer), 'silver.dim_accounts', pg_hook)
                acc_buffer = []
            if tx_buffer:
                upload_via_copy(pd.DataFrame(tx_buffer), 'silver.fact_transactions', pg_hook)
                tx_buffer = []
            print(f"Intermediate flush at {current_date} done.")

        current_date += timedelta(days=1)

    # --- 4. ФИНАЛЬНАЯ ЗАГРУЗКА ОСТАТКОВ (имена должны совпадать!) ---
    if acc_buffer:
        upload_via_copy(pd.DataFrame(acc_buffer), 'silver.dim_accounts', pg_hook)
    
    if tx_buffer:
        upload_via_copy(pd.DataFrame(tx_buffer), 'silver.fact_transactions', pg_hook)
    
    print("Bootstrap finished successfully!")

with DAG(
    dag_id='revolut_bootstrap_history_v3_optimized',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:
    PythonOperator(
        task_id='run_optimized_bootstrap', 
        python_callable=bootstrap_history
    )