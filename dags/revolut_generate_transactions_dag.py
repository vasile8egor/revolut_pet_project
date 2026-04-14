import random
from datetime import datetime, timedelta

import numpy as np
from airflow import DAG
from airflow.operators.python import PythonOperator
from faker import Faker
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from constants.constants import (
    ACTIVITY_DISTRIBUTION,
    ITERATIONS_QUANTITY,
    LOCATIONS
)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='metropolis_transaction_generator',
    default_args=default_args,
    description='Generate transactions with Metropolis Algorithm',
    schedule_interval='0 3 * * *',
    catchup=False,
    max_active_runs=1,
)


def get_random_company():
    random_locate = random.choice(LOCATIONS)
    fake = Faker(random_locate)
    return fake.company()


class MetropolisTransactionGenerator:
    """Simple transactions generation with Markov Chain Monte Carlo (MCMC)"""

    def __init__(self, temperature=0.3, base_lambda=50):
        self.temperature = temperature
        self.base_lambda = base_lambda

        self.target_intensity = np.array(ACTIVITY_DISTRIBUTION)

        self.target_intensity = (
            self.target_intensity / self.target_intensity.sum()
        )
        self.current_intensity = self.target_intensity.copy()

    def run_mcmc(self, iterations=ITERATIONS_QUANTITY):
        """One time check MCMC"""

        accepted = 0

        for _ in range(iterations):
            proposed = self.current_intensity + np.random.normal(0, 0.04, 24)
            proposed = np.clip(proposed, 0.01, 1.0)
            proposed /= proposed.sum()

            current_energy = np.sum(
                (self.current_intensity - self.target_intensity) ** 2
            )
            proposed_energy = np.sum(
                (proposed - self.target_intensity) ** 2
            )

            if (
                proposed_energy < current_energy or
                random.random() < np.exp(
                    -(proposed_energy - current_energy) / self.temperature
                )
            ):
                self.current_intensity = proposed
                accepted += 1

        print(f"Acceptance probability: {accepted/iterations}")
        return self.current_intensity

    def generate_transactions(self, account_ids, target_date):
        """Generate transactions for all accounts"""
        all_transactions = []

        for account_id in account_ids:
            daily_n = max(
                10, int(np.random.poisson(self.base_lambda))
            )
            hours = np.random.choice(
                24, size=daily_n, p=self.current_intensity
            )

            for i, hour in enumerate(hours):
                minute = random.randint(0, 59)
                second = random.randint(0, 59)

                tx_time = datetime(
                    target_date.year,
                    target_date.month,
                    target_date.day,
                    hour,
                    minute,
                    second
                )

                amount = round(
                    np.random.lognormal(mean=np.log(55), sigma=0.85), 2
                )

                tx_type = random.choices(
                    ['Debit', 'Credit'], weights=[0.73, 0.27]
                )[0]

                merchant = get_random_company() if tx_type == 'Debit' else None

                all_transactions.append({
                    'transaction_id': (
                        f"{account_id}"
                        f"_{target_date.strftime('%Y%m%d')}"
                        f"_{i+1:06d}"
                    ),
                    'account_id': account_id,
                    'booking_datetime': tx_time,
                    'value_datetime': tx_time,
                    'amount': amount,
                    'currency': 'GBP',
                    'credit_debit_indicator': tx_type,
                    'status': 'Completed',
                    'transaction_information': (
                        f"Synthetic {tx_type} transaction"
                    ),
                    'merchant_name': merchant,
                    'load_ts': datetime.now()
                })

        return all_transactions


def generate_transactions(**context):
    """Основная функция генерации через PySpark"""
    execution_date = context['execution_date']
    target_date = execution_date.date()

    print(f"Main function generation synthetic transaction on {target_date}")

    spark = SparkSession.builder \
        .appName(f"Metropolis_Generator_{target_date}") \
        .master("local[*]") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.4") \
        .getOrCreate()

    df_accounts = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres_dwh:5432/postgres") \
        .option("dbtable", "silver.dim_accounts") \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .option("driver", "org.postgresql.Driver") \
        .load()

    account_ids = [row.account_id for row in df_accounts.collect()]

    if not account_ids:
        print("Нет аккаунтов в silver.dim_accounts")
        spark.stop()
        return 0

    generator = MetropolisTransactionGenerator(
        temperature=0.35, base_lambda=65
    )
    generator.run_mcmc(iterations=ITERATIONS_QUANTITY)

    transactions = generator.generate_transactions(account_ids, target_date)
    print(f"Successful generate {len(transactions)} transaction")

    if not transactions:
        spark.stop()
        return 0

    schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("account_id", StringType(), True),
        StructField("booking_datetime", TimestampType(), True),
        StructField("value_datetime", TimestampType(), True),
        StructField("amount", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("credit_debit_indicator", StringType(), True),
        StructField("status", StringType(), True),
        StructField("transaction_information", StringType(), True),
        StructField("merchant_name", StringType(), True),
        StructField("load_ts", TimestampType(), True),
    ])

    df_transactions = spark.createDataFrame(transactions, schema=schema)

    df_transactions.write \
        .mode("append") \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres_dwh:5432/postgres") \
        .option("dbtable", "silver.fact_transactions") \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .option("driver", "org.postgresql.Driver") \
        .save()

    print(
        f"Successfull load {len(transactions)} transactions"
    )

    spark.stop()
    return len(transactions)


generate_task = PythonOperator(
    task_id='generate_synthetic_transactions',
    python_callable=generate_transactions,
    dag=dag,
)

generate_task
