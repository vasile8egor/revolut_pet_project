import random
import uuid
from datetime import datetime, timedelta

import numpy as np
from faker import Faker
from airflow import DAG
from airflow.operators.python import PythonOperator
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from constants.constants import (
    LOCATIONS,
    CUSTOMER_CATEGORIES,
    CURRENCIES,
    ACCOUNT_TYPES,
    INITIAL_MERCHANTS,
    DOMAINS,
    MIN_AGE_CLIENT,
    MAX_AGE_CLIENT,
)


fake = Faker(random.choice(LOCATIONS))

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='generate_new_accounts',
    default_args=default_args,
    description='Generate synthetic new customers with Faker names',
    schedule_interval='0 4 * * *',
    catchup=False,
    max_active_runs=1,
)


def get_currency():
    currencies = list(CURRENCIES.keys())
    weights = list(CURRENCIES.values())

    if sum(weights) != 1:
        sum_weight = sum(weights)
        norm_weights = [weight / sum_weight for weight in weights]
        return random.choices(currencies, weights=norm_weights)[0]
    return random.choices(currencies, weights)[0]


def get_account_type():
    account_type = list(ACCOUNT_TYPES.keys())
    weights = list(ACCOUNT_TYPES.values())

    if sum(weights) != 1:
        sum_weight = sum(weights)
        norm_weights = [weight / sum_weight for weight in weights]
        return random.choices(account_type, weights=norm_weights)[0]
    return random.choices(account_type, weights)[0]


class NewAccountGenerator:
    """Generations accounts with Faker"""

    def __init__(self):
        self.categories = CUSTOMER_CATEGORIES

    def get_daily_new_accounts(self, target_date):
        """Quantity of new accounts per day"""
        is_weekend = target_date.weekday() >= 5
        base_lambda = 5 if is_weekend else 10
        return np.random.poisson(base_lambda)

    def get_acquisition_channel(self):
        channels = list(self.categories.keys())
        probs = [self.categories[ch]['prob'] for ch in channels]
        return random.choices(channels, weights=probs)[0]

    def generate_account_id(self):
        return str(uuid.uuid4()).replace('-', '')

    def generate_name(self):
        return {
            'first_name': fake.first_name(),
            'last_name': fake.last_name()
        }

    def generate_email(self, first_name, last_name):
        return (
            f'{first_name.lower()}'
            f'.{last_name.lower()}'
            f'@{random.choice(DOMAINS)}'
        )

    def generate_phone(self):
        return f"44{random.randint(100000000, 999999999)}"

    def generate_initial_transaction(
            self, account_id,
            first_name, last_name,
            amount, channel,
            registration_time,
    ):

        merchant = random.choice(INITIAL_MERCHANTS)
        tx_delay = random.randint(1, 60)
        tx_time = registration_time + timedelta(minutes=tx_delay)

        return {
            'transaction_id': (
                f'{account_id}'
                f'_{registration_time.strftime('%Y%m%d')}'
                f'_000001'
            ),
            'account_id': account_id,
            'booking_datetime': tx_time,
            'value_datetime': tx_time,
            'amount': float(amount),
            'currency': 'GBP',
            'credit_debit_indicator': 'Credit',
            'status': 'Completed',
            'transaction_information': (
                f"{merchant} - New account registration "
                f"via {CUSTOMER_CATEGORIES[channel]['name']}"
            ),
            'merchant_name': merchant,
            'load_ts': datetime.now()
        }

    def generate_account(self, target_date, channel):
        """Генерирует один новый аккаунт с Faker данными"""
        category = self.categories[channel]

        hour = random.randint(0, 23)
        minute = random.randint(0, 59)
        second = random.randint(0, 59)
        registration_time = datetime(
            target_date.year, target_date.month, target_date.day,
            hour, minute, second
        )

        name = self.generate_name()
        first_name = name['first_name']
        last_name = name['last_name']

        currency = get_currency()
        account_type = get_account_type()
        account_sub_type = (
            'CurrentAccount' if account_type == 'Personal' else account_type
        )

        avg_deposit = category['avg_initial_deposit']
        initial_deposit = round(
            np.random.lognormal(mean=np.log(avg_deposit), sigma=0.5), 2
        )

        email = self.generate_email(first_name, last_name)
        phone = self.generate_phone()
        date_of_birth = fake.date_of_birth(
            minimum_age=MIN_AGE_CLIENT,
            maximum_age=MAX_AGE_CLIENT
        )

        return {
            'account_id': self.generate_account_id(),
            'first_name': first_name,
            'last_name': last_name,
            'email': email,
            'phone': phone,
            'date_of_birth': date_of_birth,
            'currency': currency,
            'account_type': account_type,
            'account_sub_type': account_sub_type,
            'acquisition_channel': channel,
            'acquisition_channel_name': category['name'],
            'initial_deposit': initial_deposit,
            'registration_datetime': registration_time,
            'inserted_at': datetime.now(),
            'churn_risk': category['churn_risk'],
            'lifetime_value': category['lifetime_value']
        }


def generate_new_accounts(**context):

    execution_date = context['execution_date']
    target_date = execution_date.date()

    generator = NewAccountGenerator()

    n_new = generator.get_daily_new_accounts(target_date)
    print(f"New accounts today: {n_new}")

    if n_new == 0:
        print("There are no clients today.")
        return 0

    accounts = []
    all_initial_transactions = []

    for _ in range(n_new):
        channel = generator.get_acquisition_channel()
        account = generator.generate_account(target_date, channel)
        accounts.append(account)

        tx = generator.generate_initial_transaction(
            account_id=account['account_id'],
            first_name=account['first_name'],
            last_name=account['last_name'],
            amount=account['initial_deposit'],
            channel=channel,
            registration_time=account['registration_datetime']
        )
        all_initial_transactions.append(tx)

    spark = SparkSession.builder \
        .appName(f"NewAccounts_{target_date}") \
        .master("local[*]") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.4") \
        .getOrCreate()

    schema_accounts = StructType([
        StructField("account_id", StringType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("date_of_birth", DateType(), True),
        StructField("currency", StringType(), True),
        StructField("account_type", StringType(), True),
        StructField("account_sub_type", StringType(), True),
        StructField("acquisition_channel", StringType(), True),
        StructField("acquisition_channel_name", StringType(), True),
        StructField("initial_deposit", DoubleType(), True),
        StructField("registration_datetime", TimestampType(), True),
        StructField("inserted_at", TimestampType(), True),
        StructField("churn_risk", StringType(), True),
        StructField("lifetime_value", StringType(), True),
    ])

    schema_transactions = StructType([
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

    df_accounts = spark.createDataFrame(accounts, schema=schema_accounts)

    df_accounts.write \
        .mode("append") \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres_dwh:5432/postgres") \
        .option("dbtable", "silver.dim_accounts") \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .option("driver", "org.postgresql.Driver") \
        .save()

    print(
        f"\n Successfull add {len(accounts)} new accounts "
        "into silver.dim_accounts"
    )

    df_transactions = spark.createDataFrame(
        all_initial_transactions, schema=schema_transactions
    )

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
        f"Successfull add {len(all_initial_transactions)} start transactions"
    )

    spark.stop()
    return len(accounts)


generate_task = PythonOperator(
    task_id='generate_new_accounts',
    python_callable=generate_new_accounts,
    dag=dag,
)

generate_task
