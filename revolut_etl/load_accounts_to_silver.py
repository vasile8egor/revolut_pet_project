from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, explode

MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"

ACCOUNTS_PATH = "s3a://raw/accounts/2026-04-02_15-10/accounts.json"

POSTGRES_URL = "jdbc:postgresql://localhost:5432/postgres"
POSTGRES_USER = "postgres"
POSTGRES_PASSWORD = "postgres"

JARS_PATH = "/home/vasile8/pet-project/revolut/revolut_pet_project/jars"
HADOOP_AWS_JAR = f"{JARS_PATH}/hadoop-aws-3.3.4.jar"
AWS_SDK_JAR = f"{JARS_PATH}/aws-java-sdk-bundle-1.12.262.jar"

spark = SparkSession.builder \
    .appName("Revolut_Accounts_to_Silver") \
    .master("local[*]") \
    .config("spark.jars", f"{HADOOP_AWS_JAR},{AWS_SDK_JAR}") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.4") \
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config(
        "spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
    ) \
    .getOrCreate()

print("Successfull Create SparkSession")

df_raw = spark.read.option("multiline", "true").json(ACCOUNTS_PATH)

df_accounts = df_raw \
    .select(explode(col("Data.Account")).alias("account")) \
    .select(
        col("account.AccountId").alias("account_id"),
        col("account.Currency").alias("currency"),
        col("account.AccountType").alias("account_type"),
        col("account.AccountSubType").alias("account_sub_type"),
        lit(datetime.now()).alias("inserted_at")
    ) \
    .dropDuplicates(["account_id"])

print(f"Downloaded from raw JSON: {df_raw.count()} str")

df_raw.show(5, truncate=False)

spark.sql("CREATE SCHEMA IF NOT EXISTS silver")

df_accounts.write \
    .mode("append") \
    .jdbc(
        url=POSTGRES_URL,
        table="silver.dim_accounts",
        properties={
            "user": POSTGRES_USER,
            "password": POSTGRES_PASSWORD,
            "driver": "org.postgresql.Driver"
        }
    )

print('Successfull load accounts into silver.dim_accounts')

spark.stop()
