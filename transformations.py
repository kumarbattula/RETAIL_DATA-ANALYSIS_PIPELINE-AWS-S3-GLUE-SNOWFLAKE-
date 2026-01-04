import sys
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql.functions import col, expr
from awsglue.job import Job

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr

# Spark session (Glue creates automatically, but this works locally too)
spark = SparkSession.builder.appName("RetailETL").getOrCreate()

# ------------ READ RAW DATA FROM YOUR BUCKET ------------

transactions_df = spark.read.option("header", True).csv(
    "s3://bks-retail-data-lake/raw/transactions/"
)

products_df = spark.read.option("header", True).csv(
    "s3://bks-retail-data-lake/raw/products/"
)

customers_df = spark.read.option("header", True).csv(
    "s3://bks-retail-data-lake/raw/customers/"
)

# ------------ CLEAN & ADD TOTAL AMOUNT ------------

transactions_df = (
    transactions_df
        .withColumn("quantity", col("quantity").cast("int"))
        .withColumn("price", col("price").cast("double"))
        .withColumn("total_amount", expr("quantity * price"))
)

# ------------ JOIN TABLES ------------

fact_sales_df = (
    transactions_df
        .join(customers_df, "customer_id", "left")
        .join(products_df, "product_id", "left")
)

# ------------ SELECT FINAL COLUMNS ------------

fact_sales_df = fact_sales_df.select(
    "transaction_id",
    "customer_id",
    "customer_name",
    "city",
    "product_id",
    "product_name",
    "category",
    "transaction_date",
    "quantity",
    "price",
    "total_amount"
)

# ------------ WRITE CURATED DATA TO S3 (PARQUET) ------------

fact_sales_df.write \
    .mode("overwrite") \
    .partitionBy("transaction_date") \
    .parquet("s3://bks-retail-data-lake/curated/fact_sales/")

job.commit()
