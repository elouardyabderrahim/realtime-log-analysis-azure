# Databricks notebook source
pip install azure.eventhub

# COMMAND ----------

pip install azure-storage-blob azure-identity

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, split, col, to_timestamp, coalesce, unix_timestamp
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType
from azure.eventhub import EventHubConsumerClient
from pyspark.sql import functions as F
from pyspark.sql.functions import split, col, to_timestamp, coalesce, unix_timestamp, expr
from pyspark.sql.functions import split, col, to_timestamp
from pyspark.sql.functions import split, col, to_timestamp, from_utc_timestamp
from pyspark.sql.functions import lower
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import when
from pyspark.sql.functions import substring
from pyspark.sql.functions import regexp_extract, col
import hashlib
from pyspark.sql.functions import udf

# Create a SparkSession
spark = SparkSession.builder \
    .appName("movies-ratings-app") \
    .config("spark.jars.packages", "com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.22") \
    .getOrCreate()
namespace = "five-a"
accessKeyName = "RootManageSharedAccessKey"
accessKey = "pNZWBJw5nkNoKU6HyMQ+oUv4r47aOa6Uk+AEhIxkW5w="
eventHubName = "five-a"

connection_string = f"Endpoint=sb://{namespace}.servicebus.windows.net/;SharedAccessKeyName={accessKeyName};SharedAccessKey={accessKey};EntityPath={eventHubName}"

ehConf = {}

ehConf['eventhubs.connectionString'] = sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connection_string)
# ehConf["eventhubs.startingPosition"] = -1

df = spark \
  .readStream \
  .format("eventhubs") \
  .options(**ehConf) \
  .load()

selected = df.withColumn("body", F.col("body").cast("string"))

# Split the 'value' column into multiple columns

splited_df = selected.select(
    split(col("body"), "\\|")[0].alias("timestamp"),
    split(col("body"), "\\|")[1].alias("log_level"),
    split(col("body"), "\\|")[2].alias("request_id"),
    split(col("body"), "\\|")[3].alias("session_id"),
    split(col("body"), "\\|")[4].alias("user_id"),
    split(col("body"), "\\|")[5].alias("action"),
    split(col("body"), "\\|")[6].alias("http_method"),
    split(col("body"), "\\|")[7].alias("endpoint"),
    split(col("body"), "\\|")[8].alias("referrer"),
    split(col("body"), "\\|")[9].alias("ip_address"),
    split(col("body"), "\\|")[10].alias("user_agent"),
    split(col("body"), "\\|")[11].alias("response_time"),
    split(col("body"), "\\|")[12].alias("product_id"),
    split(col("body"), "\\|")[13].alias("cart_size"),
    split(col("body"), "\\|")[14].alias("checkout_status"),
    split(col("body"), "\\|")[15].alias("token"),
    split(col("body"), "\\|")[16].alias("auth_method"),
    split(col("body"), "\\|")[17].alias("auth_level"),
    split(col("body"), "\\|")[18].alias("correlation_id"),
    split(col("body"), "\\|")[19].alias("server_ip"),
    split(col("body"), "\\|")[20].alias("port"),
    split(col("body"), "\\|")[21].alias("protocol"),
    split(col("body"), "\\|")[22].alias("http_status"),
    split(col("body"), "\\|")[23].alias("detail")
)

# transformation
transformed_df = splited_df.withColumn("timestamp", col("timestamp").cast("timestamp"))
transformed_df = transformed_df.withColumn("log_level", lower(col("log_level")))
transformed_df = transformed_df.withColumn("action", lower(col("action")))
transformed_df = transformed_df.withColumn("http_method", lower(col("http_method")))
transformed_df = transformed_df.withColumn("endpoint", substring(col("endpoint"), 3, 1000))
transformed_df = transformed_df.withColumn("referrer", substring(col("referrer"), 12, 1000))
transformed_df = transformed_df.withColumn("ip_address", substring(col("ip_address"), 5, 1000))
transformed_df = transformed_df.withColumn("user_agent", substring(col("user_agent"), 8, 1000))
transformed_df = transformed_df.withColumn(
    "response_time",
    regexp_extract(col("response_time"), r'Response Time: ([0-9.]+)s', 1).cast("float")
)
transformed_df = transformed_df.withColumn("product_id", regexp_replace(col("product_id"), "Product ID: ", ""))
transformed_df = transformed_df.withColumn("cart_size", regexp_replace(col("cart_size"), "Cart Size: ", ""))
transformed_df = transformed_df.withColumn("checkout_status", regexp_replace(col("checkout_status"), "Checkout Status: ", ""))

@udf(StringType())
def encrypt_token(token):
    # Use SHA-256 hash function
    sha256 = hashlib.sha256()
    sha256.update(token.encode('utf-8'))
    encrypted_token = sha256.hexdigest()
    return encrypted_token

transformed_df = transformed_df.withColumn("token", encrypt_token(col("token")))
transformed_df = transformed_df.withColumn("server_ip", encrypt_token(col("server_ip")))

transformed_df = transformed_df.withColumn("auth_method", regexp_replace(col("auth_method"), "Auth Method: ", ""))
transformed_df = transformed_df.withColumn("auth_level", regexp_replace(col("auth_level"), "Auth Level: ", ""))
transformed_df = transformed_df.withColumn("correlation_id", regexp_replace(col("correlation_id"), "Correlation ID: ", ""))
transformed_df = transformed_df.withColumn("port", regexp_replace(col("port"), "Port: ", ""))
transformed_df = transformed_df.withColumn("protocol", regexp_replace(col("protocol"), "Protocol: ", ""))
transformed_df = transformed_df.withColumn("detail", regexp_replace(col("detail"), "Detail: ", ""))


import os

# Create output directories if they don't exist
output_dir_invalid = "/tmp/Invalid_Log.csv"
output_dir_valid = "/tmp/Valid_Log.csv"

if not os.path.exists(output_dir_invalid):
    os.makedirs(output_dir_invalid)

if not os.path.exists(output_dir_valid):
    os.makedirs(output_dir_valid)


# Create checkpoint directories if they don't exist
checkpoint_dir_alerts = "/tmp/alerts-checkpoint"
checkpoint_dir_logs = "/tmp/Logs-checkpoint"

if not os.path.exists(checkpoint_dir_alerts):
    os.makedirs(checkpoint_dir_alerts)

if not os.path.exists(checkpoint_dir_logs):
    os.makedirs(checkpoint_dir_logs)


# Filter data for specific alerts
alert_df = transformed_df.filter(
    (col("cart_size").between(100, 1000)) |
    (col("detail") == "500 Internal Server Error") |
    (col("response_time") > 6) |
    (col("action") == "unusual_action")
)

# Write alerts to CSV
alert_query = alert_df.writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", "/tmp/Invalid_Log.csv") \
    .option("checkpointLocation", "/tmp/alerts-checkpoint") \
    .start()


# Write RemainingValid Data to CSV
valid_query = transformed_df.writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", "/tmp/Valid_Log.csv") \
    .option("checkpointLocation", "/tmp/Logs-checkpoint") \
    .start()


# COMMAND ----------

# MAGIC %fs ls dbfs:/tmp/

# COMMAND ----------

# MAGIC %fs head dbfs:/tmp/Invalid_Log.csv/
