from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, udf, upper
import uuid
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)

# Initialize Spark session
spark = SparkSession.builder.appName("ADLS_Skip_Processed_Files").getOrCreate()

# ADLS account details (move secrets to Azure Key Vault or Databricks Secrets in production)
adls_account_name = "augauto"
adls_container_name = "raw"
key = "HPYHx8Hxr3w4ZTiWoPepySyjRaybQqI1iUoPbhsjtWDsUrnWjs8hHF8djdQfk9sjuBjfEB+OU1L/+AStSoaDSg=="

# Set Spark configs for ADLS Gen2
spark.conf.set(f"fs.azure.account.auth.type.{adls_account_name}.dfs.core.windows.net", "SharedKey")

spark.conf.set(f"fs.azure.account.key.{adls_account_name}.dfs.core.windows.net", key)

# File paths
adls_raw_path = f"abfss://{adls_container_name}@{adls_account_name}.dfs.core.windows.net/"
processed_files_metadata_path = f"{adls_raw_path}processed_files/processed_files.txt"
processed_files_dir = f"{adls_raw_path}processed_files/"

# Function to check if the processed_files.txt file exists

def file_exists(file_path):
    try:
        files_in_dir = dbutils.fs.ls(processed_files_dir)
        return any(file.name == "processed_files.txt" for file in files_in_dir)
    except Exception:
        return False


# Function to append file name to processed_files.txt
def append_to_processed_files(file_name):
    # Check if the file exists
    if file_exists(processed_files_metadata_path):
        # Read existing content
        existing_content = dbutils.fs.head(processed_files_metadata_path)
    else:
        existing_content = ""

    # Append new file name
    updated_content = existing_content + file_name + "\n"

    # Overwrite the file with updated content
    try:
        dbutils.fs.put(processed_files_metadata_path, updated_content, overwrite=True)
    except Exception as e:
        print(f"Error appending to processed files: {e}")


# Get already processed file names
processed_files = []
if file_exists(processed_files_metadata_path):
    processed_files = dbutils.fs.head(processed_files_metadata_path).splitlines()
else:
    logging.info("No processed files metadata found. Starting fresh.")
    print('No precessed file metadata found . starting fresh ')

# List files in raw path
all_files = dbutils.fs.ls(adls_raw_path)
raw_file_names = [file.name for file in all_files if file.name.endswith(".csv")]

# Identify unprocessed files
unprocessed_files = [file for file in raw_file_names if file not in processed_files]

# Define UUID UDF
uuid_udf = udf(lambda: str(uuid.uuid4()))

# Process each unprocessed file
for file_name in unprocessed_files:
    file_path = adls_raw_path + file_name
    logging.info(f"Processing: {file_name}")
    print(f"Processing: {file_name}")

    # Read CSV
    df = spark.read.csv(file_path, header=True, inferSchema=True)
    df = df.limit(10)

    # Optional schema validation
    expected_columns = ["LocationID", "Borough"]  # Add more as needed
    if not set(expected_columns).issubset(df.columns):
        logging.warning(f"Skipping file due to schema mismatch: {file_name}")
        continue

    # Add metadata columns
    df = df.withColumn("create_user", lit("SYS_USER")) \
           .withColumn("create_date", current_timestamp().cast("timestamp")) \
           .withColumn("record_id", uuid_udf())

    # Snowflake connection (move to secrets in prod)
    url = (
        "jdbc:snowflake://UKQZPWD-YN23455.snowflakecomputing.com/?user=soma"
        "&password=Somapujari@1995&warehouse=COMPUTE_WH&db=TAXI_ZONE&schema=QA"
    )

    # Write to RAW layer
    df.write.mode("append") \
        .format("jdbc") \
        .option("driver", "net.snowflake.client.jdbc.SnowflakeDriver") \
        .option("url", url) \
        .option("dbtable", "TAXI_ZONE.QA.TRIP_LOOKUP_RAW") \
        .save()

    # Write to BRONZE layer after filtering
    bronze_df = df.filter(col("LocationID") == 100).withColumn("Borough", upper(col("Borough")))
    bronze_df.write.mode("append") \
        .format("jdbc") \
        .option("driver", "net.snowflake.client.jdbc.SnowflakeDriver") \
        .option("url", url) \
        .option("dbtable", "TAXI_ZONE.QA.TRIP_LOOKUP_BRONZE") \
        .save()

    # Mark file as processed
    append_to_processed_files(file_name)
    logging.info(f"File marked as processed: {file_name}")
    print(f"File marked as processed: {file_name}")

logging.info("✅ All unprocessed files have been handled.")
print("✅ All unprocessed files have been handled.")