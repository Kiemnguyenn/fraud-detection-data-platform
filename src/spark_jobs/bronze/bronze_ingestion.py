import sys
import os
from datetime import datetime

# --- SETUP PATH TO IMPORT UTILS ---
current_dir = os.path.dirname(os.path.abspath(__file__))
spark_jobs_dir = os.path.dirname(current_dir)
src_dir = os.path.dirname(spark_jobs_dir)
root_dir = os.path.dirname(src_dir)
sys.path.append(root_dir)

# --- [WINDOWS OS FIX] SET HADOOP HOME MANUALLY ---
# 1. Set HADOOP_HOME environment variable
os.environ['HADOOP_HOME'] = "C:\\hadoop"
# 2. Add the 'bin' directory to system PATH so Spark can load the native library
sys.path.append("C:\\hadoop\\bin")
if "C:\\hadoop\\bin" not in os.environ['PATH']:
    os.environ['PATH'] += ";C:\\hadoop\\bin"

from pyspark.sql.functions import col, current_timestamp, lit
from src.utils.spark_utils import get_spark_session

class BronzeIngestion:
    def __init__(self):
        # 1. Initialize Spark from utils
        self.spark = get_spark_session("Bronze Ingestion Job")

        # 2. Configure paths
        self.kafka_bootstrap = "localhost:9092"
        self.minio_path = "s3a://bronze/all_tables/"
        self.checkpoint_path = "s3a://bronze/checkpoints/"

        # 3. Get Job Start Time
        self.job_start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def get_raw_stream(self):
        """
        STEP 2: CONNECT (READ)
        Read raw data from Kafka
        """
        print("Connecting to Kafka...")
        return (self.spark.readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", self.kafka_bootstrap)
                .option("subscribePattern", "dbserver1.public.*")
                .option("startingOffsets", "earliest")
                .option("maxOffsetsPerTrigger", "1000")
                .load())

    def transform_data(self, df):
        """
        STEP 3: TRANSFORM & ADD METADATA COLUMNS
        """
        print("Transforming data and adding Audit columns...")
        return df.select(
            # 1. Keep original Kafka data (Important for debugging)
            col("key").cast("string").alias("kafka_key"),
            col("value").cast("string").alias("json_payload"),
            col("topic").alias("source_table"),
            col("partition"),
            col("offset"),

            # 2. Job Start Time column (Fixed for this batch)
            lit(self.job_start_time).cast("timestamp").alias("job_start_time"),

            # 3. Ingestion Time / End Time column
            # This is the actual time data passes through Spark
            current_timestamp().alias("ingestion_time"),

            # 4. Status column - Default is SUCCESS
            lit("SUCCESS").alias("status")
        )

    def write_to_minio(self, df):
        """
        STEP 4: WRITE
        Write to MinIO in Parquet format
        """
        print(f"Writing to MinIO at: {self.minio_path}")

        query = (df.writeStream
                 .format("parquet")
                 .outputMode("append")
                 .option("path", self.minio_path)
                 .option("checkpointLocation", self.checkpoint_path)
                 .partitionBy("source_table")
                 .trigger(processingTime="5 seconds")
                 .start())
        return query

    def run(self):
        print(f"Starting Bronze Ingestion Job at: {self.job_start_time}")

        # Call functions sequentially
        raw_df = self.get_raw_stream()
        clean_df = self.transform_data(raw_df)
        query = self.write_to_minio(clean_df)

        # Await termination to keep the job running
        query.awaitTermination()


# Run Job
if __name__ == "__main__":
    job = BronzeIngestion()
    job.run()