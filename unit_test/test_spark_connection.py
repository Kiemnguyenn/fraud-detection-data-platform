import sys
import os
from pyspark.sql import SparkSession

current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(current_dir)
sys.path.append(root_dir)

from src.utils.spark_utils import get_spark_session

def test_connection():
    print("Initializing Spark from tests folder...")

    # Call function from src
    spark = get_spark_session("Test Connection Job")

    print("INITIALIZATION SUCCESSFUL!")
    print(f"(+) Spark Version: {spark.version}")
    print(f"(+) App Name: {spark.sparkContext.appName}")

    # Test S3/MinIO libraries (Crucial Check)
    try:
        # Hadoop Configuration must have s3a implementation
        hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
        fs_impl = hadoop_conf.get("fs.s3a.impl")
        print(f"S3A Implementation: {fs_impl}")

        if fs_impl:
            print("Hadoop-AWS libraries loaded successfully!")
        else:
            print("Warning: S3A Implementation not found!")

    except Exception as e:
        print(f"S3 Check Error: {e}")

    print("Stopping Spark Session...")
    spark.stop()

if __name__ == "__main__":
    test_connection()