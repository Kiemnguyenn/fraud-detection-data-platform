import sys
import os
import platform
from pyspark.sql import SparkSession

def is_running_in_docker():
    return platform.system() == 'Linux'

def get_spark_session(app_name):
    # 1. CẤU HÌNH WINDOWS HADOOP
    if not is_running_in_docker():
        os.environ['HADOOP_HOME'] = "C:\\hadoop"
        os.environ['PATH'] = "C:\\hadoop\\bin;" + os.environ.get('PATH', '')
        os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'
        os.environ['SPARK_MASTER_HOST'] = '127.0.0.1'
        python_path = sys.executable
        if "Program Files" in python_path:
            python_path = python_path.replace("Program Files", "Progra~1")
        os.environ['PYSPARK_PYTHON'] = python_path
        os.environ['PYSPARK_DRIVER_PYTHON'] = python_path

    # 2. KHỞI TẠO SPARK LÕI
    packages = [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.532",
        "io.delta:delta-spark_2.12:3.1.0",
        "com.clickhouse:clickhouse-jdbc:0.4.6"
    ]

    builder = (SparkSession.builder
             .appName(app_name)
             .config("spark.jars.packages", ",".join(packages))
             .config("spark.driver.memory", "4g")
             .config("spark.executor.memory", "4g")
             .config("spark.memory.offHeap.enabled", "true")
             .config("spark.memory.offHeap.size", "4g")
             .config("spark.sql.shuffle.partitions", "200")
             .config("spark.sql.autoBroadcastJoinThreshold", "-1")
             .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
             .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"))

    # 3. CẤU HÌNH MINIO (Docker vs Local)
    if is_running_in_docker():
        builder = (builder
                 .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
                 .config("spark.driver.bindAddress", "0.0.0.0"))
    else:
        builder = (builder
                 .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
                 .config("spark.driver.host", "127.0.0.1")
                 .config("spark.driver.bindAddress", "127.0.0.1"))

    spark = (builder
             .config("spark.hadoop.fs.s3a.access.key", "admin")
             .config("spark.hadoop.fs.s3a.secret.key", "Admin123")
             .config("spark.hadoop.fs.s3a.path.style.access", "true")
             .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
             .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
             .getOrCreate())
    return spark