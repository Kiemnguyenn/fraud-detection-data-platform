import sys
import os

# --- THÊM 2 DÒNG NÀY ĐỂ ÉP SPARK DÙNG ĐÚNG MẠNG LOCAL ---
os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'
os.environ['SPARK_MASTER_HOST'] = '127.0.0.1'

from pyspark.sql import SparkSession

def get_spark_session(app_name):
    os.environ['HADOOP_HOME'] = "C:\\hadoop"
    os.environ['PATH'] = "C:\\hadoop\\bin;" + os.environ.get('PATH', '')

    python_path = sys.executable
    if "Program Files" in python_path:
        python_path = python_path.replace("Program Files", "Progra~1")

    os.environ['PYSPARK_PYTHON'] = python_path
    os.environ['PYSPARK_DRIVER_PYTHON'] = python_path

    packages = [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.532",
        "io.delta:delta-spark_2.12:3.1.0",
        "com.clickhouse:clickhouse-jdbc:0.4.6"
    ]

    spark = (SparkSession.builder
             .appName(app_name)
             .config("spark.jars.packages", ",".join(packages))

             # --- CHẾ ĐỘ MỞ VAN RAM TỐI ĐA (FIX TRIỆT ĐỂ OOM LOCAL) ---
             .config("spark.driver.memory", "8g")  # Tăng vọt lên 8GB nếu máy có 32GB
             .config("spark.executor.memory", "8g")

             # Chìa khóa vàng: Tắt hoàn toàn giới hạn bộ nhớ off-heap, cho Spark dùng thẳng RAM thật của OS
             .config("spark.memory.offHeap.enabled", "true")
             .config("spark.memory.offHeap.size", "8g")

             # Băm nát dữ liệu ra 200 mảnh nhỏ để CPU gặm từ từ (Mặc định là 200, nãy ta ép xuống 50 bị ngộp)
             .config("spark.sql.shuffle.partitions", "200")

             # Cấm tuyệt đối Spark lưu bảng nhỏ vào RAM
             .config("spark.sql.autoBroadcastJoinThreshold", "-1")

             # ... (Các cấu hình MinIO và DeltaLake giữ nguyên) ...
             .config("spark.driver.host", "127.0.0.1")
             .config("spark.driver.bindAddress", "127.0.0.1")
             .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
             .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
             .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
             .config("spark.hadoop.fs.s3a.access.key", "admin")
             .config("spark.hadoop.fs.s3a.secret.key", "Admin123")
             .config("spark.hadoop.fs.s3a.path.style.access", "true")
             .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
             .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
             .config("spark.hadoop.fs.s3a.connection.maximum", "100")
             .config("spark.hadoop.fs.s3a.attempts.maximum", "10")
             .config("spark.hadoop.fs.s3a.connection.timeout", "600000")
             .getOrCreate()
             )

    return spark