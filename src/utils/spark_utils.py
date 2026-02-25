from pyspark.sql import SparkSession


def get_spark_session(app_name):
    packages = [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.532",
        "io.delta:delta-spark_2.12:3.1.0"
    ]

    spark = (SparkSession.builder
             .appName(app_name)
             .config("spark.jars.packages", ",".join(packages))

             # --- QUẢN LÝ RAM & CPU ---
             .config("spark.driver.memory", "4g")
             .config("spark.executor.memory", "4g")
             .config("spark.sql.parquet.enableVectorizedReader", "false")

             # --- FIX LỖI MẠNG LOCALHOST (WINDOWS) ---
             .config("spark.driver.host", "127.0.0.1")
             .config("spark.driver.bindAddress", "127.0.0.1")

             # --- CẤU HÌNH DELTA LAKE ---
             .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
             .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

             # --- CẤU HÌNH MINIO (BỌC THÉP CHỐNG ĐỨT KẾT NỐI) ---
             .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
             .config("spark.hadoop.fs.s3a.access.key", "admin")
             .config("spark.hadoop.fs.s3a.secret.key", "Admin123")
             .config("spark.hadoop.fs.s3a.path.style.access", "true")
             .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
             .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")

             # 3 dòng thần thánh giúp MinIO không bị ngộp
             .config("spark.hadoop.fs.s3a.connection.maximum", "100")
             .config("spark.hadoop.fs.s3a.attempts.maximum", "10")
             .config("spark.hadoop.fs.s3a.connection.timeout", "60000")

             .getOrCreate()
             )

    return spark