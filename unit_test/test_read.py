from src.utils.spark_utils import get_spark_session

spark = get_spark_session("Debug_Bronze")
df = spark.read.parquet("s3a://bronze/all_tables/source_table=dbserver1.public.transaction/")

# Chỉ in ra 1 dòng JSON duy nhất, không cắt xén chữ
df.select("json_payload").show(1, truncate=False)