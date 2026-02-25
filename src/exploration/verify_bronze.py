import sys
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(os.path.dirname(current_dir))
sys.path.append(root_dir)

os.environ['HADOOP_HOME'] = "C:\\hadoop"
if "C:\\hadoop\\bin" not in os.environ['PATH']:
    os.environ['PATH'] += ";C:\\hadoop\\bin"

from src.utils.spark_utils import get_spark_session

spark = get_spark_session("Verify Bronze Data")
spark.sparkContext.setLogLevel("ERROR")

tables = [
    "transaction", "user_accounts", "user_profile", "status",
    "beneficiary_list", "blacklist_rules", "card_issuing_bank",
    "description_card", "device_fingerprint", "identity",
    "location", "master_card", "product_description"
]

print("==================================================")
print("🔍 STARTING BRONZE LAYER DATA VERIFICATION IN MINIO")
print("==================================================")

for table in tables:
    try:
        path = f"s3a://bronze/all_tables/source_table=dbserver1.public.{table}/"
        df = spark.read.parquet(path)
        count = df.count()
        print(f"✅ Table [{table}]: {count} rows")

        if table == "transaction":
            print("\n--- 🛠️ Transaction Table Schema Structure ---")
            df.printSchema()
            print("\n--- 📄 Sample Data (First 3 rows - JSON truncated for readability) ---")
            df.selectExpr(
                "kafka_key",
                "substring(json_payload, 1, 100) as json_preview",
                "job_start_time"
            ).show(3, truncate=False)
            print("-" * 50)

    except Exception as e:
        print(f"❌ Table [{table}]: No data found or incorrect path.")

print("==================================================")