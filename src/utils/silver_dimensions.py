import sys
import os
# THÊM schema_of_json và lit VÀO ĐÂY
from pyspark.sql.functions import col, from_json, schema_of_json, lit
from delta.tables import DeltaTable

# --- SETUP PATH ---
current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(os.path.dirname(os.path.dirname(current_dir)))
sys.path.append(root_dir)

from src.utils.spark_utils import get_spark_session


def process_silver_dimensions():
    print("\n" + "=" * 50)
    print("STARTING 12 DIMENSION TABLES JOB (NATIVE INFER SCHEMA)")
    print("=" * 50)

    spark = get_spark_session("Silver_Dimensions_Job")
    spark.sparkContext.setLogLevel("ERROR")

    # Configuration: Table Name -> Primary Key
    tables_config = {
        "identity": "TransactionID",
        "user_profile": "client_no",
        "location": "LocationID",
        "card_issuing_bank": "Bank_ID",
        "device_fingerprint": "Device_ID",
        "beneficiary_list": "Beneficiary_ID",
        "user_accounts": "account_number",
        "master_card": "Card_Key",
        "blacklist_rules": "Rule_ID",
        "product_description": "ProductCD",
        "status": "StatusID",
        "description_card": "Card_Network_ID"
    }

    for table, pk in tables_config.items():
        print(f"\nProcessing Table: {table.upper()} (PK: {pk})")

        bronze_path = f"s3a://bronze/all_tables/source_table=dbserver1.public.{table}/"
        silver_path = f"s3a://silver/{table}/"

        try:
            # 1. Read Bronze
            df_bronze = spark.read.parquet(bronze_path)
            if df_bronze.isEmpty():
                print(f"Table is empty, skipping.")
                continue

            # 2. TỰ ĐỘNG INFER SCHEMA BẰNG NATIVE SPARK SQL (KHÔNG DÙNG RDD/PYTHON WORKER NỮA)
            print("   -> Automatically inferring schema from JSON natively...")

            # Lấy 1 dòng JSON duy nhất ra làm mẫu
            sample_json_row = df_bronze.select("json_payload").first()
            if not sample_json_row:
                continue

            sample_json_str = sample_json_row[0]

            # Dùng hàm Native để tự động sinh ra cấu trúc DDL của JSON
            schema_ddl = spark.range(1).select(schema_of_json(lit(sample_json_str))).first()[0]

            # 3. Flatten & Clean
            df_clean = (
                df_bronze
                .withColumn("parsed", from_json(col("json_payload"), schema_ddl))
                .filter(col("parsed.payload.op") != 'd')  # Chui vào lớp payload
                .select("parsed.payload.after.*")  # Lấy toàn bộ data trong after
            )

            # Show sample
            print(f"   -> Sample data for {table}:")
            df_clean.show(3)

            # 4. Upsert Delta Lake
            if DeltaTable.isDeltaTable(spark, silver_path):
                print(f"   -> Upserting...")
                dt = DeltaTable.forPath(spark, silver_path)
                (dt.alias("t")
                 .merge(df_clean.alias("s"), f"t.{pk} = s.{pk}")
                 .whenMatchedUpdateAll()
                 .whenNotMatchedInsertAll()
                 .execute())
            else:
                print(f"   -> Creating new table...")
                df_clean.write.format("delta").mode("overwrite").save(silver_path)

            print(f"Finished table {table}.")

        except Exception as e:
            print(f"Error processing {table}: {e}")

    print("\nALL DIMENSION TABLES PROCESSED SUCCESSFULLY!")
    spark.stop()


if __name__ == "__main__":
    process_silver_dimensions()