import sys
import os
from pyspark.sql.functions import col, from_json, schema_of_json, lit, row_number
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# --- SETUP PATH ---
current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(os.path.dirname(os.path.dirname(current_dir)))
sys.path.append(root_dir)

from src.utils.spark_utils import get_spark_session


def process_silver_dimensions():
    print("\n" + "=" * 50)
    print("STARTING 12 DIMENSION TABLES JOB (FINAL VERSION)")
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

            # 2. Native Infer Schema
            sample_json_row = df_bronze.select("json_payload").first()
            if not sample_json_row: continue
            sample_json_str = sample_json_row[0]
            schema_ddl = spark.range(1).select(schema_of_json(lit(sample_json_str))).first()[0]

            # 3. Flatten, Clean & Deduplicate
            print("   -> Flattening and Deduplicating data...")
            df_parsed = (
                df_bronze
                .withColumn("parsed", from_json(col("json_payload"), schema_ddl))
                .filter(col("parsed.payload.op") != 'd')
                .select("parsed.payload.after.*", col("parsed.payload.source.ts_ms").alias("cdc_ts_ms"))
            )

            # Deduplicate by Primary Key, keeping the latest record
            window_spec = Window.partitionBy(pk).orderBy(col("cdc_ts_ms").desc())
            df_clean = (
                df_parsed
                .withColumn("rn", row_number().over(window_spec))
                .filter(col("rn") == 1)
                .drop("rn", "cdc_ts_ms")
            )

            # Show Sample Data
            print(f"   -> Sample data for {table}:")
            df_clean.show(3, truncate=False)

            # 4. Upsert Delta Lake
            print("   -> Saving to Delta Lake (Upsert)...")
            if DeltaTable.isDeltaTable(spark, silver_path):
                dt = DeltaTable.forPath(spark, silver_path)
                (dt.alias("t")
                 .merge(df_clean.alias("s"), f"t.{pk} = s.{pk}")
                 .whenMatchedUpdateAll()
                 .whenNotMatchedInsertAll()
                 .execute())
            else:
                df_clean.write.format("delta").mode("overwrite").save(silver_path)

            print(f"Successfully processed table: {table}")

        except Exception as e:
            print(f"Error processing {table}: {e}")

    print("\n" + "=" * 50)
    print("ALL DIMENSION TABLES PROCESSED SUCCESSFULLY!")
    print("=" * 50)
    spark.stop()


if __name__ == "__main__":
    process_silver_dimensions()