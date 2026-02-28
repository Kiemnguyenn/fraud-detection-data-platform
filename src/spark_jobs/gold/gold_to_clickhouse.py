import sys
import os
from pyspark.sql.functions import col

current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(os.path.dirname(os.path.dirname(current_dir)))
sys.path.append(root_dir)

from src.utils.spark_utils import get_spark_session


def process_gold_and_serve():
    print("\n" + "=" * 60)
    print("STARTING GOLD LAYER: PIPELINE TO CLICKHOUSE DWH")
    print("=" * 60)

    spark = get_spark_session("Gold_To_ClickHouse")
    spark.sparkContext.setLogLevel("ERROR")

    # 1. LOAD SILVER TABLES INTO MEMORY
    print("\n[1/4] Loading Silver tables into memory...")
    tables = [
        "transaction", "identity", "user_profile", "user_accounts",
        "master_card", "card_issuing_bank", "description_card",
        "product_description", "device_fingerprint", "location"
    ]

    for tbl in tables:
        try:
            spark.read.format("delta").load(f"s3a://silver/{tbl}").createOrReplaceTempView(f"silver_{tbl}")
            print(f"[OK] Created TempView: silver_{tbl}")
        except Exception as e:
            print(f"[WARNING] Could not load {tbl}")

    # 2. "BULLETPROOF" JOIN (Giữ nguyên logic cực mượt của hôm qua)
    print("\n[2/4] Executing Master JOIN (Heavy Operation)...")
    gold_sql = """
        WITH Deduplicated_Master_Card AS (
            SELECT 
                Card_Raw_ID,
                MAX(Client_No) as client_no,
                MAX(Bank_ID) as Bank_ID,
                MAX(Card_Network_ID) as Card_Network_ID
            FROM silver_master_card
            GROUP BY Card_Raw_ID
        )
        SELECT 
            t.TransactionID,
            CAST('2026-01-01 00:00:00' AS TIMESTAMP) + INTERVAL 1 SECOND * t.TransactionDT AS Transaction_Time,
            CAST(t.TransactionAmt AS DOUBLE) as TransactionAmt,
            CAST(t.isFraud AS INT) as isFraud,
            CAST(t.z_score AS DOUBLE) as z_score,
            COALESCE(pd.ProductDesc, 'Unknown') AS ProductDesc,

            COALESCE(up.client_no, 'Unknown') AS client_no,
            COALESCE(up.full_name, 'Unknown') AS full_name,
            COALESCE(up.email_address, 'Unknown') AS email_address,
            COALESCE(up.phone_number, 'Unknown') AS phone_number,
            COALESCE(up.customer_segment, 'Unknown') AS customer_segment,
            COALESCE(up.kyc_level, 0) AS kyc_level,

            COALESCE(ua.account_number, 'Unknown') AS account_number,
            COALESCE(CAST(ua.current_balance AS STRING), '0') AS current_balance,
            COALESCE(ua.account_status, 'Unknown') AS account_status,
            'Unknown' AS Beneficiary_Name,

            COALESCE(cb.Bank_Name, 'Unknown') AS Bank_Name,
            COALESCE(dc.Network_Name, 'Unknown') AS Network_Name,
            COALESCE(dc.Card_Type, 'Unknown') AS Card_Type,

            CAST(t.addr1 AS DOUBLE) as addr1,
            CAST(t.addr2 AS DOUBLE) as addr2,
            COALESCE(l.Region, 'Unknown') AS Region,
            COALESCE(l.Country, 'Unknown') AS Country,

            COALESCE(i.DeviceType, 'Unknown') AS DeviceType,
            COALESCE(dfp.Device_Name, 'Unknown') AS Device_Name,
            COALESCE(dfp.OS_Version, 'Unknown') AS OS_Version,
            COALESCE(dfp.Browser_Version, 'Unknown') AS Browser_Version,
            COALESCE(dfp.Screen_Resolution, 'Unknown') AS Screen_Resolution

        FROM silver_transaction t

        LEFT JOIN silver_product_description pd ON t.ProductCD = pd.ProductCD
        LEFT JOIN Deduplicated_Master_Card mc ON t.card1 = mc.Card_Raw_ID
        LEFT JOIN silver_card_issuing_bank cb ON mc.Bank_ID = cb.Bank_ID
        LEFT JOIN silver_description_card dc ON mc.Card_Network_ID = dc.Card_Network_ID
        LEFT JOIN silver_user_profile up ON mc.client_no = up.client_no
        LEFT JOIN silver_user_accounts ua ON up.client_no = ua.client_no AND ua.is_primary = true
        LEFT JOIN silver_identity i ON t.TransactionID = i.TransactionID
        LEFT JOIN silver_device_fingerprint dfp ON i.DeviceInfo = dfp.Device_Name
        LEFT JOIN (SELECT DISTINCT addr1, Region, Country FROM silver_location) l ON t.addr1 = l.addr1
    """

    gold_df = spark.sql(gold_sql)

    # Khử trùng lặp
    gold_df = gold_df.dropDuplicates(["TransactionID"])

    # 3. TRẠM TRUNG CHUYỂN: LƯU VÀO MINIO ĐỂ GIẢI PHÓNG RAM
    print("\n[3/4] Materializing data to MinIO (Checkpointing to save RAM)...")
    gold_path = "s3a://gold/transaction_master_temp"
    try:
        optimized_df = gold_df.repartition(8)
        optimized_df.write.format("delta").mode("overwrite").save(gold_path)
        print("SUCCESS: Data materialized to MinIO!")
    except Exception as e:
        print(f"FAILED to materialize data: {e}")
        spark.stop()
        return

    # 4. ĐỌC TỪ MINIO LÊN VÀ ĐẨY VÀO CLICKHOUSE DWH
    print("\n[4/4] Pushing clean data to ClickHouse DWH...")
    clean_gold_df = spark.read.format("delta").load(gold_path)

    # --- IN RA SỐ DÒNG VÀ PREVIEW DATA THEO Ý SẾP ---
    print("\n--- DATA PREVIEW ---")
    total_rows = clean_gold_df.count()
    print(f"Total records to push: {total_rows:,} rows")

    clean_gold_df.select(
        "TransactionID", "Transaction_Time", "full_name",
        "TransactionAmt", "isFraud", "Bank_Name", "DeviceType"
    ).show(10, truncate=False)
    print("--------------------------\n")
    print("\nSUCCESS")

    # Cấu hình đẩy vào ClickHouse bằng JDBC
    # clickhouse_url = "jdbc:clickhouse://localhost:8123/fraud_db"
    #
    # try:
    #     clean_gold_df.write \
    #         .format("jdbc") \
    #         .option("url", clickhouse_url) \
    #         .option("dbtable", "gold_transaction_master") \
    #         .option("user", "default") \
    #         .option("password", "admin") \
    #         .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
    #         .option("batchsize", "10000") \
    #         .option("isolationLevel", "NONE") \
    #         .mode("append") \
    #         .save()
    #
    #     print("\nBIG SUCCESS: DATA IS NOW SAFELY STORED IN CLICKHOUSE DWH!")
    # except Exception as e:
    #     print(f"\nFAILED to push to ClickHouse: {e}")

    spark.stop()


if __name__ == "__main__":
    process_gold_and_serve()