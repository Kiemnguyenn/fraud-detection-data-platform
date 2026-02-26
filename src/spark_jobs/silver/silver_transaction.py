import sys
import os
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DoubleType
from pyspark.sql.functions import col, from_json, when, avg, stddev, row_number, expr
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# --- SETUP PATH ---
current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(os.path.dirname(os.path.dirname(current_dir)))
sys.path.append(root_dir)

from src.utils.spark_utils import get_spark_session


def process_silver_transaction():
    print("\n" + "=" * 50)
    print("STARTING SILVER TRANSACTION JOB (ULTIMATE BASE64 FIX)")
    print("=" * 50)

    spark = get_spark_session("Silver_Transaction_Job")
    spark.sparkContext.setLogLevel("ERROR")

    # ====================================================
    # STEP 1: EXTRACT
    # ====================================================
    print("\n[STEP 1] Reading raw data from MinIO...")
    bronze_path = "s3a://bronze/all_tables/source_table=dbserver1.public.transaction/"

    try:
        df_bronze = spark.read.parquet(bronze_path)
        print(f"   -> Total raw rows: {df_bronze.count()}")
    except Exception as e:
        print(f"Error reading Bronze: {e}")
        return

    # ====================================================
    # STEP 2: FLATTEN & NATIVE DECODE BASE64 (TRY-CATCH BY EXPR)
    # ====================================================
    print("\n[STEP 2] Flattening JSON & Decoding Base64 (Safe Mode)...")

    after_fields = [
        StructField("TransactionID", IntegerType(), True),
        StructField("isFraud", IntegerType(), True),
        StructField("TransactionDT", LongType(), True),
        StructField("TransactionAmt", StringType(), True),  # Hứng String
        StructField("ProductCD", StringType(), True),
        StructField("card1", IntegerType(), True),
        StructField("card2", DoubleType(), True),
        StructField("card3", DoubleType(), True),
        StructField("card4", StringType(), True),
        StructField("card5", DoubleType(), True),
        StructField("card6", StringType(), True),
        StructField("addr1", DoubleType(), True),
        StructField("addr2", DoubleType(), True),
        StructField("dist1", DoubleType(), True),
        StructField("dist2", DoubleType(), True),
        StructField("P_emaildomain", StringType(), True),
        StructField("R_emaildomain", StringType(), True)
    ]
    after_fields.extend([StructField(f"C{i}", DoubleType(), True) for i in range(1, 15)])
    after_fields.extend([StructField(f"D{i}", DoubleType(), True) for i in range(1, 16)])
    after_fields.extend([StructField(f"M{i}", StringType(), True) for i in range(1, 10)])
    after_fields.extend([StructField(f"V{i}", DoubleType(), True) for i in range(1, 340)])

    payload_schema = StructType([
        StructField("payload", StructType([
            StructField("after", StructType(after_fields), True),
            StructField("op", StringType(), True),
            StructField("source", StructType([StructField("ts_ms", LongType(), True)]), True)
        ]), True)
    ])

    # TÁCH JSON
    df_parsed = (
        df_bronze
        .withColumn("parsed", from_json(col("json_payload"), payload_schema))
        .filter(col("parsed.payload.op") != 'd')
        .select(
            "parsed.payload.after.*",
            col("parsed.payload.source.ts_ms").alias("cdc_ts_ms")
        )
    )

    # GIẢI MÃ BẤT TỬ BẰNG SQL EXPRESSION (BỌC TRY-CATCH TRONG SQL)
    # Lệnh 'try_cast' hoặc 'try_add' của Spark SQL không áp dụng cho unbase64.
    # Do đó, ta sẽ dùng câu lệnh CASE WHEN để kiểm tra độ dài chuỗi trước.
    # Chỉ giải mã những chuỗi nào có độ dài > 0 và chia hết cho 4. Nếu bị hỏng, trả về Null.
    decode_expr = """
        CASE 
            WHEN TransactionAmt IS NULL OR length(TransactionAmt) = 0 THEN 0.0
            WHEN length(TransactionAmt) % 4 != 0 THEN 0.0
            ELSE cast(conv(hex(unbase64(TransactionAmt)), 16, 10) as double) / 1000.0
        END
    """

    df_flattened = df_parsed.withColumn("TransactionAmt", expr(decode_expr))

    print("   -> Sample data after native decoding:")
    df_flattened.select("TransactionID", "TransactionAmt", "isFraud", "cdc_ts_ms").show(5)

    # ====================================================
    # STEP 3: CLEAN & DEDUPLICATE
    # ====================================================
    print("\n[STEP 3] Cleaning & Deduplicating...")

    # Fill Null bằng 0.0 để nếu cái expr bên trên trả về Null, nó sẽ biến thành 0
    df_filled = df_flattened.fillna({"TransactionAmt": 0.0, "card1": -1})

    window_dedup = Window.partitionBy("TransactionID").orderBy(col("cdc_ts_ms").desc())

    df_dedup = (
        df_filled
        .withColumn("rn", row_number().over(window_dedup))
        .filter(col("rn") == 1)
        .drop("rn")
    )

    count_flat = df_flattened.count()
    count_dedup = df_dedup.count()
    print(f"   -> Before dedup: {count_flat} | After dedup: {count_dedup}")
    print(f"   -> Removed: {count_flat - count_dedup} duplicated rows.")

    # ====================================================
    # STEP 4: Z-SCORE
    # ====================================================
    print("\n[STEP 4] Calculating Z-Score...")

    window_spec = Window.partitionBy("card1")

    df_featured = (
        df_dedup
        .withColumn("avg_amt", avg("TransactionAmt").over(window_spec))
        .withColumn("std_amt", stddev("TransactionAmt").over(window_spec))
        .withColumn("z_score",
                    when((col("std_amt").isNull()) | (col("std_amt") == 0), 0)
                    .otherwise((col("TransactionAmt") - col("avg_amt")) / col("std_amt")))
        .drop("cdc_ts_ms")
    )

    print("   -> Sample Z-Score Results:")
    df_featured.select("TransactionID", "TransactionAmt", "avg_amt", "z_score").show(5)

    # ====================================================
    # STEP 5: LOAD TO DELTA LAKE
    # ====================================================
    silver_path = "s3a://silver/transaction/"
    print(f"\n[STEP 5] Writing to Delta Lake: {silver_path}")

    if DeltaTable.isDeltaTable(spark, silver_path):
        print("   -> Table exists. Executing MERGE...")
        delta_tbl = DeltaTable.forPath(spark, silver_path)
        (
            delta_tbl.alias("t")
            .merge(df_featured.alias("s"), "t.TransactionID = s.TransactionID")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        print("   -> Creating new table...")
        df_featured.write.format("delta").mode("overwrite").save(silver_path)

    print("\nTRANSACTION PROCESSING COMPLETED SUCCESSFULLY!")
    spark.stop()


if __name__ == "__main__":
    process_silver_transaction()