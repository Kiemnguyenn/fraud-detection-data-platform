import sys
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(os.path.dirname(os.path.dirname(current_dir)))
sys.path.append(root_dir)

from src.utils.spark_utils import get_spark_session


def push_to_parquet_only():
    print("\n" + "=" * 60)
    print("STEP 1: SPARK DUMPS DATA TO PARQUET ON MINIO")
    print("=" * 60)

    spark = get_spark_session("Gold_To_Parquet")
    spark.sparkContext.setLogLevel("ERROR")

    # 1. Read data from the intermediate Delta table
    gold_path_delta = "s3a://gold/transaction_master_temp"
    df = spark.read.format("delta").load(gold_path_delta)

    # 2. Write directly to a new directory in PARQUET format
    gold_path_parquet = "s3a://gold/transaction_master_parquet"
    print("\nWriting Parquet format to MinIO...")

    try:
        df.write.format("parquet").mode("overwrite").save(gold_path_parquet)
        print(f"SUCCESS: Exported {df.count():,} rows to Parquet in MinIO!")
    except Exception as e:
        print(f"FAILED: {e}")

    spark.stop()


if __name__ == "__main__":
    push_to_parquet_only()