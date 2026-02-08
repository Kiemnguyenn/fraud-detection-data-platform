import pandas as pd
import logging
import os
from sqlalchemy import text
from src.config import DATA_RAW_DIR, FILES_CONFIG
from src.utils import get_db_engine

# --- SETUP LOGGING ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("ingestion.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def clean_column_names(df):
    """
    Normalize column names:
    1. Replace hyphens '-' with underscores '_' (e.g., fix id-01 -> id_01)
    2. Strip leading/trailing whitespace
    """
    df.columns = [c.replace('-', '_').strip() for c in df.columns]
    return df


def align_schema(df, table_name):
    """
    Synchronize schema structure:
    - If processing 'transaction' table and 'isFraud' column is missing (test file),
      automatically add 'isFraud' with None values (NULL in DB).
    """
    if table_name == 'transaction':
        if 'isFraud' not in df.columns:
            df['isFraud'] = None
    return df


def ingest_file_list(table_name, file_names, engine, chunk_size=10000):
    """
    Read CSV files, process columns, and load into Postgres.
    """
    total_success_table = 0
    total_fail_table = 0

    sorted_files = sorted(file_names, key=lambda x: 0 if 'train' in x else 1)

    logger.info(f"STARTING INGESTION FOR TABLE: {table_name.upper()}")
    logger.info(f"   Files processing order: {sorted_files}")

    for file_name in sorted_files:
        file_path = os.path.join(DATA_RAW_DIR, file_name)

        if not os.path.exists(file_path):
            logger.error(f"⚠️ File not found: {file_path}. Skipping.")
            continue

        logger.info(f"Processing file: {file_name}")

        try:
            with pd.read_csv(file_path, chunksize=chunk_size) as reader:
                for i, chunk in enumerate(reader):
                    try:
                        # --- STEP 1: DATA PROCESSING ---
                        # Normalize column names (id-01 -> id_01)
                        chunk = clean_column_names(chunk)

                        # Handle missing columns (add isFraud=NULL for test data)
                        chunk = align_schema(chunk, table_name)

                        # --- STEP 2: LOAD INTO DB ---
                        chunk.to_sql(
                            name=table_name,
                            con=engine,
                            if_exists='append',
                            index=False,
                            method='multi'
                        )

                        count = len(chunk)
                        total_success_table += count
                        logger.info(f"   -> Batch {i + 1}: Success ({count} rows).")

                    except Exception as e:
                        # Log lỗi gọn gàng hơn
                        error_msg = str(e).split('\n')[0]  # Chỉ lấy dòng lỗi đầu tiên
                        total_fail_table += len(chunk)
                        logger.error(f"   -> Batch {i + 1}: FAILED. Error: {error_msg}")

        except Exception as e:
            logger.error(f"Critical error reading file {file_name}: {e}")

    logger.info(
        f"COMPLETED {table_name}. Success: {total_success_table}, Failed: {total_fail_table} rows")
    return total_success_table


def verify_data(engine):
    """KVerify row counts in the database after ingestion"""
    logger.info("Verifying database records...")
    with engine.connect() as conn:
        tables = ['transaction', 'identity']
        for t in tables:
            try:
                # Check if table exists before counting
                check_tb = conn.execute(text(f"SELECT to_regclass('public.{t}')")).scalar()
                if check_tb:
                    count = conn.execute(text(f"SELECT COUNT(*) FROM {t}")).scalar()
                    logger.info(f"Table {t.upper()} contains: {count:,} rows.")
                else:
                    logger.warning(f"Table {t.upper()} does not exist.")
            except Exception as e:
                logger.error(f"Error verifying table {t}: {e}")


def run_ingestion():
    """Main execution function"""
    engine = get_db_engine()

    # 1. Load Transaction (Train -> Test)
    ingest_file_list('transaction', FILES_CONFIG['transaction'], engine)

    # 2. Load Identity (Train -> Test)
    ingest_file_list('identity', FILES_CONFIG['identity'], engine)

    # 3. Verify results
    verify_data(engine)
