import sys
import os

# # Add 'src' directory to system path to locate modules
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from src.ingestion.load_data_to_postgres import run_ingestion

if __name__ == "__main__":
    print("==========================================")
    print("   FRAUD DETECTION DATA PLATFORM - ETL    ")
    print("==========================================")

    try:
        run_ingestion()
        print("\n ETL process completed successfully!")
        #print("\n Ingestion skipped (Already done).")
    except Exception as e:
        print(f"\n ETL process failed. Error: {e}")
