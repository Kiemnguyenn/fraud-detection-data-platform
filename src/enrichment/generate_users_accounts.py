import pandas as pd
from sqlalchemy import text
from faker import Faker
import random
import sys
import os
import logging

# --- PATH CONFIGURATION ---
# Add root directory to sys.path to import src.config
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from src.config import DB_URL
from src.utils import get_db_engine

# --- LOGGING SETUP ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

fake = Faker()
engine = get_db_engine()

# Global cache for account mapping
# Key: (addr1, email_domain) -> Value: client_no
user_mapping_cache = {}


def generate_user_profile(batch_size=2000):
    """
    Generates full User Profiles.
    Logic: Groups Transactions by addr1 + P_emaildomain => 1 Unique User (Entity Resolution).
    """
    logger.info("START: Generating User Profiles (Entity Resolution)...")

    # 1. Retrieve unique identifier pairs from Transaction
    # Only select rows with complete identification data
    query = """
    SELECT DISTINCT "addr1", "P_emaildomain" 
    FROM transaction 
    WHERE "addr1" IS NOT NULL AND "P_emaildomain" IS NOT NULL
    """
    try:
        df_users = pd.read_sql(query, engine)
    except Exception as e:
        logger.error(f"Error reading Transaction table: {e}")
        return

    total = len(df_users)
    logger.info(f"   -> Found {total} unique entities.")

    profiles = []

    for idx, row in df_users.iterrows():
        # Generate Standard CIF: 00000001
        client_no = str(idx + 1).zfill(8)

        # Safe Type Casting
        try:
            addr1_val = int(row['addr1'])  # Cast float 315.0 -> int 315
        except:
            addr1_val = 0

        email_val = str(row['P_emaildomain']) if row['P_emaildomain'] else "unknown.com"

        # Cache for next step
        user_mapping_cache[(addr1_val, email_val)] = client_no

        # Generate Mock Data & Truncate Strings to avoid DB Errors
        fake_name = fake.name()
        clean_name = fake_name.lower().replace(' ', '.')

        # CRITICAL FIX: Truncate phone number to max 20 chars
        raw_phone = fake.phone_number()
        safe_phone = raw_phone[:20]

        profiles.append({
            # 1. Identity
            "client_no": client_no,
            "full_name": fake_name[:100],  # Max 100 chars
            "tax_code": str(fake.random_number(digits=10, fix_len=True)),
            "identity_card_id": str(fake.random_number(digits=12, fix_len=True)),
            "identity_type": random.choice(['CCCD', 'Passport']),

            # 2. Traceability (Source Link)
            "raw_email_domain": email_val[:100],
            "raw_region_code": addr1_val,

            # 3. Contact
            "email_address": f"{clean_name}@{email_val}"[:100],
            "phone_number": safe_phone,  # Truncated
            "permanent_address": fake.address().replace('\n', ', ')[:255] if fake.address() else None,

            # 4. Classification
            "kyc_level": random.choice([1, 2, 3]),
            "customer_segment": random.choice(['Retail', 'Priority', 'Corporate']),
            "status": 'ACTIVE'
        })

        # Batch Insert
        if len(profiles) >= batch_size:
            try:
                pd.DataFrame(profiles).to_sql('user_profile', engine, if_exists='append', index=False, method='multi')
                logger.info(f"   -> Inserted {idx + 1}/{total} profiles.")
                profiles = []  # Reset list
            except Exception as e:
                logger.error(f"Batch Insert Error at index {idx}: {e}")

    # Insert remaining records
    if profiles:
        pd.DataFrame(profiles).to_sql('user_profile', engine, if_exists='append', index=False, method='multi')

    logger.info("COMPLETED STEP 1: User Profile.")


def generate_user_accounts(batch_size=2000):
    """
    Generates User Accounts linked to Profiles.
    Logic: User (client_no) + Card (card1) => Unique Account Number.
    """
    logger.info("START: Generating User Accounts (Account Mapping)...")

    if not user_mapping_cache:
        logger.warning("User cache is empty! Step 1 might not have run or failed.")
        return

    # 1. Retrieve card usage info for each User
    query = """
    SELECT DISTINCT "addr1", "P_emaildomain", "card1", "card6"
    FROM transaction 
    WHERE "addr1" IS NOT NULL AND "P_emaildomain" IS NOT NULL AND "card1" IS NOT NULL
    """
    df_accs = pd.read_sql(query, engine)
    total_accs = len(df_accs)
    logger.info(f"   -> Found {total_accs} cards requiring accounts.")

    accounts = []
    user_card_counter = {}  # Counter to generate sequence number (STT)

    count_inserted = 0
    for _, row in df_accs.iterrows():
        try:
            addr1_val = int(row['addr1'])
        except:
            addr1_val = 0
        email_val = str(row['P_emaildomain'])

        # Traceability: Lookup client_no from cache
        client_no = user_mapping_cache.get((addr1_val, email_val))

        if not client_no:
            continue  # User not found, skip

        # Generate Account Number: 190 + CIF + Sequence (01, 02...)
        cnt = user_card_counter.get(client_no, 0) + 1
        user_card_counter[client_no] = cnt
        stt = str(cnt).zfill(2)
        acc_number = f"190{client_no}{stt}"

        # Naming Logic
        card_type = str(row['card6']) if row['card6'] else 'debit'
        # Changed 'TK' (Tai Khoan) to 'ACCT' for English consistency
        acc_name = f"ACCT {card_type.title()} {stt}"

        accounts.append({
            "account_number": acc_number[:20],
            "client_no": client_no,
            "linked_raw_card_bin": int(row['card1']),
            "linked_card_type": card_type[:20],
            "account_name": acc_name[:100],
            "currency_code": 'USD',
            "current_balance": round(random.uniform(1000, 100000), 2),
            "daily_transfer_limit": 50000.00,
            "account_status": 'OPEN',
            "is_primary": (cnt == 1),
            "opened_date": fake.date_between(start_date='-3y', end_date='today')
        })

        if len(accounts) >= batch_size:
            pd.DataFrame(accounts).to_sql('user_accounts', engine, if_exists='append', index=False, method='multi')
            count_inserted += len(accounts)
            logger.info(f"   -> Inserted {count_inserted}/{total_accs} accounts.")
            accounts = []

    if accounts:
        pd.DataFrame(accounts).to_sql('user_accounts', engine, if_exists='append', index=False, method='multi')

    logger.info("✅ COMPLETED STEP 2: User Accounts.")


if __name__ == "__main__":
    try:
        generate_user_profile()
        generate_user_accounts()
        print("\n DATA GENERATION SUCCESSFUL!")
    except Exception as e:
        print(f"\n FATAL ERROR: {e}")
