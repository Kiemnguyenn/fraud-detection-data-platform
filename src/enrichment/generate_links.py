import pandas as pd
from sqlalchemy import text
from faker import Faker
import random
import sys
import os
import logging

# --- PATH CONFIGURATION ---
# Add root directory to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from src.config import DB_URL
from src.utils import get_db_engine

# --- LOGGING SETUP ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

fake = Faker()
engine = get_db_engine()


def generate_master_card():
    """
    Populate Master Card table (Linking Table).
    Logic: Connects User (Client_No) <-> Bank (Bank_ID) <-> Network (Card_Network_ID)
    using raw data from Transaction table.
    """
    logger.info("START: Generating Master Card Links...")

    # We use pure SQL for performance and data integrity
    # Logic:
    # 1. Join Transaction with User_Profile to get Client_No (based on addr1 + email)
    # 2. Join Transaction with Description_Card to get Network_ID (based on card4 + card6)
    # 3. Bank_ID is simply card1

    query = """
    INSERT INTO master_card ("Card_Raw_ID", "Client_No", "Bank_ID", "Card_Network_ID")
    SELECT DISTINCT
        t.card1 AS Card_Raw_ID,
        u.client_no AS Client_No,
        t.card1 AS Bank_ID,
        d."Card_Network_ID" AS Card_Network_ID
    FROM transaction t
    -- Link to User
    JOIN user_profile u ON t."addr1" = u.raw_region_code AND t."P_emaildomain" = u.raw_email_domain
    -- Link to Card Description (Network)
    JOIN description_card d ON t.card4 = d."Network_Name" AND t.card6 = d."Card_Type"
    -- Link to Bank (Ensure Bank exists)
    JOIN card_issuing_bank b ON t.card1 = b."Bank_ID"
    WHERE t.card1 IS NOT NULL
    ON CONFLICT DO NOTHING;
    """

    try:
        with engine.begin() as conn:
            result = conn.execute(text(query))
            logger.info(f"   -> Successfully inserted/linked records into Master Card.")
    except Exception as e:
        logger.error(f"Error generating Master Card: {e}")
        return

    logger.info("COMPLETED STEP 1: Master Card.")


def generate_beneficiaries(batch_size=2000):
    """
    Generate Beneficiary List (Fake Data).
    Logic: For each real Client_No, generate 0-3 fake beneficiaries.
    """
    logger.info("START: Generating Beneficiary List...")

    # 1. Get list of real Client_No
    try:
        clients = pd.read_sql('SELECT client_no FROM user_profile', engine)['client_no'].tolist()
    except Exception as e:
        logger.error(f"Error reading User Profile: {e}")
        return

    total_clients = len(clients)
    logger.info(f"   -> Found {total_clients} clients. Starting generation...")

    benes = []
    count_inserted = 0

    # Pre-generate some banks for realism
    bank_list = [f"{fake.last_name()} Bank" for _ in range(20)]

    for client_no in clients:
        # 30% chance a user has no beneficiaries
        if random.random() > 0.7:
            continue

        # Generate 1 to 3 beneficiaries per user
        for _ in range(random.randint(1, 3)):
            fake_name = fake.name()
            # Create a realistic email
            fake_email = f"{fake_name.replace(' ', '.').lower()}@{fake.free_email_domain()}"

            benes.append({
                "Client_No": client_no,
                "Beneficiary_Name": fake_name[:100],
                "Beneficiary_Account": fake.iban()[:34],  # IBAN max length is 34
                "Beneficiary_Bank": random.choice(bank_list)[:100],
                "Beneficiary_Email": fake_email[:100],
                "Relation_Type": random.choice(['Friend', 'Family', 'Partner', 'Business', 'Service']),
                "Date_Added": fake.date_between(start_date='-2y', end_date='today')
            })

        if len(benes) >= batch_size:
            pd.DataFrame(benes).to_sql('beneficiary_list', engine, if_exists='append', index=False, method='multi')
            count_inserted += len(benes)
            logger.info(f"   -> Inserted {count_inserted} beneficiaries...")
            benes = []

    # Insert remaining
    if benes:
        pd.DataFrame(benes).to_sql('beneficiary_list', engine, if_exists='append', index=False, method='multi')

    logger.info("✅ COMPLETED STEP 2: Beneficiary List.")


if __name__ == "__main__":
    try:
        generate_master_card()
        generate_beneficiaries()
        print("\n LINKING DATA SUCCESSFUL!")
    except Exception as e:
        print(f"\n FATAL ERROR: {e}")
