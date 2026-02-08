import os
from urllib.parse import quote_plus
# --- CONFIG DATABASE ---
DB_USER     = "admin"
DB_PASSWORD = "Admin123!@#"
DB_HOST     = "localhost"
DB_PORT     = "5433"
DB_NAME     = "core"

encoded_password = quote_plus(DB_PASSWORD)
# Format: postgresql://user:password@host:port/dbname
DB_URL = f"postgresql://{DB_USER}:{encoded_password}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# --- DATA PATH CONFIGURATION ---
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

DATA_RAW_DIR = os.path.join(PROJECT_ROOT, 'data', 'raw')

FILES_CONFIG = {
    'transaction': ['train_transaction.csv', 'test_transaction.csv'],
    'identity': ['train_identity.csv', 'test_identity.csv']
}
