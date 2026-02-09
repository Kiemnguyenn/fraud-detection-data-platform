-- USER PROFILE
-- DROP TABLE IF EXISTS user_profile CASCADE;
CREATE TABLE user_profile (
    client_no CHAR(8) PRIMARY KEY,
    full_name VARCHAR(100),
    tax_code VARCHAR(20),
    identity_card_id VARCHAR(20),
    identity_type VARCHAR(10) DEFAULT 'CCCD',
    raw_email_domain VARCHAR(100),
    raw_region_code INT,
    email_address VARCHAR(100),
    phone_number VARCHAR(20),
    permanent_address TEXT,
    kyc_level SMALLINT DEFAULT 1,
    customer_segment VARCHAR(20),
    status VARCHAR(20) DEFAULT 'ACTIVE',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(50) DEFAULT 'SYSTEM_ETL',
    updated_at TIMESTAMP,
    updated_by VARCHAR(50),
    deleted_at TIMESTAMP,
    deleted_reason VARCHAR(255)
);
CREATE INDEX idx_profile_mapping ON user_profile(raw_email_domain, raw_region_code);


-- USER ACCOUNTS
-- DROP TABLE IF EXISTS user_accounts CASCADE;
CREATE TABLE user_accounts (
    account_number VARCHAR(20) PRIMARY KEY,
    client_no CHAR(8) NOT NULL,
    linked_raw_card_bin INT,
    linked_card_type VARCHAR(20),
    account_name VARCHAR(100),
    currency_code CHAR(3) DEFAULT 'USD',
    current_balance NUMERIC(15, 2),
    daily_transfer_limit NUMERIC(15, 2),
    account_status VARCHAR(20) DEFAULT 'OPEN',
    is_primary BOOLEAN DEFAULT FALSE,
    opened_date DATE DEFAULT CURRENT_DATE,
    closed_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(50) DEFAULT 'SYSTEM_ETL',
    deleted_at TIMESTAMP,
    CONSTRAINT fk_user_profile FOREIGN KEY (client_no) REFERENCES user_profile(client_no)
);
CREATE INDEX idx_acc_bin_client ON user_accounts(linked_raw_card_bin, client_no);


-- MASTER CARD
-- DROP TABLE IF EXISTS master_card CASCADE;
CREATE TABLE master_card (
    "Card_Key" SERIAL PRIMARY KEY,
    "Card_Raw_ID" INT, -- card1
    "Client_No" CHAR(8) REFERENCES user_profile(client_no),
    "Bank_ID" INT REFERENCES card_issuing_bank("Bank_ID"),
    "Card_Network_ID" INT REFERENCES description_card("Card_Network_ID")
);
ALTER TABLE master_card ADD CONSTRAINT fk_bank FOREIGN KEY ("Bank_ID") REFERENCES card_issuing_bank ("Bank_ID");
ALTER TABLE master_card ADD CONSTRAINT fk_network FOREIGN KEY ("Card_Network_ID") REFERENCES description_card ("Card_Network_ID");


-- BENEFICIARY LIST
-- DROP TABLE IF EXISTS beneficiary_list CASCADE;
CREATE TABLE beneficiary_list (
    "Beneficiary_ID" SERIAL PRIMARY KEY,
    "Client_No" CHAR(8) ,
    "Beneficiary_Name" VARCHAR(100),
    "Beneficiary_Account" VARCHAR(50),
    "Beneficiary_Bank" VARCHAR(100),
    "Beneficiary_Email" VARCHAR(100),
    "Relation_Type" VARCHAR(50),
    "Date_Added" DATE DEFAULT CURRENT_DATE
);
