-- CARD-ISSUING BANK
-- DROP TABLE IF EXISTS card_issuing_bank CASCADE;
CREATE TABLE card_issuing_bank (
    "Bank_ID" INT PRIMARY KEY,
    "Bank_Name" VARCHAR(255),
    "Bank_Country_Code" INT,
    "Bank_Sub_Branch" INT
);

INSERT INTO card_issuing_bank ("Bank_ID", "Bank_Country_Code", "Bank_Sub_Branch")
SELECT DISTINCT
    "card1",
    CAST("card3" AS INT),
    CAST("card2" AS INT)
FROM transaction
WHERE "card1" IS NOT NULL
ON CONFLICT ("Bank_ID") DO NOTHING;


-- DESCRIPTION CARD
-- DROP TABLE IF EXISTS description_card CASCADE;
CREATE TABLE description_card (
    "Card_Network_ID" SERIAL PRIMARY KEY,
    "Network_Name" VARCHAR(50),
    "Card_Type" VARCHAR(50),
    "Network_Country" VARCHAR(100),
    "Support_Hotline" VARCHAR(50)
);

INSERT INTO description_card ("Network_Name", "Card_Type")
SELECT DISTINCT "card4", "card6"
FROM transaction
WHERE "card4" IS NOT NULL AND "card6" IS NOT NULL;
