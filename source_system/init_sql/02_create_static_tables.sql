-- PRODUCT DESCRIPTION
-- DROP TABLE IF EXISTS product_description;
CREATE TABLE product_description (
    "ProductCD" VARCHAR(10) PRIMARY KEY,
    "ProductDesc" VARCHAR(255)
);

INSERT INTO product_description VALUES
('W', 'Web Transaction (Giao dịch qua Web)'),
('C', 'Credit Payment (Thanh toán tín dụng)'),
('R', 'Retail (Bán lẻ)'),
('H', 'High Value (Giao dịch giá trị cao)'),
('S', 'Service (Dịch vụ)');


-- STATUS
-- DROP TABLE IF EXISTS status;
CREATE TABLE status (
    "StatusID" INT PRIMARY KEY,
    "StatusCode" VARCHAR(50),
    "Description" VARCHAR(255)
);

INSERT INTO status VALUES
(1, 'PENDING', 'Giao dịch đang chờ xử lý'),
(2, 'COMPLETED', 'Giao dịch thành công'),
(3, 'FAILED', 'Giao dịch thất bại'),
(4, 'BLOCKED', 'Giao dịch bị chặn do nghi ngờ gian lận'),
(5, 'REVIEW', 'Giao dịch cần kiểm tra thủ công');


-- LOCATION
-- DROP TABLE IF EXISTS location;
CREATE TABLE location (
    "LocationID" SERIAL PRIMARY KEY,
    "addr1" REAL,
    "addr2" REAL,
    "Region" VARCHAR(100),
    "Country" VARCHAR(100)
);

INSERT INTO location ("addr1", "addr2")
SELECT DISTINCT "addr1", "addr2"
FROM transaction
WHERE "addr1" IS NOT NULL OR "addr2" IS NOT NULL;


-- BLACKLIST RULES
-- DROP TABLE IF EXISTS blacklist_rules CASCADE;
CREATE TABLE blacklist_rules (
    "Rule_ID" SERIAL PRIMARY KEY,
    "Rule_Name" VARCHAR(100),
    "Criteria_Logic" TEXT,
    "Action_Type" VARCHAR(50),
    "Severity_Level" INT,
    "Created_Date" TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    "Is_Active" BOOLEAN DEFAULT TRUE
);

INSERT INTO blacklist_rules ("Rule_Name", "Criteria_Logic", "Action_Type", "Severity_Level") VALUES
('High Amount Transaction', 'TransactionAmt > 5000 AND card_type = Credit', 'ALERT', 5),
('Foreign IP Address', 'addr2 != Billing_Country_Code', '2FA_REQ', 7),
('Blacklisted Device', 'Device_ID IN (SELECT Blacklisted_Device_ID FROM blacklist_devices)', 'BLOCK', 10),
('High Velocity', 'Transaction_Count_1Min > 5', 'BLOCK', 9),
('Suspicious Email Domain', 'P_emaildomain IN (protonmail.com, tempmail.org)', 'REVIEW', 6);
