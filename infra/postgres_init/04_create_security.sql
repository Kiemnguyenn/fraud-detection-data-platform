-- DEVICE FINGERPRINT
-- DROP TABLE IF EXISTS device_fingerprint CASCADE;
CREATE TABLE device_fingerprint (
    "Device_ID" SERIAL PRIMARY KEY,
    "Device_Name" VARCHAR(255),
    "Device_Type" VARCHAR(50),
    "OS_Version" VARCHAR(100),
    "Browser_Version" VARCHAR(100),
    "Screen_Resolution" VARCHAR(50)--
);

INSERT INTO device_fingerprint (
    "Device_Name", "Device_Type", "OS_Version", "Browser_Version", "Screen_Resolution"
)
SELECT DISTINCT
    "DeviceInfo",
    "DeviceType",
    "id_30",
    "id_31",
    "id_33"
FROM identity
WHERE "DeviceInfo" IS NOT NULL
ON CONFLICT DO NOTHING;
