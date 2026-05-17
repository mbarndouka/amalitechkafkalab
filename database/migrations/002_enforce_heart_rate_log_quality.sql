UPDATE heart_rate_logs
SET created_at = CURRENT_TIMESTAMP
WHERE created_at IS NULL;

ALTER TABLE heart_rate_logs
ALTER COLUMN created_at SET NOT NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'heart_rate_logs_customer_id_not_blank'
    ) THEN
        ALTER TABLE heart_rate_logs
        ADD CONSTRAINT heart_rate_logs_customer_id_not_blank
        CHECK (length(trim(customer_id)) > 0) NOT VALID;
    END IF;
END $$;

ALTER TABLE heart_rate_logs
VALIDATE CONSTRAINT heart_rate_logs_customer_id_not_blank;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'heart_rate_logs_heart_rate_range'
    ) THEN
        ALTER TABLE heart_rate_logs
        ADD CONSTRAINT heart_rate_logs_heart_rate_range
        CHECK (heart_rate BETWEEN 0 AND 300) NOT VALID;
    END IF;
END $$;

ALTER TABLE heart_rate_logs
VALIDATE CONSTRAINT heart_rate_logs_heart_rate_range;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'heart_rate_logs_status_valid'
    ) THEN
        ALTER TABLE heart_rate_logs
        ADD CONSTRAINT heart_rate_logs_status_valid
        CHECK (status IN ('NORMAL', 'WARNING', 'CRITICAL')) NOT VALID;
    END IF;
END $$;

ALTER TABLE heart_rate_logs
VALIDATE CONSTRAINT heart_rate_logs_status_valid;
