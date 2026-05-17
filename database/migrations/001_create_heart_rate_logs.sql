CREATE TABLE IF NOT EXISTS heart_rate_logs (
    id SERIAL PRIMARY KEY,
    event_id UUID UNIQUE NOT NULL,
    customer_id VARCHAR(50) NOT NULL
        CONSTRAINT heart_rate_logs_customer_id_not_blank
        CHECK (length(trim(customer_id)) > 0),
    heart_rate INTEGER NOT NULL
        CONSTRAINT heart_rate_logs_heart_rate_range
        CHECK (heart_rate BETWEEN 0 AND 300),
    status VARCHAR(15) NOT NULL
        CONSTRAINT heart_rate_logs_status_valid
        CHECK (status IN ('NORMAL', 'WARNING', 'CRITICAL')),
    recorded_at TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_customer_recorded_at
ON heart_rate_logs (customer_id, recorded_at DESC);

CREATE INDEX IF NOT EXISTS idx_status
ON heart_rate_logs (status);
