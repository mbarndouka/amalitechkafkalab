-- database/schema.sql

CREATE TABLE IF NOT EXISTS heart_rate_logs (
    id SERIAL PRIMARY KEY,
    event_id UUID UNIQUE NOT NULL,
    customer_id VARCHAR(50) NOT NULL,
    heart_rate INTEGER NOT NULL,
    status VARCHAR(15) NOT NULL,
    recorded_at TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Optimize queries for specific customer history analysis (e.g., Dashboards)
CREATE INDEX IF NOT EXISTS idx_customer_recorded_at
ON heart_rate_logs (customer_id, recorded_at DESC);

-- Optimize queries searching for system-wide anomalies
CREATE INDEX IF NOT EXISTS idx_status ON heart_rate_logs (status);