CREATE TABLE IF NOT EXISTS heart_rate_logs (
    id SERIAL PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL,
    heart_rate INTEGER NOT NULL,
    recorded_at TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index for fast time-range queries (Essential for dashboards)
CREATE INDEX idx_recorded_at ON heart_rate_logs (recorded_at);