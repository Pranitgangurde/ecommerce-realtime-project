CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS marts;

-- Raw landing table for clickstream events from Kafka
CREATE TABLE raw.clickstream_events (
    event_id UUID PRIMARY KEY,
    event_type VARCHAR(50) NOT NULL,
    user_id BIGINT,
    session_id UUID,
    product_id BIGINT,
    event_timestamp TIMESTAMP NOT NULL,
    properties JSONB,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_clickstream_event_timestamp ON raw.clickstream_events(event_timestamp);
CREATE INDEX idx_clickstream_event_type ON raw.clickstream_events(event_type);
CREATE INDEX idx_clickstream_user_id ON raw.clickstream_events(user_id);

-- Aggregated mart for Metabase dashboard
CREATE TABLE marts.revenue_per_minute (
    minute_bucket TIMESTAMP PRIMARY KEY,
    event_count INTEGER NOT NULL,
    unique_users INTEGER NOT NULL,
    total_clicks INTEGER NOT NULL,
    total_purchases INTEGER NOT NULL
);