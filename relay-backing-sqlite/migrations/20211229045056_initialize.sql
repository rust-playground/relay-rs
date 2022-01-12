CREATE TABLE IF NOT EXISTS jobs (
    id varchar NOT NULL,
    data blob NOT NULL,
    state blob DEFAULT NULL,
    retries number default 0,
    in_flight BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP NOT NULL,
    PRIMARY KEY (id)
);
CREATE INDEX IF NOT EXISTS idx_id_created_at ON jobs (id, created_at);