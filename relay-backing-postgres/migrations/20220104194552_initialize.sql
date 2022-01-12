CREATE TABLE IF NOT EXISTS jobs (
    id          varchar NOT NULL,
    data        jsonb NOT NULL,
    state       jsonb DEFAULT NULL,
    retries     integer default 0,
    in_flight   boolean DEFAULT FALSE,
    created_at  timestamp without time zone NOT NULL,
    PRIMARY KEY (id)
);
CREATE INDEX IF NOT EXISTS idx_id_created_at ON jobs (id, created_at);