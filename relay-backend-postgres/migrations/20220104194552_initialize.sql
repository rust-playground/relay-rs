CREATE TABLE IF NOT EXISTS jobs (
    id           varchar NOT NULL,
    queue        varchar NOT NULL,
    timeout      interval NOT NULL,
    max_retries  integer NOT NULL,
    retries_remaining integer NOT NULL,
    data         jsonb NOT NULL,
    state        jsonb DEFAULT NULL,
    in_flight    boolean DEFAULT FALSE,
    expires_at   timestamp without time zone,
    updated_at   timestamp without time zone NOT NULL,
    created_at   timestamp without time zone NOT NULL,
    run_at       timestamp without time zone NOT NULL,
    PRIMARY KEY (queue, id)
);
CREATE INDEX IF NOT EXISTS idx_queue_in_flight_created_at ON jobs (queue, in_flight, run_at);
CREATE INDEX IF NOT EXISTS idx_queue_expires_remain ON jobs (in_flight, expires_at, retries_remaining);

CREATE TABLE IF NOT EXISTS internal_state (
    id varchar NOT NULL,
    last_run timestamp without time zone NOT NULL,
    PRIMARY KEY (id)
)