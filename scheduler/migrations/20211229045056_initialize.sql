CREATE TABLE IF NOT EXISTS scheduled_jobs (
    id         varchar NOT NULL,
    data       jsonb NOT NULL,
    last_run timestamp without time zone default null,
    PRIMARY KEY (id)
);