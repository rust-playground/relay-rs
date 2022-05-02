CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

ALTER TABLE jobs ADD COLUMN IF NOT EXISTS "iid" uuid DEFAULT uuid_generate_v4();
CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_iid ON jobs (iid);