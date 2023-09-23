ALTER TABLE jobs ALTER COLUMN data TYPE text;
ALTER TABLE jobs ALTER COLUMN state TYPE text;
ALTER TABLE jobs ALTER COLUMN data TYPE bytea USING data::bytea;
ALTER TABLE jobs ALTER COLUMN state TYPE bytea USING state::bytea;