ENVS := RUST_LOG=debug \
        DATABASE_MAX_CONNECTIONS=3

services.up:
	@docker-compose up -d
	@dockerize -wait tcp://:5432
	@sleep 3

services.down:
	@docker-compose stop --timeout 0

test:
	$(ENVS) \
    DATABASE_URL=postgres://username:pass@localhost:5432/relay?sslmode=disable \
    cargo test -- --show-output

test.all: services.up test services.down

.PHONY: test services.up services.down test.all
