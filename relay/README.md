# relay

This contains a no nonsense, horizontally scalable, ordered job runner backed by Postgres.

### Features
Optional features:
- [`metrics-prometheus`][]: Enables emitting of Prometheus metrics via a scraping endpoint.
- [`backend-postgres`][]: Enables the Postgres backend (default).
- [`metrics-prometheus`][]: Enables emitting of Prometheus metrics via a scraping endpoint.

[`frontend-http`]: `relay_frontend_http`
[`backend-postgres`]: `relay_backend_postgres`
[`metrics-prometheus`]: https://crates.io/crates/metrics-exporter-prometheus

#### Requirements
- Postgres 9.5+

#### HTTP API
For details about the API see [here](../relay-frontend-http/API.md). 

#### How to build
```shell
~ cargo build -p relay --release
```

#### Clients
Here is a list of existing clients.

| Language                                                 | Description                 |
|----------------------------------------------------------|-----------------------------|
| [Go](https://github.com/go-playground/relay-client-go)   | Go low & high level client. |
| [Rust](https://github.com/rust-playground/relay-rs/blob/main/relay-frontend-http/src/client/client.rs) | Rust client and consumer.   |


#### License

### Licensing

AGPL-3.0. See [LICENSE-AGPL](LICENSE) for details.