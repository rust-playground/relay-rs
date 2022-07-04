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

| Language | Description                 |
|----------|-----------------------------|
| [Go](https://github.com/go-playground/relay-client-go)   | Go low & high level client. |


#### License

<sup>
Licensed under either of <a href="LICENSE-APACHE">Apache License, Version
2.0</a> or <a href="LICENSE-MIT">MIT license</a> at your option.
</sup>

<br>

<sub>
Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in Proteus by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.
</sub>
