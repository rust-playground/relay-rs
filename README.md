# relay-rs

This crate contains a no nonsense ordered job runner with configurable backends for optional backing storage, metrics and servers to host.

### Features
Optional features:
- [`metrics-prometheus`][]: Enables emitting of Prometheus metrics via a scraping endpoint.
- [`backing-sqlite`][]: Enables an SQLite backed persistent store to handle crashes/restarts..
- [`backing-postgres`][]: Enables a Postgres backed persistent store to handle crashes/restarts.
- [`backing-redis`][]: Enables a Redis backed persistent store to handle crashes/restarts.
- [`backing-dynamodb`][]: Enables an DynamoDB backed persistent store to handle crashes/restarts.

[`metrics-prometheus`]: https://crates.io/crates/metrics-exporter-prometheus
[`backing-sqlite`]: https://crates.io/crates/sqlx
[`backing-postgres`]: https://crates.io/crates/sqlx
[`backing-redis`]: https://crates.io/crates/redis
[`backing-dynamodb`]: https://crates.io/crates/aws-sdk-dynamodb

#### Servers

| server                                | description                                                                                          |
|---------------------------------------|------------------------------------------------------------------------------------------------------|
| [HTTP](./relay-server-http/README.md) | Exposes the relay service over HTTP, see [here](./relay-server-http/README.md) for more information. |

#### How to build
```shell
~ cargo build -p relay-bin --release
```

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
