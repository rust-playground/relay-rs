use chrono::Utc;
use tokio_postgres::Client;

pub(crate) struct Migration {
    name: &'static str,
    sql: &'static str,
}

impl Migration {
    pub(crate) const fn new(name: &'static str, sql: &'static str) -> Migration {
        Migration { name, sql }
    }
}

#[tracing::instrument(name = "run_migrations", level = "debug", skip_all, fields(table_name = %table_name))]
pub(crate) async fn run_migrations(
    table_name: &str,
    client: &mut Client,
    migrations: &[Migration],
) -> anyhow::Result<()> {
    let transaction = client.transaction().await?;

    transaction
        .batch_execute(&format!(
            r#"
        SELECT pg_advisory_xact_lock(13);
        CREATE TABLE IF NOT EXISTS {table_name} (
            name         varchar NOT NULL,
            applied_at   timestamp without time zone NOT NULL,
            PRIMARY KEY (name)
        );
        "#
        ))
        .await?;

    for m in migrations {
        let exists: bool = transaction
            .query_one(
                &format!("SELECT EXISTS(SELECT 1 FROM {table_name} WHERE name=$1)"),
                &[&m.name],
            )
            .await?
            .get(0);
        if exists {
            continue;
        }
        transaction
            .execute(
                &format!("INSERT INTO {table_name} (name, applied_at) VALUES ($1, $2)"),
                &[&m.name, &Utc::now().naive_utc()],
            )
            .await?;

        transaction.batch_execute(m.sql).await?;
    }

    transaction.commit().await?;
    Ok(())
}
