use async_stream::stream;
use async_trait::async_trait;
use aws_config::meta::region::RegionProviderChain;
use aws_sdk_dynamodb::error::{
    DeleteItemError, DeleteItemErrorKind, PutItemError, PutItemErrorKind, ScanError, ScanErrorKind,
    UpdateItemError, UpdateItemErrorKind,
};
use aws_sdk_dynamodb::model::{
    AttributeDefinition, AttributeValue, BillingMode, KeySchemaElement, KeyType,
    ScalarAttributeType,
};
use aws_sdk_dynamodb::{Client, Endpoint, Region, SdkError};
use chrono::Utc;
use relay::memory_store::backing::{Backing, Error, Result};
use relay::memory_store::StoredJob;
use serde_json::value::RawValue;
use std::pin::Pin;
use tokio_stream::Stream;

const PK_KEY: &str = "queue_job_id";
const TIMESTAMP_KEY: &str = "timestamp";
const DATA_KEY: &str = "data";
const STATE_KEY: &str = "state";
const RETRIES_KEY: &str = "retries";
const IN_FLIGHT_KEY: &str = "in_flight";

/// `DynamoDB` backing store
pub struct Store {
    client: Client,
    table: String,
}

impl Store {
    /// Creates a new backing store with default settings for `DynamoDB`.
    /// If region is set to `localhost` it will be configured to read from a local `DynamoDB` instance
    /// for local development and integration tests and table automatically created.
    ///
    /// # Errors
    ///
    /// Will return `Err` if initializing the `DynamoDB` Client fails or when running locally
    /// fails to create the `DynamoDB` table.
    pub async fn default(region: String, table: String) -> anyhow::Result<Self> {
        let local = region == "localhost";
        let region_provider = RegionProviderChain::default_provider().or_else(Region::new(region));
        let shared_config = aws_config::from_env().region(region_provider).load().await;

        let dynamodb_local_config = if local {
            aws_sdk_dynamodb::config::Builder::from(&shared_config)
                .endpoint_resolver(
                    // 8000 is the default dynamodb port
                    Endpoint::immutable(http::Uri::from_static("http://localhost:8000")),
                )
                .build()
        } else {
            aws_sdk_dynamodb::config::Builder::from(&shared_config).build()
        };

        let client = Client::from_conf(dynamodb_local_config);
        let store = Self { client, table };
        store.create_table().await?;
        Ok(store)
    }

    async fn create_table(&self) -> anyhow::Result<()> {
        if let Err(e) = self
            .client
            .delete_table()
            .table_name(&self.table)
            .send()
            .await
        {
            dbg!(e);
        }

        let pk = KeySchemaElement::builder()
            .attribute_name(PK_KEY)
            .key_type(KeyType::Hash)
            .build();

        let ad_pk = AttributeDefinition::builder()
            .attribute_name(PK_KEY)
            .attribute_type(ScalarAttributeType::S)
            .build();

        self.client
            .create_table()
            .billing_mode(BillingMode::PayPerRequest)
            .table_name(&self.table)
            .key_schema(pk)
            .attribute_definitions(ad_pk)
            .send()
            .await?;

        Ok(())
    }
}

#[async_trait]
impl Backing for Store {
    async fn push(&self, stored: &StoredJob) -> Result<()> {
        let data = serde_json::to_string(&stored).map_err(|e| Error::Push {
            job_id: stored.job.id.clone(),
            queue: stored.job.queue.clone(),
            message: e.to_string(),
            is_retryable: false,
        })?;

        let mut request = self
            .client
            .put_item()
            .table_name(&self.table)
            .item(
                PK_KEY,
                AttributeValue::S(format!("{}-{}", &stored.job.queue, &stored.job.id)),
            )
            .item(DATA_KEY, AttributeValue::S(data))
            .item(
                TIMESTAMP_KEY,
                AttributeValue::N(Utc::now().timestamp_nanos().to_string()),
            )
            .item(RETRIES_KEY, AttributeValue::N(stored.retries.to_string()))
            .item(IN_FLIGHT_KEY, AttributeValue::Bool(stored.in_flight))
            .condition_expression(format!("attribute_not_exists({})", PK_KEY));

        if let Some(state) = &stored.state {
            request = request.item(STATE_KEY, AttributeValue::S(state.to_string()));
        }

        request.send().await.map_err(|e| Error::Push {
            job_id: stored.job.id.clone(),
            queue: stored.job.queue.clone(),
            message: e.to_string(),
            is_retryable: is_retryable_put(e),
        })?;
        Ok(())
    }

    async fn remove(&self, stored: &StoredJob) -> Result<()> {
        self.client
            .delete_item()
            .table_name(&self.table)
            .key(
                PK_KEY,
                AttributeValue::S(format!("{}-{}", &stored.job.queue, &stored.job.id)),
            )
            .send()
            .await
            .map_err(|e| Error::Remove {
                job_id: stored.job.id.clone(),
                queue: stored.job.queue.clone(),
                message: e.to_string(),
                is_retryable: is_retryable_delete(e),
            })?;
        Ok(())
    }

    async fn update(
        &self,
        queue: &str,
        job_id: &str,
        state: &Option<Box<RawValue>>,
        retries: Option<u8>,
        in_flight: Option<bool>,
    ) -> Result<()> {
        let mut update = self
            .client
            .update_item()
            .table_name(&self.table)
            .key(PK_KEY, AttributeValue::S(format!("{}-{}", &queue, &job_id)));

        let state = state.as_ref().map(std::string::ToString::to_string);

        match (state, retries, in_flight) {
            (Some(state), Some(retries), Some(in_flight)) => {
                update = update
                    .expression_attribute_names("#s", STATE_KEY)
                    .expression_attribute_values(":state", AttributeValue::S(state))
                    .expression_attribute_names("#r", RETRIES_KEY)
                    .expression_attribute_values(":r", AttributeValue::N(retries.to_string()))
                    .expression_attribute_names("#i", STATE_KEY)
                    .expression_attribute_values(":i", AttributeValue::Bool(in_flight))
                    .update_expression("SET #s = :state, #r = :r, #i = :i");
            }
            (Some(state), Some(retries), None) => {
                update = update
                    .expression_attribute_names("#s", STATE_KEY)
                    .expression_attribute_values(":state", AttributeValue::S(state))
                    .expression_attribute_names("#r", RETRIES_KEY)
                    .expression_attribute_values(":r", AttributeValue::N(retries.to_string()))
                    .update_expression("SET #s = :state, #r = :r");
            }
            (Some(state), None, Some(in_flight)) => {
                update = update
                    .expression_attribute_names("#s", STATE_KEY)
                    .expression_attribute_values(":state", AttributeValue::S(state))
                    .expression_attribute_names("#i", STATE_KEY)
                    .expression_attribute_values(":i", AttributeValue::Bool(in_flight))
                    .update_expression("SET #s = :state, #i = :i");
            }
            (Some(state), None, None) => {
                update = update
                    .expression_attribute_names("#s", STATE_KEY)
                    .expression_attribute_values(":state", AttributeValue::S(state))
                    .update_expression("SET #s = :state");
            }
            (None, Some(retries), Some(in_flight)) => {
                update = update
                    .expression_attribute_names("#r", RETRIES_KEY)
                    .expression_attribute_values(":r", AttributeValue::N(retries.to_string()))
                    .expression_attribute_names("#i", STATE_KEY)
                    .expression_attribute_values(":i", AttributeValue::Bool(in_flight))
                    .update_expression("SET #r = :r, #i = :i");
            }
            (None, Some(retries), None) => {
                update = update
                    .expression_attribute_names("#r", RETRIES_KEY)
                    .expression_attribute_values(":r", AttributeValue::N(retries.to_string()))
                    .update_expression("SET #r = :r");
            }
            (None, None, Some(in_flight)) => {
                update = update
                    .expression_attribute_names("#i", STATE_KEY)
                    .expression_attribute_values(":i", AttributeValue::Bool(in_flight))
                    .update_expression("SET #i = :i");
            }
            (None, None, None) => {
                return Ok(());
            }
        };

        update.send().await.map_err(|e| Error::Update {
            job_id: job_id.to_string(),
            queue: queue.to_string(),
            message: e.to_string(),
            is_retryable: is_retryable_update(e),
        })?;

        Ok(())
    }

    fn recover(&self) -> Pin<Box<dyn Stream<Item = Result<StoredJob>> + '_>> {
        Box::pin(stream! {
            struct SortableJobs {
                job: StoredJob,
                timestamp: i64,
            }
            let mut jobs = Vec::new();
            let mut last = None;

            loop {
                let results = self
                    .client
                    .scan()
                    .table_name(&self.table)
                    .limit(10_000)
                    .set_exclusive_start_key(last)
                    .send()
                    .await
                    .map_err(|e| Error::Recovery {
                        message: e.to_string(),
                        is_retryable: is_retryable_scan(e),
                    })?;

                if let Some(items) = results.items {
                    for item in items {
                        let timestamp = match item.get(TIMESTAMP_KEY) {
                            None => i64::MAX,
                            Some(av) => match av {
                                AttributeValue::S(ts) => match ts.parse::<i64>() {
                                    Ok(i) => i,
                                    Err(_) => i64::MAX,
                                },
                                _ => i64::MAX,
                            },
                        };
                        let data = match item.get(DATA_KEY) {
                            None => None,
                            Some(av) => match av {
                                AttributeValue::S(data) => {
                                    let job: StoredJob = serde_json::from_str(data).map_err(|e| Error::Recovery {
                                        message: e.to_string(),
                                        is_retryable: false,
                                    })?;
                                    Some(job)
                                }
                                _ => None,
                            },
                        };
                        let state = match item.get(STATE_KEY) {
                            None => None,
                            Some(state) => match state {
                                AttributeValue::S(s) =>  {
                                    let state: serde_json::Result<Box<RawValue>> = serde_json::from_str(s);
                                    match state {
                                        Err(_) => None,
                                        Ok(state)=> Some(state)
                                    }
                                },
                                _ => None,
                            },
                        };
                        let retries = match item.get(RETRIES_KEY) {
                            None => 0,
                            Some(v) => match v {
                                AttributeValue::N(n) => match n.parse::<u8>() {
                                    Ok(i) => i,
                                    Err(_) => 0,
                                },
                                _ => 0,
                            },
                        };
                        let in_flight = match item.get(IN_FLIGHT_KEY) {
                            None => false,
                            Some(v) => match v {
                                AttributeValue::Bool(b) => *b,
                                _ => false,
                            },
                        };
                        if let Some(mut job) = data {
                            if state.is_some(){
                                job.state = state;
                            }
                            job.retries = retries;
                            job.in_flight = in_flight;
                            jobs.push(SortableJobs { job, timestamp });
                        }
                    }
                }

                if let Some(last_key) = results.last_evaluated_key {
                    last = Some(last_key);
                    continue;
                }
                break;
            }

            jobs.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));
            for sj in jobs {
                yield Ok(sj.job)
            }
        })
    }
}

#[inline]
fn is_retryable_put(e: SdkError<PutItemError>) -> bool {
    match e {
        SdkError::ServiceError { err, .. } => matches!(
            err.kind,
            PutItemErrorKind::ProvisionedThroughputExceededException(_)
                | PutItemErrorKind::RequestLimitExceeded(_)
        ),
        _ => is_retryable(e),
    }
}

#[inline]
fn is_retryable_delete(e: SdkError<DeleteItemError>) -> bool {
    match e {
        SdkError::ServiceError { err, .. } => matches!(
            err.kind,
            DeleteItemErrorKind::ProvisionedThroughputExceededException(_)
                | DeleteItemErrorKind::RequestLimitExceeded(_)
        ),
        _ => is_retryable(e),
    }
}

#[inline]
fn is_retryable_update(e: SdkError<UpdateItemError>) -> bool {
    match e {
        SdkError::ServiceError { err, .. } => matches!(
            err.kind,
            UpdateItemErrorKind::ProvisionedThroughputExceededException(_)
                | UpdateItemErrorKind::RequestLimitExceeded(_)
        ),
        _ => is_retryable(e),
    }
}

#[inline]
fn is_retryable_scan(e: SdkError<ScanError>) -> bool {
    match e {
        SdkError::ServiceError { err, .. } => matches!(
            err.kind,
            ScanErrorKind::ProvisionedThroughputExceededException(_)
                | ScanErrorKind::RequestLimitExceeded(_)
        ),
        _ => is_retryable(e),
    }
}

#[inline]
fn is_retryable<E>(e: SdkError<E>) -> bool {
    match e {
        SdkError::TimeoutError(_) | SdkError::ResponseError { .. } => true,
        SdkError::DispatchFailure(e) => e.is_timeout() || e.is_io(),
        _ => false,
    }
}
