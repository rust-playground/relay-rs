use crate::client::Client;
use async_channel::{Receiver, Sender};
use relay_core::{Job, Worker};
use serde::de::DeserializeOwned;
use std::future::Future;
use std::marker::PhantomData;
use std::sync::Arc;
use tokio::{select, task};

pub struct ConsumerBuilder<W, P, S> {
    client: Arc<Client>,
    queue: String,
    worker: Arc<W>,
    max_workers: usize,
    _payload: PhantomData<P>,
    _state: PhantomData<S>,
}

impl<W, P, S> ConsumerBuilder<W, P, S>
where
    W: Worker<Client, P, S>,
{
    pub fn new(client: Arc<Client>, queue: &str, worker: W) -> Self {
        Self {
            client,
            queue: queue.to_string(),
            worker: Arc::new(worker),
            max_workers: 10,
            _payload: Default::default(),
            _state: Default::default(),
        }
    }

    pub fn max_workers(mut self, max_workers: usize) -> Self {
        self.max_workers = max_workers;
        self
    }

    pub fn build(self) -> Consumer<W, P, S> {
        Consumer {
            client: self.client,
            queue: self.queue,
            worker: self.worker,
            max_workers: self.max_workers,
            _payload: Default::default(),
            _state: Default::default(),
        }
    }
}

pub struct Consumer<W, P, S> {
    client: Arc<Client>,
    queue: String,
    max_workers: usize,
    worker: Arc<W>,
    _payload: PhantomData<P>,
    _state: PhantomData<S>,
}

impl<W, P, S> Consumer<W, P, S>
where
    W: Worker<Client, P, S> + Send + Sync + 'static,
    P: DeserializeOwned + Send + Sync + 'static,
    S: DeserializeOwned + Send + Sync + 'static,
{
    pub async fn start(
        &self,
        cancel: impl Future<Output = ()> + Send + 'static,
    ) -> Result<(), anyhow::Error> {
        let (tx, rx): (Sender<Job<P, S>>, Receiver<Job<P, S>>) =
            async_channel::bounded(self.max_workers);

        let (tx_sem, rx_sem): (Sender<()>, Receiver<()>) = async_channel::bounded(self.max_workers);

        let handles = (0..self.max_workers)
            .map(|_| {
                let rx = rx.clone();
                let rx_rem = rx_sem.clone();
                task::spawn(self.worker(rx_rem, rx))
            })
            .collect::<Vec<_>>();

        task::spawn(self.poller(cancel, tx_sem, tx))
            .await
            .expect("spawned task failure")?;

        for handle in handles {
            handle.await?;
        }
        Ok(())
    }

    fn poller(
        &self,
        cancel: impl Future<Output = ()> + Send + 'static,
        tx_sem: Sender<()>,
        tx: Sender<Job<P, S>>,
    ) -> impl Future<Output = Result<(), anyhow::Error>> {
        let client = Arc::clone(&self.client);
        let queue = self.queue.clone();

        async move {
            tokio::pin!(cancel);

            let mut num_jobs = 0;

            'outer: loop {
                if num_jobs == 0 {
                    tx_sem.send(()).await?;
                    num_jobs += 1;
                }
                while let Ok(_) = tx_sem.try_send(()) {
                    num_jobs += 1;
                }

                let jobs = select! {
                  _ = &mut cancel => {
                        break 'outer;
                    },
                    res = client.poll::<P, S>(&queue, num_jobs) => res?
                };
                let l = jobs.len();
                for job in jobs {
                    tx.send(job).await?;
                }
                num_jobs = num_jobs - l;
            }
            // automatically gets dropped?
            // drop(tx);
            // drop(tx_sem);
            Ok(())
        }
    }

    fn worker(&self, rx_sem: Receiver<()>, rx: Receiver<Job<P, S>>) -> impl Future<Output = ()> {
        let client = Arc::clone(&self.client);
        let worker = Arc::clone(&self.worker);
        async move {
            while let Ok(job) = rx.recv().await {
                worker.run(&client, job).await;
                rx_sem
                    .recv()
                    .await
                    .expect("semaphore shutdown in correct order");
            }
        }
    }
}
