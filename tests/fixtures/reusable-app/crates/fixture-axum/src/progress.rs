use edgezero_core::{EdgeError, app::Hooks, router::RouterService};
use fixture_core::qualification::progress::{self, Job, ProbeRuntime};
use std::{future::Future, pin::Pin, sync::Arc, time::Duration};

pub struct NativeProgressApp;
pub struct NativeRuntime;

struct AbortOnDrop(tokio::task::AbortHandle);
impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

fn spawn_owned(
    runtime: tokio::runtime::Handle,
    worker: impl Future<Output = Result<u64, EdgeError>> + Send + 'static,
) -> Job {
    let task = runtime.spawn(worker);
    let guard = AbortOnDrop(task.abort_handle());
    Box::pin(async move {
        let _guard = guard;
        task.await.map_err(|_| progress::probe_failure())?
    })
}

impl Hooks for NativeProgressApp {
    fn routes() -> RouterService {
        progress::router(
            Arc::new(NativeRuntime),
            serde_json::json!({"source":"not-exposed-by-native-entrypoint","known_empty":null}),
            None,
        )
    }
}

async fn fetch(target: String) -> Result<u64, EdgeError> {
    let response = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(2))
        .build()
        .map_err(|_| progress::probe_failure())?
        .get(target)
        .send()
        .await
        .map_err(|_| progress::probe_failure())?;
    if !response.status().is_success() {
        return Err(progress::probe_failure());
    }
    let bytes = response
        .bytes()
        .await
        .map_err(|_| progress::probe_failure())?;
    if bytes.len() > 64 {
        return Err(progress::probe_failure());
    }
    Ok(bytes.len() as u64)
}

impl ProbeRuntime for NativeRuntime {
    fn adapter(&self) -> &'static str {
        "axum"
    }
    fn perform(&self, target: String) -> Job {
        Box::pin(fetch(target))
    }
    fn start(&self, target: String) -> Result<Job, EdgeError> {
        let runtime =
            tokio::runtime::Handle::try_current().map_err(|_| progress::probe_failure())?;
        Ok(spawn_owned(runtime, fetch(target)))
    }
    fn pause(&self, duration: Duration) -> Pin<Box<dyn Future<Output = ()>>> {
        Box::pin(async move {
            tokio::time::sleep(duration).await;
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fixture_core::qualification::progress::ProbeRuntime;

    #[test]
    fn unpolled_consumer_drop_aborts_the_worker_before_runtime_shutdown() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        struct Released(Arc<AtomicUsize>);
        impl Drop for Released {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        runtime.block_on(async {
            for before_first_poll in [true, false] {
                let releases = Arc::new(AtomicUsize::new(0));
                let polls = Arc::new(AtomicUsize::new(0));
                let (released, started) = (Released(Arc::clone(&releases)), Arc::clone(&polls));
                let job = spawn_owned(tokio::runtime::Handle::current(), async move {
                    let _released = released;
                    started.fetch_add(1, Ordering::SeqCst);
                    std::future::pending::<Result<u64, EdgeError>>().await
                });
                if before_first_poll {
                    assert_eq!(polls.load(Ordering::SeqCst), 0);
                } else {
                    tokio::time::timeout(Duration::from_secs(1), async {
                        while polls.load(Ordering::SeqCst) == 0 {
                            tokio::task::yield_now().await;
                        }
                    })
                    .await
                    .unwrap();
                }
                drop(job);
                tokio::time::timeout(Duration::from_secs(1), async {
                    while releases.load(Ordering::SeqCst) == 0 {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .unwrap();
                assert_eq!(releases.load(Ordering::SeqCst), 1);
                tokio::task::yield_now().await;
                assert_eq!(releases.load(Ordering::SeqCst), 1);
            }
        });
    }

    #[test]
    fn progress_start_rejects_absent_runtime_without_spawning() {
        assert!(
            NativeRuntime
                .start("http://127.0.0.1:1/g3/test/slot-0".into())
                .is_err()
        );
    }
}
