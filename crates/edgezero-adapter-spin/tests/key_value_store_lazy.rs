#![cfg(not(target_arch = "wasm32"))]

extern crate self as spin_sdk;

mod key_value {
    use std::cell::Cell;
    use std::fmt;
    use std::future::{pending, ready};

    thread_local! {
        pub static OPENS: Cell<usize> = const { Cell::new(0) };
        pub static MODE: Cell<u8> = const { Cell::new(0) };
    }

    #[derive(Debug)]
    pub enum Error {
        AccessDenied,
        NoSuchStore,
        Other(String),
        StoreTableFull,
    }

    impl fmt::Display for Error {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::Other(message) => f.write_str(message),
                Self::AccessDenied => f.write_str("access denied"),
                Self::NoSuchStore => f.write_str("no such store"),
                Self::StoreTableFull => f.write_str("store table full"),
            }
        }
    }

    pub struct Store;
    impl Store {
        pub async fn delete(&self, _key: &str) -> Result<(), Error> {
            ready(Ok(())).await
        }
        pub async fn exists(&self, _key: &str) -> Result<bool, Error> {
            ready(Ok(true)).await
        }
        pub async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, Error> {
            if key == "pending" {
                pending().await
            } else {
                Ok(Some(b"value".to_vec()))
            }
        }
        pub async fn get_keys(&self) -> Keys {
            ready(Keys).await
        }
        pub async fn open(_label: &str) -> Result<Self, Error> {
            OPENS.set(OPENS.get().saturating_add(1));
            match MODE.get() {
                0 => Ok(Self),
                1 => Err(Error::NoSuchStore),
                2 => Err(Error::AccessDenied),
                3 => Err(Error::StoreTableFull),
                4 => Err(Error::Other("credential-bearing-provider-token".to_owned())),
                _ => pending().await,
            }
        }
        pub async fn set(&self, _key: &str, _value: &[u8]) -> Result<(), Error> {
            ready(Ok(())).await
        }
    }
    pub struct Keys;
    impl Keys {
        pub async fn collect(self) -> Result<Vec<String>, Error> {
            ready(Ok(vec!["key".to_owned()])).await
        }
    }
}

#[path = "../src/key_value_store.rs"]
mod key_value_store;
#[path = "../src/kv_pagination.rs"]
mod kv_pagination;

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use edgezero_core::key_value_store::{KvError, KvStore as _, KvStoreOpenFailure};
    use futures::{FutureExt as _, executor::block_on};
    use std::time::Duration;

    use super::{key_value, key_value_store::SpinKvStore};

    #[test]
    fn declared_kv_async_open_is_request_local_and_shared() {
        let first = SpinKvStore::declared("declared", "physical".to_owned(), 100);
        assert_eq!(key_value::OPENS.get(), 0);
        let mut read = Box::pin(first.get_bytes("pending"));
        assert!(read.as_mut().now_or_never().is_none());
        assert!(block_on(first.exists("key")).unwrap());
        assert_eq!(
            key_value::OPENS.get(),
            1,
            "operation must not retain initialization lock"
        );
        drop(read);
        let second = SpinKvStore::declared("declared", "physical".to_owned(), 100);
        assert!(block_on(second.exists("key")).unwrap());
        assert_eq!(key_value::OPENS.get(), 2);
    }

    #[test]
    fn declared_kv_open_failure_retains_binding_and_recovers() {
        for (mode, expected) in [
            (1, KvStoreOpenFailure::MissingBinding),
            (2, KvStoreOpenFailure::InvalidConfiguration),
            (3, KvStoreOpenFailure::Unavailable),
            (4, KvStoreOpenFailure::Unavailable),
        ] {
            key_value::MODE.set(mode);
            let store = SpinKvStore::declared("failed", "provider-name-token".to_owned(), 100);
            let before = key_value::OPENS.get();
            let failures = block_on(async {
                vec![
                    store.get_bytes("key").await.err(),
                    store.exists("key").await.err(),
                    store.put_bytes("key", Bytes::new()).await.err(),
                    store
                        .put_bytes_with_ttl("key", Bytes::new(), Duration::from_secs(60))
                        .await
                        .err(),
                    store.delete("key").await.err(),
                    store.list_keys_page("", None, 1).await.err(),
                ]
            });
            for failure in failures {
                let error = failure.expect("declared open failure");
                assert!(matches!(error, KvError::OpenFailed { reason } if reason == expected));
                assert!(!error.to_string().contains("token"));
            }
            assert_eq!(key_value::OPENS.get(), before + 1);
        }
        key_value::MODE.set(0);
        let restored = SpinKvStore::declared("failed", "provider-name-token".to_owned(), 100);
        assert_eq!(
            block_on(restored.get_bytes("key")).unwrap(),
            Some(Bytes::from_static(b"value"))
        );
        assert!(matches!(
            block_on(restored.put_bytes_with_ttl("key", Bytes::new(), Duration::from_secs(60))),
            Err(KvError::Unsupported { .. })
        ));
        let _store = block_on(SpinKvStore::open("physical")).expect("explicit open");
    }

    #[test]
    fn cancelled_initializer_and_waiter_do_not_poison_request() {
        key_value::MODE.set(5);
        let store = SpinKvStore::declared("pending", "physical".to_owned(), 100);
        let mut first = Box::pin(store.exists("key"));
        assert!(first.as_mut().now_or_never().is_none());
        let mut waiter = Box::pin(store.exists("key"));
        assert!(waiter.as_mut().now_or_never().is_none());
        assert_eq!(key_value::OPENS.get(), 1);
        drop(waiter);
        drop(first);
        key_value::MODE.set(0);
        assert!(block_on(store.exists("key")).unwrap());
        assert_eq!(key_value::OPENS.get(), 2);
    }
}
