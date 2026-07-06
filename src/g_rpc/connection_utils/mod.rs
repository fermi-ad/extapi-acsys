//! Connection Utility Module
//!
//! Describes structures to manage a gRPC service connection. It enables sharing a multiplexed HTTPS connection when several
//! threads want the same remote service at the same time.

use rust_env_var_lib::env_var;
use tokio::sync::RwLock;
use tonic::{Response, Status, transport::Error};
use tracing::error;
use uuid::Uuid;

/// The trait to be implemented by wrapped objects within [`ConnectionPort`]. The idea is this will house an inner gRPC client
/// backed by [`tonic::transport::Channel`]. `Channel` offers a lightweight implementation of [`Clone`] that multiplexes an HTTPS connection
/// to the host server. This allows multiple threads to share the connection simultaneously.
pub trait ConnectionAdapter: Clone + Sized {
    /// Creates an instance of the adapter.
    ///
    /// This function is `async` to allow the connection to be made as part of the initialization process.
    /// The return value is a [`Result`] to allow an [`Error`] to propagate to the calling code.
    fn new(host: String) -> impl Future<Output = Result<Self, Error>>;
}

/// A structure to hold a lock on the inner [`ConnectionAdapter`] and safely run several requests to the same remote host at once.
pub struct ConnectionPort<T: ConnectionAdapter> {
    connection: RwLock<Option<T>>,
    host_var: &'static str,
}
impl<T: ConnectionAdapter> ConnectionPort<T> {
    /// A lightweight constructor to capture the name of the environment variable
    /// where the remote host name is stored. Initializes with an empty [`RwLock`] for speed and
    /// consistency. Will attempt to make the connection on the first call to [`run_with_client`](Self::run_with_client).
    pub fn new(host_var: &'static str) -> Self {
        ConnectionPort {
            connection: RwLock::new(None),
            host_var,
        }
    }

    /// Executes the provided [`Action`](AsyncFnOnce) by passing it the wrapped [`ConnectionAdapter`]. Handles making the connection to the
    /// remote host and managing if the connection goes bad. Returns an error that is safe to hand back to clients.
    pub async fn run_with_client<Action, R>(
        &self, action: Action,
    ) -> Result<R, Status>
    where
        Action: AsyncFnOnce(T) -> Result<Response<R>, Status>,
    {
        let client = self.get_connection().await?;
        match action(client).await {
            Ok(response) => Ok(response.into_inner()),
            Err(e) => {
                let err_token = Uuid::new_v4().as_hyphenated().to_string();
                error!(
                    "{} Error returned from call to {}: {}",
                    err_token, self.host_var, e
                );
                // Drop existing connection to force a fresh one to be made on the next call.
                let _ = self.connection.write().await.take();
                Err(Status::internal(format!(
                    "See server logs for details; reference token {err_token}"
                )))
            }
        }
    }

    /// The logic to handle acquiring a connection in a thread-safe way.
    ///
    /// First, it attempts to reuse an existing connection by calling `read` on the [`RwLock`]. Any threads attempting
    /// to access the resource at the same time will be allowed concurrent read access. A clone of the [`ConnectionAdapter`] is returned
    /// so that each caller can run in parallel, if needed.
    ///
    /// If there is no existing connection, this method attempts to acquire a `write` lock. Once a lock is acquired, another check is done
    /// to see if some other process created a connection while we were waiting for the write lock. If so, we just use a clone of that connection.
    /// Otherwise, we attempt to establish a connection and store it in the lock, returning a clone for use by the calling process.
    async fn get_connection(&self) -> Result<T, Status> {
        let naive_check = self.connection.read().await.as_ref().cloned();
        match naive_check {
            Some(conn) => Ok(conn),
            None => {
                let mut lock = self.connection.write().await;
                match lock.as_ref() {
                    Some(conn) => {
                        // Another process established the connection while we were waiting for the write lock.
                        // Return the adapter.
                        Ok(conn.clone())
                    }
                    None => match self.establish_connection().await {
                        Ok(conn) => {
                            *lock = Some(conn.clone());
                            Ok(conn)
                        }
                        Err(err_token) => Err(Status::internal(format!(
                            "See server logs for details; reference token {err_token}"
                        ))),
                    },
                }
            }
        }
    }

    /// The logic to actually make the connection to the remote host.
    ///
    /// Reads the value of [`Self::host_var`] from the environment and passes it to the [`ConnectionAdapter::new`] method for `T`.
    async fn establish_connection(&self) -> Result<T, String> {
        match env_var::get(self.host_var).to_option::<String>() {
            Some(host) => T::new(host.clone()).await.map_err(|e| {
                let err_token = Uuid::new_v4().as_hyphenated().to_string();
                error!("{} Failed to connect to {}: {:?}", err_token, host, e);
                err_token
            }),
            None => {
                let err_token = Uuid::new_v4().as_hyphenated().to_string();
                error!("{} No value set for {}", err_token, self.host_var);
                Err(err_token)
            }
        }
    }
}
