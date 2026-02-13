use crate::env_var;

use std::future::Future;

use tonic::{Response, Status, transport::Error};
use tracing::error;

pub mod groups;
pub mod layouts;
pub mod timers;

const GRPC_ALARMS_DB_HOST: &str = "GRPC_ALARMS_DB_HOST";

fn get_alarms_db_host() -> String {
    env_var::expect(GRPC_ALARMS_DB_HOST)
}

async fn establish_connection<Producer, ClientFut, Client>(
    produce_client: Producer,
) -> Result<Client, Status>
where
    Producer: Fn(String) -> ClientFut,
    ClientFut: Future<Output = Result<Client, Error>>,
{
    produce_client(get_alarms_db_host()).await
        .map_err(|e| {
            error!("Failed to connect to grpc-alarms-db: {e:?}");
            Status::internal("Could not connect to the database service. See server logs for details.")
        })
}

async fn execute_with_client<
    Producer,
    ClientFut,
    Client,
    ClientFn,
    ResponseFut,
    R,
>(
    produce_client: Producer, execute_with: ClientFn,
) -> Result<R, Status>
where
    Producer: Fn(String) -> ClientFut,
    ClientFut: Future<Output = Result<Client, Error>>,
    ClientFn: FnOnce(Client) -> ResponseFut,
    ResponseFut: Future<Output = Result<Response<R>, Status>>,
{
    let client = establish_connection(produce_client).await?;
    let response = execute_with(client).await?;
    Ok(response.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn execute_with_client_returns_result() {
        let expected_host = get_alarms_db_host();
        let host_ref = &expected_host;
        let result = execute_with_client(
            |input| async move {
                assert_eq!(&input, host_ref);
                Ok(true)
            },
            |val| async move { Ok(Response::new(val)) },
        )
        .await;
        assert!(result.unwrap());
    }
}
