use crate::g_rpc::proto::services::alarms::{
    user_layouts_service_client::UserLayoutsServiceClient, UserLayouts,
};

use tonic::{Request, Status};

pub async fn read_layouts(request: Request<()>) -> Result<UserLayouts, Status> {
    super::execute_with_client(
        UserLayoutsServiceClient::connect,
        |mut client| async move { client.get_user_layouts(request).await },
    )
    .await
}
