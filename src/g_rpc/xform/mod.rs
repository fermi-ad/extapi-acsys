use proto::x_form_api_client::XFormApiClient;

pub mod proto {
    tonic::include_proto!("fnal.xform");
}

pub async fn activate_expression(
    event: String, op: Box<proto::Operation>,
) -> Result<tonic::Response<tonic::Streaming<proto::ExprResult>>, tonic::Status>
{
    match XFormApiClient::connect("http://clx76.fnal.gov:6803/").await {
        Ok(mut client) => {
            let req = proto::Expr {
                op: Some(*op),
                event,
            };

            client.activate_expression(req).await
        }
        Err(_) => Err(tonic::Status::unavailable("XForm service unavailable")),
    }
}
