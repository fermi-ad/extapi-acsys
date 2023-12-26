use std::path::Path;
use warp::Filter;

pub mod acsys;

// Starts the web server that receives GraphQL queries. The
// configuration of the server is pulled together by obtaining
// configuration information from the submodules. All accesses are
// wrapped with CORS support from the `warp` crate.

pub async fn start_service() {
    let filter = acsys::filter("acsys").with(
        warp::cors()
            .allow_any_origin()
            .allow_headers(vec![
                "content-type",
                "Access-Control-Allow-Origin",
                "Sec-WebSocket-Protocol",
            ])
            .allow_methods(vec!["OPTIONS", "GET", "POST"]),
    );

    warp::serve(filter)
        .tls()
        .cert_path(Path::new("/etc/ssl/private/acsys-proxy.fnal.gov/cert.pem"))
        .key_path(Path::new("/etc/ssl/private/acsys-proxy.fnal.gov/key.pem"))
        .run(([0, 0, 0, 0], 8000))
        .await;
}
