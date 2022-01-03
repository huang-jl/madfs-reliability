use std::{net::SocketAddr, str::FromStr, sync::Arc, time::Duration};

use super::{client::ServerClient, Monitor};

const MONITOR_ADDR: &str = "10.0.0.0:8000";

async fn create_server_client(id: usize) -> Arc<ServerClient> {
    let handle = madsim::Handle::current();
    let host = handle
        .create_host(format!("0.0.1.{}:0", id).parse::<SocketAddr>().unwrap())
        .unwrap();
    handle.net.connect(host.local_addr());
    host.spawn(async move {
        ServerClient::new(id as _, MONITOR_ADDR.parse().unwrap())
            .await
            .unwrap()
    })
    .await
}

#[madsim::test]
async fn simple_test() {
    const SERVER_NUM: usize = 10;
    const PG_NUM: usize = 256;
    let _monitor = {
        let handle = madsim::Handle::current();
        let host = handle
            .create_host(MONITOR_ADDR.parse::<SocketAddr>().unwrap())
            .unwrap();
        handle.net.connect(host.local_addr());
        host.spawn(async move {
            Monitor::new(
                PG_NUM,
                (0..SERVER_NUM)
                    .map(|id| format!("0.0.1.{}:0", id).parse().unwrap())
                    .collect(),
            )
        })
        .await
    };
    let server_clients = {
        let mut res = Vec::new();
        for id in 0..SERVER_NUM {
            res.push(create_server_client(id).await);
        }
        res
    };
    madsim::time::sleep(Duration::from_millis(50_000)).await;
    let target_map = server_clients[0].fetch_target_map(None).await.unwrap();
    let local_target_map = server_clients[0].get_local_target_map().await;
    for (info, local_info) in target_map.map.iter().zip(local_target_map.map.iter()) {
        assert_eq!(info.state, local_info.state);
        assert!(info.is_active());
    }
}
