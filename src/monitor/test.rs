use super::client::ServerClient;
use crate::constant::*;
use crate::test::{common::*, create_monitor};
use std::sync::Arc;

async fn create_server_client(id: usize, monitor_addr: &str) -> Arc<ServerClient> {
    let monitor_addr = str_to_addr(monitor_addr);
    let handle = madsim::Handle::current();
    let host = handle.create_host(gen_server_addr(id)).unwrap();
    handle.net.connect(host.local_addr());
    host.spawn(async move { ServerClient::new(id as _, monitor_addr).await.unwrap() })
        .await
}

#[madsim::test]
async fn monitor_simple_test() {
    const SERVER_NUM: usize = 10;
    const PG_NUM: usize = 256;

    let monitor = create_monitor(
        PG_NUM,
        MONITOR_ADDR,
        (0..SERVER_NUM).map(|id| gen_server_addr(id)).collect(),
    )
    .await;
    let server_clients = {
        let mut res = Vec::new();
        for id in 0..SERVER_NUM {
            res.push(create_server_client(id, MONITOR_ADDR).await);
        }
        res
    };
    // Wait 50 seconds to see whether server clients report to monitor correctly by heartbeat
    madsim::time::sleep(Duration::from_millis(50_000)).await;
    let monitor_target_map = monitor.inner.lock().unwrap().get_target_map(None).unwrap();
    for server_client in server_clients.iter() {
        let target_map = server_client.fetch_target_map(None).await.unwrap();
        let local_target_map = server_client.get_local_target_map().await;
        for ((info, local_info), monitor_info) in target_map
            .map
            .iter()
            .zip(local_target_map.map.iter())
            .zip(monitor_target_map.map.iter())
        {
            assert_eq!(info.state, local_info.state);
            assert_eq!(info.state, monitor_info.state);
            assert!(info.is_active());
        }
    }
}
