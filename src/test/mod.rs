use self::common::*;
use crate::{
    constant::*,
    monitor::client::Client as MonitorClient,
    rpc::{Get, KvRequest, Put},
};
pub use common::{create_monitor, gen_random_put, Client, KvServerCluster};
use futures::stream::{FuturesUnordered, StreamExt};
use log::{info, warn};
use madsim::time::sleep;
use std::{collections::HashMap, time::Duration};

pub mod common;

#[madsim::test]
async fn cluster_simple_test() {
    use self::common::*;
    use crate::constant::*;

    const SERVER_NUM: usize = 10;
    const PG_NUM: usize = 256;

    let _monitor = create_monitor(
        PG_NUM,
        (0..SERVER_NUM).map(|id| gen_server_addr(id)).collect(),
    )
    .await;
    let _cluster = KvServerCluster::new(PG_NUM, SERVER_NUM).await;
    let client = Client::new(
        PG_NUM,
        0,
        MonitorClient::new(str_to_addr(MONITOR_ADDR)).await.unwrap(),
    );

    // Wait for the cluster to start up (peering)
    sleep(Duration::from_secs(5)).await;
    let mut golden = HashMap::new();

    for _ in 0..1000 {
        let (key, value) = gen_random_put(10, 20);
        golden.insert(key.clone(), value.clone());
        let request = Put { key, value };

        let res = client.send(request.clone(), None).await;
        assert!(matches!(res, Ok(Ok(_))));
    }

    warn!("Send put done. Start check consistency...");

    for (key, value) in golden.into_iter() {
        client.check_consistency(&key, &[value]).await;
    }
}

#[madsim::test]
async fn one_pg_crash_and_up() {
    const SERVER_NUM: usize = 5;
    const CRASH_TARGET_IDX: usize = 0;
    const PG_NUM: usize = 1;

    let _monitor = create_monitor(
        PG_NUM,
        (0..SERVER_NUM).map(|id| gen_server_addr(id)).collect(),
    )
    .await;
    let cluster = KvServerCluster::new(PG_NUM, SERVER_NUM).await;
    let client = Client::new(
        PG_NUM,
        0,
        MonitorClient::new(str_to_addr(MONITOR_ADDR)).await.unwrap(),
    );

    let mut golden = HashMap::new();

    // Wait for the cluster to start up (peering)
    sleep(Duration::from_millis(5000)).await;

    // Put some random keys
    for _ in 0..500 {
        let (key, value) = gen_random_put(5, 10);
        golden.insert(key.clone(), vec![value.clone()]);
        let request = Put { key, value };

        let res = client.send(request.clone(), None).await;
        assert!(matches!(res, Ok(Ok(_))));
    }

    // Find the keys which are stored on CRASH server
    let mut keys = Vec::new();
    for key in golden.keys() {
        if client
            .get_target_addrs(key.as_bytes())
            .await
            .iter()
            .any(|addr| *addr == gen_server_addr(CRASH_TARGET_IDX))
        {
            keys.push(key.to_owned());
        }
    }

    cluster.crash(CRASH_TARGET_IDX);
    warn!("Server crash!");
    sleep(Duration::from_secs(25)).await;
    client.update_target_map().await;

    let (key, value) = gen_random_put(5, 10);
    warn!("random gen {:?}", gen_random_put(5, 10));
    let request = Put {
        key: key,
        value: value,
    };
    let res = client.send(request, None).await;
    assert!(matches!(res, Ok(Err(_)) | Err(_)), "send put get {:?}", res);

    // Make sure all keys is unavaiable right after the server is crash
    let mut tasks = FuturesUnordered::new();
    for key in golden.keys() {
        let target = client.get_target_addrs(key.as_bytes()).await;
        target.into_iter().for_each(|target_addr| {
            let c = client.clone();
            let request = Get(key.clone());
            tasks.push(async move { c.send_to(request, target_addr, None).await })
        });
    }
    while let Some(res) = tasks.next().await {
        assert!(matches!(res, Ok(Err(_))))
    }

    cluster.restart(CRASH_TARGET_IDX).await;
    warn!("Server restart!");
    sleep(Duration::from_secs(25)).await;
    client.update_target_map().await;

    for (key, value) in golden.iter() {
        client.check_consistency(key, value).await;
    }
}

// #[madsim::test]
async fn crash_and_up() {
    const SERVER_NUM: usize = 10;
    const CRASH_TARGET_IDX: usize = 0;
    const PG_NUM: usize = 256;

    let _monitor = create_monitor(
        PG_NUM,
        (0..SERVER_NUM).map(|id| gen_server_addr(id)).collect(),
    )
    .await;
    let cluster = KvServerCluster::new(PG_NUM, SERVER_NUM).await;
    let client = Client::new(
        PG_NUM,
        0,
        MonitorClient::new(str_to_addr(MONITOR_ADDR)).await.unwrap(),
    );

    let mut golden = HashMap::new();

    // Wait for the cluster to start up (peering)
    sleep(Duration::from_millis(5000)).await;

    // Put some random keys
    for _ in 0..3000 {
        let (key, value) = gen_random_put(5, 10);
        golden.insert(key.clone(), vec![value.clone()]);
        let request = Put { key, value };

        let res = client.send(request.clone(), None).await;
        assert!(
            matches!(res, Ok(Ok(_))),
            "Send put {} failed: {:?}",
            request.key(),
            res
        );
    }

    // Server 0 is crashed and then restart
    cluster.crash(CRASH_TARGET_IDX);
    warn!("Server crash!");
    sleep(Duration::from_millis(25_000)).await;
    cluster.restart(CRASH_TARGET_IDX).await;
    warn!("Server restart!");
    sleep(Duration::from_millis(15_000)).await;
    client.update_target_map().await;
    for (key, value) in golden.iter() {
        client.check_consistency(key, value).await;
    }
}
