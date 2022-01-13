use self::common::*;
use crate::{
    constant::*,
    monitor::client::{self, Client as MonitorClient},
    rpc::{Get, KvRequest, Put},
};
pub use common::{create_monitor, gen_random_put, Client, KvServerCluster};
use futures::stream::{self, FuturesUnordered, StreamExt};
use log::{info, warn};
use madsim::time::sleep;
use std::{collections::HashMap, time::Duration};

pub mod common;

#[madsim::test]
async fn cluster_simple_test() {
    use self::common::*;
    use crate::constant::*;

    const SERVER_NUM: usize = 10;

    let _monitor = create_monitor(
        PG_NUM,
        (0..SERVER_NUM).map(|id| gen_server_addr(id)).collect(),
    )
    .await;
    let _cluster = KvServerCluster::new(SERVER_NUM).await;
    let client = Client::new(
        0,
        MonitorClient::new(str_to_addr(MONITOR_ADDR)).await.unwrap(),
    );

    // Wait for the cluster to start up (peering)
    sleep(Duration::from_millis(5000)).await;

    for _ in 0..1000 {
        let (key, value) = gen_random_put(10, 20);
        let request = Put { key, value };

        let res = client.send(request.clone(), None).await;
        assert!(matches!(res, Ok(Ok(_))));

        let targets = client.get_target_addrs(request.key().as_bytes()).await;
        for target in targets {
            let key = request.key().to_owned();
            let res = client.send_to(Get(key), target, None).await;
            assert_eq!(res.unwrap().unwrap().unwrap(), request.value().unwrap());
        }
    }
}

#[madsim::test]
async fn one_server_crash() {
    const SERVER_NUM: usize = 10;
    const CRASH_TARGET_IDX: usize = 0;

    let _monitor = create_monitor(
        PG_NUM,
        (0..SERVER_NUM).map(|id| gen_server_addr(id)).collect(),
    )
    .await;
    let cluster = KvServerCluster::new(SERVER_NUM).await;
    let client = Client::new(
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
    // Server 0 is crashed
    cluster.crash(CRASH_TARGET_IDX);
    warn!("Server crash!");

    // Make sure all keys is unavaiable right after the server is crash
    let mut tasks = keys
        .iter()
        .map(|key| {
            let client = client.clone();
            let (_, value) = gen_random_put(5, 10);
            golden
                .entry(key.to_string())
                .and_modify(|val| val.push(value.clone()));
            async move {
                let request = Put {
                    key: key.to_string(),
                    value: value.clone(),
                };
                let res = client.send(request, None).await;
                info!("Send Put after server crash, response : {:?}", res);
                assert!(matches!(res, Ok(Err(_)) | Err(_)));
            }
        })
        .collect::<FuturesUnordered<_>>();
    while let Some(_) = tasks.next().await {}

    // wait for system to recover
    sleep(Duration::from_millis(25_000)).await;
    client.update_target_map().await;

    for (key, value) in golden.iter() {
        client.check_consistency(key, value).await;
    }
}

#[madsim::test]
async fn crash_and_up() {
    const SERVER_NUM: usize = 10;
    const CRASH_TARGET_IDX: usize = 0;

    let _monitor = create_monitor(
        PG_NUM,
        (0..SERVER_NUM).map(|id| gen_server_addr(id)).collect(),
    )
    .await;
    let cluster = KvServerCluster::new(SERVER_NUM).await;
    let client = Client::new(
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
    // Do not know the reason: After restart, the first packet will be lost and cause timeout
    let res = client
        .send_to(
            Get("dummy".to_owned()),
            gen_server_addr(CRASH_TARGET_IDX),
            None,
        )
        .await;
    assert!(
        res.is_err(),
        "First packet after restart, response = {:?}",
        res
    );

    client.update_target_map().await;
    for (key, value) in golden.iter() {
        client.check_consistency(key, value).await;
    }
}
