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
use std::collections::HashMap;

pub mod common;

#[madsim::test]
async fn cluster_simple_test() {
    use self::common::*;
    use crate::constant::*;

    const SERVER_NUM: usize = 10;

    let _monitor = create_monitor(
        PG_NUM,
        MONITOR_ADDR,
        (0..SERVER_NUM).map(|id| gen_server_addr(id)).collect(),
    )
    .await;
    let _cluster = KvServerCluster::new(SERVER_NUM, MONITOR_ADDR).await;
    let client = Client::new(
        0,
        MonitorClient::new(str_to_addr(MONITOR_ADDR)).await.unwrap(),
    );

    // Wait for the cluster to start up (peering)
    sleep(Duration::from_millis(5000)).await;

    for _ in 0..100 {
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
        MONITOR_ADDR,
        (0..SERVER_NUM).map(|id| gen_server_addr(id)).collect(),
    )
    .await;
    let cluster = KvServerCluster::new(SERVER_NUM, MONITOR_ADDR).await;
    let client = Client::new(
        0,
        MonitorClient::new(str_to_addr(MONITOR_ADDR)).await.unwrap(),
    );

    let mut golden = HashMap::new();

    // Wait for the cluster to start up (peering)
    sleep(Duration::from_millis(5000)).await;

    for _ in 0..100 {
        let (key, value) = gen_random_put(10, 20);
        golden.insert(key.clone(), vec![value.clone()]);
        let request = Put { key, value };

        let res = client.send(request.clone(), None).await;
        assert!(matches!(res, Ok(Ok(_))));
    }

    let mut keys = Vec::new();
    let crash_addr = cluster.get_addrs()[CRASH_TARGET_IDX];
    for (key, _) in golden.iter() {
        if client
            .get_target_addrs(key.as_bytes())
            .await
            .iter()
            .any(|addr| *addr == crash_addr)
        {
            keys.push(key.clone());
        }
    }
    // Server 0 is crashed
    cluster.crash(CRASH_TARGET_IDX);
    warn!("Server crash!");

    let mut tasks = keys
        .iter()
        .map(|key| {
            let client = client.clone();
            let (_, value) = gen_random_put(5, 20);
            golden
                .entry(key.to_string())
                .and_modify(|val| val.push(value.clone()));
            async move {
                let request = Put {
                    key: key.clone(),
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
    client.monitor_client.update_target_map().await;

    // Here we check that the data is consistent among all replicas and
    // check the final state is some value we put before instead of some damaged or weird data.
    for (key, value) in golden.iter() {
        let targets = client.get_target_addrs(key.as_bytes()).await;
        let mut target_vals = Vec::new();
        for target in targets {
            let res = client.send_to(Get(key.clone()), target, None).await;
            let res = res.unwrap().unwrap().unwrap();
            target_vals.push(res);
        }
        assert!(target_vals
            .iter()
            .zip(target_vals.iter().skip(1))
            .all(|(a, b)| a == b));
        assert!(target_vals.iter().all(|val| value.iter().any(|v| v == val)));
    }
}
