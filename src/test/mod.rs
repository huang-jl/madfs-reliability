use self::common::*;
use crate::{
    constant::*,
    monitor::client::Client as MonitorClient,
    rpc::{Get, KvRequest, Put},
    Error,
};
pub use common::{create_monitor, gen_random_put, Client, KvServerCluster};
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
        MONITOR_ADDR,
        (0..SERVER_NUM).map(|id| gen_server_addr(id)).collect(),
    )
    .await;
    let _cluster = KvServerCluster::new(SERVER_NUM, MONITOR_ADDR).await;
    let client = Client::new(
        0,
        MonitorClient::new(str_to_addr(MONITOR_ADDR)).await.unwrap(),
    );

    // Wait for the cluster to start up
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

async fn one_server_crash() {
    todo!("Test the crash situation");
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

    for _ in 0..100 {
        let (key, value) = gen_random_put(10, 20);
        golden.insert(key.clone(), value.clone());
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
            .find(|addr| **addr == crash_addr)
            .is_some()
        {
            keys.push(key.clone());
        }
    }
    // Server 0 is crashed
    cluster.crash(CRASH_TARGET_IDX);

    for key in keys.iter() {
        let request = Get(key.clone());
        let res = client.send(request, None).await;
        assert!(matches!(res, Ok(Err(Error::PgIsRecovering))));
    }

    sleep(Duration::from_millis(20_000)).await;

    for (key, value) in golden.iter() {
        let targets = client.get_target_addrs(key.as_bytes()).await;
        for target in targets {
            let res = client.send_to(Get(key.clone()), target, None).await;
            assert_eq!(&res.unwrap().unwrap().unwrap(), value);
        }
    }
}
