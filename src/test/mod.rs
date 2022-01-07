use crate::{
    monitor::client::Client as MonitorClient,
    rpc::{Get, KvRequest, Put},
};
pub use common::{create_monitor, gen_random_put, Client, KvServerCluster};

pub mod common;

#[madsim::test]
async fn cluster_simple_test() {
    use self::common::*;
    use crate::constant::*;

    const PG_NUM: usize = 256;
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
