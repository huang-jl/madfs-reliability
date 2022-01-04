use crate::{
    monitor::client::Client as MonitorClient,
    service::{KvArgs, KvRes},
};
pub use common::{create_monitor, gen_random_put, Client, KvServerCluster};

pub mod common;

#[madsim::test]
async fn cluster_simple_test() {
    use self::common::*;

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

    let request = gen_random_put(10, 20);
    let res = client.send(request.clone(), None).await;
    assert!(matches!(res, Ok(KvRes::Put)));

    let targets = client.get_target_addrs(&request).await;
    for target in targets {
        let key = request.get_key();
        let res = client.send_to(KvArgs::Get(key), target, None).await;
        assert_eq!(
            res.unwrap().get_value().unwrap(),
            request.get_value().unwrap()
        );
    }
}
