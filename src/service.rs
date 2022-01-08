use crate::PgId;
use log::info;
use madsim::net::rpc::Serialize;
use serde::Deserialize;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fmt::Debug;

/// What we need is a key/value Store
pub trait Store {
    fn put(&mut self, key: String, value: Vec<u8>);
    fn get(&self, key: &str) -> Option<Vec<u8>>;
    fn get_pg_data(&self, pgid: PgId) -> Vec<u8>;
    fn push_pg_data(&mut self, pgid: PgId, data: Vec<u8>);

    fn get_key_version(&self, key: &str) -> Option<u64>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Value {
    data: Vec<u8>,
    version: u64, //version of this key
}

pub struct KvService {
    kv: BTreeMap<String, Value>,
}

impl Store for KvService {
    fn put(&mut self, key: String, data: Vec<u8>) {
        info!("Put key: {:?}", key);
        self.kv
            .entry(key)
            .and_modify(|value| {
                value.data = data.clone();
                value.version += 1;
            })
            .or_insert(Value { data, version: 0 });
    }

    fn get(&self, key: &str) -> Option<Vec<u8>> {
        let res = self.kv.get(key).cloned().map(|v| v.data);
        info!(
            "Get key: {:?}, {}",
            key,
            match &res {
                Some(_) => "Get the value",
                None => "value does not exist",
            }
        );
        res
    }

    /// Will return the bytes representation of a BtreeMap
    fn get_pg_data(&self, pgid: PgId) -> Vec<u8> {
        let pg: BTreeMap<String, Value> = self
            .kv
            .range(
                pgid.to_string() + "."
                    ..(pgid).to_string() + &format!("{}", ('.' as u8 + 1) as char),
            )
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        bincode::serialize(&pg).unwrap()
    }

    fn push_pg_data(&mut self, pgid: PgId, data: Vec<u8>) {
        //1. clear the potential pgid's data
        assert_eq!(
            self.kv
                .range(
                    pgid.to_string() + "."
                        ..(pgid).to_string() + &format!("{}", ('.' as u8 + 1) as char)
                )
                .count(),
            0
        );
        //2. push the pg data into kv
        let mut pg_data: BTreeMap<String, Value> = bincode::deserialize(&data).unwrap();
        self.kv.append(&mut pg_data);
    }

    fn get_key_version(&self, key: &str) -> Option<u64> {
        self.kv.get(key).map(|value| value.version)
    }
}

impl KvService {
    pub fn new() -> Self {
        KvService {
            kv: BTreeMap::new(),
        }
    }
}

#[cfg(test)]
mod test {
    use std::collections::BTreeMap;

    use super::{KvService, Store, Value};
    use crate::{
        constant::{PG_NUM, REPLICA_SIZE},
        distributor::{Distributor, SimpleHashDistributor},
        test::common::gen_random_put,
    };

    #[test]
    fn test_pgid() {
        let mut service = KvService::new();
        let distributor = SimpleHashDistributor::<REPLICA_SIZE>;
        let mut golden: Vec<BTreeMap<String, Vec<u8>>> = vec![BTreeMap::new(); PG_NUM];
        for _ in 0..5000 {
            let (k, v) = gen_random_put(3, 10);
            let pgid = distributor.assign_pgid(k.as_bytes());
            service.put(format!("{}.{}", pgid, k), v.clone());
            golden[pgid].insert(format!("{}.{}", pgid, k), v);
        }
        for pgid in 0..PG_NUM {
            let pg_data: BTreeMap<String, Value> =
                bincode::deserialize(&service.get_pg_data(pgid)).unwrap();
            let golden = &golden[pgid];
            for (d, g) in pg_data.iter().zip(golden.iter()) {
                assert_eq!(d.0, g.0);
                assert_eq!(&d.1.data, g.1);
            }
            for key in pg_data.keys() {
                let pgid_of_key = key.split('.').next().unwrap();
                assert_eq!(pgid_of_key, pgid.to_string())
            }
        }
    }
}
