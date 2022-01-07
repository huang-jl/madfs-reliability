use crate::constant::REPLICA_SIZE;
use crate::distributor::{Distributor, SimpleHashDistributor};
use crate::{ctl::ServerError, PgId};
use log::info;
use madsim::net::rpc::{Request as Req, Serialize};
use madsim::Request;
use serde::Deserialize;
use std::collections::BTreeMap;
use std::fmt::Debug;

pub trait ServiceInput {
    fn key_bytes(&self) -> &[u8];
    fn check_modify_operation(&self) -> bool;
}

pub trait Service {
    type Input: ServiceInput + Req<Response = Result<Self::Output, ServerError>> + Clone + Debug;
    type Output;
    /// Dispatch args to the inner method. We only care about *modify* operations here.
    fn dispatch_write(&mut self, args: Self::Input) -> Self::Output;
    fn dispatch_read(&self, args: Self::Input) -> Self::Output;
    fn copy_pg_data(&self, pgid: PgId) -> Vec<u8>;
    fn recovery_pg_data(&mut self, pgid: PgId, data: Vec<u8>);
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Value {
    data: String,
    version: u64,   //version of this key
}

pub struct KvService {
    kv: BTreeMap<String, Value>,
    distributor: Box<dyn Distributor<REPLICA_SIZE>>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Request)]
#[rtype("Result<KvRes, ServerError>")]
pub enum KvArgs {
    Put { key: String, value: String },
    Get(String),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum KvRes {
    Put,
    Get(Option<String>),
}

impl ServiceInput for KvArgs {
    fn check_modify_operation(&self) -> bool {
        match self {
            KvArgs::Put { .. } => true,
            _ => false,
        }
    }

    fn key_bytes(&self) -> &[u8] {
        match self {
            KvArgs::Get(key) => key.as_bytes(),
            KvArgs::Put { key, .. } => key.as_bytes(),
        }
    }
}

impl KvArgs {
    pub fn get_key(&self) -> String {
        match self {
            KvArgs::Put { key, .. } => key.clone(),
            KvArgs::Get(key) => key.clone(),
        }
    }

    pub fn get_value(&self) -> Option<String> {
        match self {
            KvArgs::Get(..) => None,
            KvArgs::Put { value, .. } => Some(value.clone()),
        }
    }
}

impl KvRes {
    pub fn get_value(&self) -> Option<String> {
        match self {
            KvRes::Put => None,
            KvRes::Get(res) => res.clone(),
        }
    }
}

impl Service for KvService {
    type Input = KvArgs;
    type Output = KvRes;

    fn dispatch_write(&mut self, args: Self::Input) -> KvRes {
        assert!(args.check_modify_operation());
        match args {
            KvArgs::Put { key, value } => {
                self.put(key, value);
                KvRes::Put
            }
            _ => unreachable!(),
        }
    }

    fn dispatch_read(&self, args: Self::Input) -> KvRes {
        assert!(!args.check_modify_operation());
        match args {
            KvArgs::Get(key) => {
                let value = self.get(key);
                KvRes::Get(value)
            }
            _ => unreachable!(),
        }
    }

    /// Will return the bytes representation of a BtreeMap
    fn copy_pg_data(&self, pgid: PgId) -> Vec<u8> {
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

    fn recovery_pg_data(&mut self, pgid: PgId, data: Vec<u8>) {
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
}

impl KvService {
    pub fn new() -> Self {
        KvService {
            kv: BTreeMap::new(),
            distributor: Box::new(SimpleHashDistributor::<REPLICA_SIZE>),
        }
    }

    fn put(&mut self, key: String, value: String) {
        info!("Put key: {}, value: {}", key, value);
        let key_ = format!("{}.{}", self.distributor.assign_pgid(key.as_bytes()), key);
        let entry = self.kv.entry(key_).or_insert(Value {
            data: value,
            version: 0,
        });
        entry.version += 1;
    }

    fn get(&self, key: String) -> Option<String> {
        let key_ = format!("{}.{}", self.distributor.assign_pgid(key.as_bytes()), key);
        let res = self.kv.get(&key_).cloned().map(|v| v.data);
        info!("Get key: {}, return: {:?}", key, res);
        res
    }
}

#[cfg(test)]
mod test {
    use std::collections::BTreeMap;

    use super::{KvService, Service, Value};
    use crate::{constant::PG_NUM, test::common::gen_random_put};

    #[test]
    fn test_pgid() {
        let mut service = KvService::new();
        for _ in 0..5000 {
            let args = gen_random_put(5, 10);
            let k = args.get_key();
            let v = args.get_value().unwrap();
            service.put(k, v);
        }
        for pgid in 0..PG_NUM {
            let pg_data: BTreeMap<String, Value> =
                bincode::deserialize(&service.copy_pg_data(pgid)).unwrap();
            for key in pg_data.keys() {
                let pgid_of_key = key.split('.').next().unwrap();
                assert_eq!(pgid_of_key, pgid.to_string())
            }
        }
    }
}
