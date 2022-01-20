use crate::{PgId, PgVersion, constant::*, distributor::{Distributor, SimpleHashDistributor}, rpc::{KvRequest, Put}};
use log::{debug, warn};
use madsim::fs::{self, File};
use std::sync::atomic::{AtomicU64, Ordering};

// TODO: consider snapshot

/// `LogManager` is responsible for maintaining logs for *each pg*.
pub struct LogManager {
    /// upper bound of log index for each pg (exclude, e.g. 5 means valid id is `[0, 5)`)
    uppers: Vec<AtomicU64>,
    distributor: Box<dyn Distributor<REPLICA_SIZE>>,
}

impl LogManager {
    pub async fn new(pg_num: usize) -> Self {
        let uppers = {
            if let Ok(data) = fs::read(format!("log.uppers")).await {
                bincode::deserialize(&data).unwrap()
            } else {
                (0..pg_num).map(|_| AtomicU64::new(0)).collect()
            }
        };
        LogManager {
            uppers,
            distributor: Box::new(SimpleHashDistributor::<REPLICA_SIZE>::new(pg_num)),
        }
    }

    pub async fn log(&self, op: &Put, id: u64) {
        let pgid = self.distributor.assign_pgid(op.key().as_bytes());
        let file = File::create(format!("log.{}.{}", pgid, id)).await.unwrap();
        file.write_all_at(&bincode::serialize(op).unwrap(), 0)
            .await
            .unwrap();
        // update uppers
        self.uppers[pgid].fetch_max(id + 1, Ordering::SeqCst);
        let file = File::create(format!("log.uppers")).await.unwrap();
        file.write_all_at(&bincode::serialize(&self.uppers).unwrap(), 0)
            .await
            .unwrap();
        debug!("Persist log {}", id);
    }

    /// Get the highest index of recorded log,
    /// which can be taken as version number
    pub fn upper(&self, pgid: PgId) -> PgVersion {
        self.uppers[pgid].load(Ordering::SeqCst)
    }

    pub async fn get(&self, pgid: PgId, log_id: u64) -> Option<Put> {
        if log_id >= self.uppers[pgid].load(Ordering::SeqCst) {
            return None;
        }
        if let Ok(data) = fs::read(format!("log.{}.{}", pgid, log_id)).await {
            match bincode::deserialize(&data) {
                Ok(op) => Some(op),
                Err(err) => {
                    warn!("get log {} of pg {} failed: {}", log_id, pgid, err);
                    None
                }
            }
        } else {
            None
        }
    }
}
