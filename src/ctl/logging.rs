use crate::{
    constant::*,
    distributor::{Distributor, SimpleHashDistributor},
    rpc::{KvRequest, Put},
    PgId, PgVersion,
};
use log::{debug, warn};
use madsim::fs::{self, File};
use std::sync::atomic::{AtomicU64, Ordering};

// TODO: consider snapshot

/// `LogManager` is responsible for maintaining logs for *each pg*.
pub struct LogManager {
    /// upper bound of log index for each pg (exclude, e.g. 5 means valid id is < 5)
    uppers: Vec<AtomicU64>,
    /// lower bound of log index for each pg (exclude, e.g. 5 means valid id is >= 5)
    lowers: Vec<AtomicU64>,
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
        let lowers = {
            if let Ok(data) = fs::read(format!("log.lowers")).await {
                bincode::deserialize(&data).unwrap()
            } else {
                (0..pg_num).map(|_| AtomicU64::new(0)).collect()
            }
        };
        LogManager {
            uppers,
            lowers,
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

    /// All id between [lower, point) will be in snapshot.
    pub async fn snapshot(&self, pgid: PgId, point: u64) {
        let lower = self.lower(pgid);
        // update lowers
        self.lowers[pgid].store(point, Ordering::SeqCst);
        let file = File::create(format!("log.lowers")).await.unwrap();
        file.write_all_at(&bincode::serialize(&self.lowers).unwrap(), 0)
            .await
            .unwrap();
        // remove logs between [lower, point)
        for log_id in lower..point {
            File::create(format!("log.{}.{}", pgid, log_id))
                .await
                .unwrap();
        }
    }

    /// Get the highest index of recorded log,
    /// which can be taken as version number
    pub fn upper(&self, pgid: PgId) -> PgVersion {
        self.uppers[pgid].load(Ordering::SeqCst)
    }

    pub fn lower(&self, pgid: PgId) -> PgVersion {
        self.lowers[pgid].load(Ordering::SeqCst)
    }

    pub async fn get(&self, pgid: PgId, log_id: u64) -> Option<Put> {
        if log_id < self.upper(pgid) && log_id >= self.lower(pgid) {
            if let Ok(data) = fs::read(format!("log.{}.{}", pgid, log_id)).await {
                match bincode::deserialize(&data) {
                    Ok(op) => return Some(op),
                    Err(err) => {
                        warn!("get log {} of pg {} failed: {}", log_id, pgid, err);
                    }
                }
            }
        }
        None
    }
}
