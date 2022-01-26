use super::{PgState, ReliableCtl};
use crate::service::Store;
use madsim::time::{sleep, Duration};

impl<T> ReliableCtl<T>
where
    T: Store + Sync + Send + 'static,
{
    pub(super) async fn background_snapshot(&self) {
        loop {
            sleep(Duration::from_secs(5)).await;
            let pgids = self
                .inner
                .pgs
                .lock()
                .unwrap()
                .iter()
                .filter(|(_, info)| info.state == PgState::Active)
                .map(|x| *x.0)
                .collect::<Vec<_>>();
            for pgid in pgids {
                let info = self.inner.pgs.lock().unwrap().get(&pgid).cloned();
                if let Some(info) = info {
                    let lower = self.inner.logger.lower(pgid);
                    if info.state == PgState::Active && info.applied_ptr - lower >= 100 {
                        self.inner.snapshot(pgid).await;
                    }
                }
            }
        }
    }
}
