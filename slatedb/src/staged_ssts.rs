use crate::db_state::SsTableId;
use std::collections::HashSet;
use std::sync::{Arc, Mutex};

/// L0 SSTs the writer has uploaded to the object store but not yet durably
/// published into the manifest (the pre-publish "staged" window).
///
/// The GC normally protects a freshly flushed L0 with a timestamp watermark
/// (the newest L0 ULID it knows about). That heuristic assumes physical SST
/// ULID order matches manifest publish order, which the parallel L0 upload pool
/// can violate: a later memtable's SST may be minted with an earlier ULID, so a
/// staged L0 can sit below the watermark and be deleted before it is published.
/// When the publish backlog clears the deleted SST is retired into a live tree
/// and the next open fails with `NoSuchKey`.
///
/// This set lets an in-process GC exclude exactly the SSTs the writer still
/// intends to publish, independent of any timestamp. An id is present from the
/// moment the SST is uploaded until its publishing manifest write is durable.
#[derive(Clone, Default)]
pub(crate) struct StagedSsts {
    inner: Arc<Mutex<HashSet<SsTableId>>>,
}

impl StagedSsts {
    pub(crate) fn add(&self, ids: impl IntoIterator<Item = SsTableId>) {
        let mut guard = self.inner.lock().expect("staged ssts mutex poisoned");
        guard.extend(ids);
    }

    pub(crate) fn remove(&self, ids: impl IntoIterator<Item = SsTableId>) {
        let mut guard = self.inner.lock().expect("staged ssts mutex poisoned");
        for id in ids {
            guard.remove(&id);
        }
    }

    pub(crate) fn snapshot(&self) -> HashSet<SsTableId> {
        self.inner
            .lock()
            .expect("staged ssts mutex poisoned")
            .clone()
    }
}
