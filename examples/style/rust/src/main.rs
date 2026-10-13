//! Local-only style example: no TTL, transport, renewal or wire protocol.
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[derive(Debug, PartialEq, Eq)]
enum Error { EmptyResource, Poisoned }
#[derive(Debug, PartialEq, Eq)]
enum ReleaseOutcome { Released, AlreadyNotOwned }

#[derive(Default)]
struct MemoryStore { owners: Mutex<HashMap<String, Arc<u8>>> }
impl MemoryStore {
    fn claim(&self, name: &str, owner: &Arc<u8>) -> Result<bool, Error> {
        use std::collections::hash_map::Entry;
        let mut owners = self.owners.lock().map_err(|_| Error::Poisoned)?;
        match owners.entry(name.to_owned()) {
            Entry::Occupied(_) => Ok(false),
            Entry::Vacant(entry) => { entry.insert(Arc::clone(owner)); Ok(true) }
        }
    }
    fn release(&self, name: &str, owner: &Arc<u8>) -> Result<ReleaseOutcome, Error> {
        let mut owners = self.owners.lock().map_err(|_| Error::Poisoned)?;
        if owners.get(name).is_some_and(|current| Arc::ptr_eq(current, owner)) {
            owners.remove(name);
            Ok(ReleaseOutcome::Released)
        } else { Ok(ReleaseOutcome::AlreadyNotOwned) }
    }
    fn expire_for_test(&self, name: &str) -> Result<(), Error> {
        self.owners.lock().map_err(|_| Error::Poisoned)?.remove(name);
        Ok(())
    }
}

/// One acquisition. No Clone implementation: ownership is moved, not copied.
struct Lease { store: Arc<MemoryStore>, resource: String, owner: Option<Arc<u8>> }
impl Lease {
    fn release(&mut self) -> Result<ReleaseOutcome, Error> {
        match self.owner.take() {
            None => Ok(ReleaseOutcome::AlreadyNotOwned),
            Some(owner) => self.store.release(&self.resource, &owner),
        }
    }
}
impl Drop for Lease {
    fn drop(&mut self) {
        // This is local memory, not a template for remote async cleanup.
        // A real SDK uses explicit async release + tracked bounded best effort.
        let _ = self.release();
    }
}

struct ResourceLock { store: Arc<MemoryStore>, resource: String }
impl ResourceLock {
    fn try_acquire(&self) -> Result<Option<Lease>, Error> {
        let owner = Arc::new(1_u8);
        if !self.store.claim(&self.resource, &owner)? { return Ok(None); }
        Ok(Some(Lease { store: Arc::clone(&self.store), resource: self.resource.clone(), owner: Some(owner) }))
    }
}
struct Provider { store: Arc<MemoryStore> }
impl Provider {
    fn lock(&self, name: &str) -> Result<ResourceLock, Error> {
        if name.is_empty() { return Err(Error::EmptyResource); }
        Ok(ResourceLock { store: Arc::clone(&self.store), resource: name.to_owned() })
    }
}

fn run_checks() -> Result<(), Error> {
    let store = Arc::new(MemoryStore::default());
    let provider = Provider { store: Arc::clone(&store) };
    let lock = provider.lock("example/resource")?;
    let mut first = lock.try_acquire()?.expect("first");
    assert!(lock.try_acquire()?.is_none());
    store.expire_for_test("example/resource")?;
    let mut second = lock.try_acquire()?.expect("second");
    assert_eq!(first.release()?, ReleaseOutcome::AlreadyNotOwned);
    assert!(lock.try_acquire()?.is_none());
    assert_eq!(second.release()?, ReleaseOutcome::Released);
    assert_eq!(second.release()?, ReleaseOutcome::AlreadyNotOwned);
    {
        let held = lock.try_acquire()?.expect("scope");
        let moved = held;
        drop(moved);
    }
    assert!(lock.try_acquire()?.is_some());
    assert!(matches!(provider.lock(""), Err(Error::EmptyResource)));
    Ok(())
}
fn main() {
    run_checks().expect("style checks");
    println!("PASS rust: busy, stale owner, idempotence, ownership/Drop, validation");
}
#[cfg(test)]
mod tests {
    #[test]
    fn local_lifecycle() { super::run_checks().unwrap(); }
}
