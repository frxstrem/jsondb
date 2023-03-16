use std::hash::{Hash, Hasher};

pub trait CacheTag<T> {
    fn process_value(&mut self, value: &T);
    fn tag(&self) -> u64;
}

#[derive(Default, Debug)]
pub struct DefaultCacheTag {
    counter: u64,
}

impl<T> CacheTag<T> for DefaultCacheTag {
    fn process_value(&mut self, _value: &T) {
        self.counter += 1;
    }

    fn tag(&self) -> u64 {
        self.counter ^ 0x6e2797fa0b96b68f
    }
}

#[derive(Default, Debug)]
pub struct HashCacheTag<H> {
    hasher: H,
}

impl<H> HashCacheTag<H> {
    pub fn new(hasher: H) -> HashCacheTag<H> {
        Self { hasher }
    }
}

impl<H, T> CacheTag<T> for HashCacheTag<H>
where
    H: Hasher,
    T: Hash,
{
    fn process_value(&mut self, value: &T) {
        value.hash(&mut self.hasher);
    }

    fn tag(&self) -> u64 {
        self.hasher.finish()
    }
}
