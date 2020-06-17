use serde::{Deserialize, Serialize};
use std::ops::{Deref, DerefMut};

use crate::boolean::{False, True};

pub type RecordId = u32;

#[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Record<T> {
    Upsert(UpsertRecord<T>),
    Delete(DeleteRecord),
}

impl<T> Record<T> {
    pub const fn upsert(id: RecordId, data: T) -> Record<T> {
        Record::Upsert(UpsertRecord {
            deleted: False,
            data: RecordData { id, data },
        })
    }

    pub const fn delete(id: RecordId) -> Record<T> {
        Record::Delete(DeleteRecord { id, deleted: True })
    }

    pub fn id(&self) -> RecordId {
        match self {
            Record::Upsert(record) => record.id(),
            Record::Delete(record) => record.id(),
        }
    }

    pub fn data(&self) -> Option<&RecordData<T>> {
        match self {
            Record::Upsert(UpsertRecord { data, .. }) => Some(&data),
            Record::Delete(_) => None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RecordData<T> {
    pub id: RecordId,
    #[serde(flatten)]
    pub data: T,
}

impl<T> Deref for RecordData<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.data
    }
}

impl<T> DerefMut for RecordData<T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.data
    }
}

#[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct UpsertRecord<T> {
    #[serde(rename = "deleted", default, skip_serializing)]
    pub deleted: False,
    #[serde(flatten)]
    pub data: RecordData<T>,
}

impl<T> UpsertRecord<T> {
    pub fn id(&self) -> RecordId {
        self.data.id
    }
}

#[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DeleteRecord {
    pub id: RecordId,
    pub deleted: True,
}

impl DeleteRecord {
    pub fn id(&self) -> RecordId {
        self.id
    }
}
