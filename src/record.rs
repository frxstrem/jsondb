use serde::{Deserialize, Serialize};

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
}

impl<T> Record<T> {
    pub fn id(&self) -> RecordId {
        match self {
            Record::Upsert(record) => record.id(),
            Record::Delete(record) => record.id(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RecordData<T> {
    pub id: RecordId,
    #[serde(flatten)]
    pub data: T,
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
