use itertools::Itertools;
use serde::{de::DeserializeOwned, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;

use crate::record::{Record, RecordData, RecordId};

pub struct Database<T: Serialize + DeserializeOwned, S: Read + Seek> {
    stream: BufReader<S>,
    offset: u64,
    records: Vec<Record<T>>,
    next_record_id: RecordId,
}

impl<T: Serialize + DeserializeOwned> Database<T, File> {
    pub fn open(path: impl AsRef<Path>) -> io::Result<Database<T, File>> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(path)?;
        let stream = BufReader::new(file);

        Ok(Database {
            stream,
            offset: 0,
            records: Vec::new(),
            next_record_id: 1,
        })
    }
}

impl<T: Serialize + DeserializeOwned, S: Read + Seek> Database<T, S> {
    pub fn new(mut stream: S) -> io::Result<Database<T, S>> {
        let offset = stream.seek(SeekFrom::Current(0))?;
        let stream = BufReader::new(stream);
        Ok(Database {
            stream,
            offset,
            records: Vec::new(),
            next_record_id: 1,
        })
    }

    pub fn close(self) -> io::Result<()> {
        drop(self);
        Ok(())
    }

    fn handle_record(&mut self, record: Record<T>) {
        if record.id() >= self.next_record_id {
            self.next_record_id = record.id() + 1;
        }
        self.records.push(record);
    }

    fn read_next(&mut self) -> io::Result<Option<Record<T>>> {
        self.stream.seek(SeekFrom::Start(self.offset))?;
        let mut d = serde_json::Deserializer::from_reader(&mut self.stream).into_iter();

        // read next record
        let record = d.next().transpose()?;
        self.offset = self.stream.seek(SeekFrom::Current(0))?;

        Ok(record)
    }

    fn is_at_end(&mut self) -> io::Result<bool> {
        let offset = self.stream.seek(SeekFrom::End(0))?;
        Ok(offset == self.offset)
    }

    pub fn reload(&mut self) -> io::Result<()> {
        while let Some(record) = self.read_next()? {
            self.handle_record(record);
        }

        Ok(())
    }

    pub fn records(&self) -> impl Iterator<Item = &RecordData<T>> {
        let mut items = self
            .records
            .iter()
            .rev()
            .unique_by(|record| record.id())
            .filter_map(Record::data)
            .collect::<Vec<_>>();
        items.sort_by_key(|data| data.id);
        items.into_iter()
    }

    pub fn records_include_deleted(&self) -> impl Iterator<Item = &RecordData<T>> {
        let mut items = self
            .records
            .iter()
            .rev()
            .filter_map(Record::data)
            .unique_by(|record| record.id)
            .collect::<Vec<_>>();
        items.sort_by_key(|data| data.id);
        items.into_iter()
    }

    pub fn record_count(&self) -> usize {
        self.records().count()
    }

    pub fn get(&self, id: RecordId) -> Option<&RecordData<T>> {
        self.records().find(|record| record.id == id)
    }
}

impl<T: Serialize + DeserializeOwned, S: Read + Write + Seek> Database<T, S> {
    fn writer(&mut self) -> io::Result<BufWriter<&mut S>> {
        // reset buffer
        self.stream.seek(SeekFrom::Current(0))?;

        // return inner
        Ok(BufWriter::new(self.stream.get_mut()))
    }

    fn write_record(&mut self, record: Record<T>) -> io::Result<()> {
        // move to end of file
        self.reload()?;
        if !self.is_at_end()? {
            return Err(io::Error::new(io::ErrorKind::Other, "Expected EOF"));
        }

        // append and flush
        {
            let mut writer = self.writer()?;
            serde_json::to_writer(&mut writer, &record)?;
            writeln!(writer)?;
            writer.flush()?;
        }

        // update internal state
        self.handle_record(record);

        Ok(())
    }

    pub fn insert(&mut self, data: T) -> io::Result<RecordId> {
        let id = self.next_record_id;
        self.next_record_id += 1;

        self.write_record(Record::upsert(id, data))?;

        Ok(id)
    }

    pub fn upsert<F>(&mut self, id: RecordId, f: F) -> io::Result<()>
    where
        F: FnOnce(Option<&T>) -> Option<T>,
    {
        let data = self.get(id).map(|record_data| &record_data.data);

        match f(data) {
            Some(new_data) => self.write_record(Record::upsert(id, new_data))?,
            None if data.is_some() => self.write_record(Record::delete(id))?,
            None => (),
        }

        Ok(())
    }

    pub fn delete(&mut self, id: RecordId) -> io::Result<()> {
        self.write_record(Record::delete(id))
    }
}
