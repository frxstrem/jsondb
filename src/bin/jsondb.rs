use indexmap::IndexMap;
use serde_json::Value;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use structopt::StructOpt;

use jsondb::{Database, RecordData};

type Object = IndexMap<String, Value>;

#[derive(Debug, StructOpt)]
struct Options {
    #[structopt(subcommand)]
    command: Command,
}

#[derive(Debug, StructOpt)]
enum Command {
    #[structopt(alias = "ls")]
    List {
        #[structopt(short = "d", long = "include-deleted")]
        include_deleted: bool,

        file: PathBuf,
        ids: Vec<u32>,
    },
    Add {
        file: PathBuf,
    },
    #[structopt(alias = "upd")]
    Update {
        file: PathBuf,
    },
    #[structopt(alias = "rm")]
    Remove {
        file: PathBuf,
        ids: Vec<u32>,
    },
}

impl Command {
    fn is_read_only(&self) -> bool {
        match self {
            Command::List { .. } => true,
            Command::Add { .. } | Command::Update { .. } | Command::Remove { .. } => false,
        }
    }

    fn file(&self) -> &Path {
        match self {
            Command::List { file, .. }
            | Command::Add { file, .. }
            | Command::Update { file, .. }
            | Command::Remove { file, .. } => file,
        }
    }
}

fn main() -> io::Result<()> {
    let opts = Options::from_args();

    let mut database = jsondb::OpenOptions::new()
        .read_only(opts.command.is_read_only())
        .open::<Object, _>(opts.command.file())?;

    match opts.command {
        Command::List {
            include_deleted,
            ids,
            ..
        } => {
            fn list_records<'a>(
                records: impl Iterator<Item = &'a RecordData<Object>>,
                ids: &[u32],
            ) -> io::Result<()> {
                let mut out = io::stdout();
                for record in records {
                    if !ids.is_empty() && !ids.contains(&record.id) {
                        continue;
                    }

                    serde_json::to_writer(&mut out, &record)?;
                    writeln!(out)?;
                    out.flush()?
                }
                Ok(())
            }

            if include_deleted {
                list_records(database.records_include_deleted(), &ids)?;
            } else {
                list_records(database.records(), &ids)?;
            }
        }

        Command::Add { .. } => {
            let input = serde_json::Deserializer::from_reader(io::stdin()).into_iter::<Object>();
            for record in input {
                let mut record = record?;

                if record.contains_key("id") {
                    record.shift_remove("id");
                }
                if record.contains_key("deleted") {
                    record.shift_remove("deleted");
                }

                database.insert(record)?;
            }
        }

        Command::Update { .. } => {
            let input = serde_json::Deserializer::from_reader(io::stdin())
                .into_iter::<RecordData<Object>>();
            for record in input {
                let mut record = record?;

                if record.contains_key("id") {
                    record.shift_remove("id");
                }
                if record.contains_key("deleted") {
                    record.shift_remove("deleted");
                }

                database.upsert(record.id, |_| Some(record.data))?;
            }
        }

        Command::Remove { ids, .. } => {
            for id in ids {
                database.delete(id)?;
            }
        }
    }

    Ok(())
}
