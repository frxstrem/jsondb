use clap::Parser;
use indexmap::IndexMap;
use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use jsondb::RecordData;

type StdError = Box<dyn std::error::Error + Send + Sync>;

type Object = IndexMap<String, Value>;

#[derive(Debug, Parser)]
struct Options {
    #[structopt(subcommand)]
    command: Command,
}

#[derive(Debug, Parser)]
enum Command {
    #[structopt(alias = "ls")]
    List {
        #[structopt(short = 'd', long = "include-deleted")]
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

        #[clap(short = 'n', long = "dry-run", requires = "jq")]
        dry_run: bool,

        #[clap(short = 'j', long = "jq", requires = "ids")]
        jq: Option<String>,

        #[clap(requires = "jq")]
        ids: Vec<u32>,
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

fn main() -> Result<(), StdError> {
    let opts = Options::parse();

    let mut database = jsondb::OpenOptions::new()
        .read_only(opts.command.is_read_only())
        .open::<Object, _>(opts.command.file())?;

    match opts.command {
        Command::List {
            include_deleted,
            ids,
            ..
        } => {
            let records = if include_deleted {
                list_records(database.records_include_deleted(), &ids)
            } else {
                list_records(database.records(), &ids)
            };

            print_records(records)?;
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

        Command::Update {
            dry_run, jq, ids, ..
        } => {
            if let Some(jq) = jq {
                let records = list_records(database.records(), &ids);
                let updated_records: Vec<RecordData<Object>> = run_jq_all(&jq, records)?;

                if dry_run {
                    print_records(&updated_records)?;
                } else {
                    for mut record in updated_records {
                        if record.contains_key("id") {
                            record.shift_remove("id");
                        }
                        if record.contains_key("deleted") {
                            record.shift_remove("deleted");
                        }

                        database.upsert(record.id, |_| Some(record.data))?;
                    }
                }
            } else {
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
        }

        Command::Remove { ids, .. } => {
            for id in ids {
                database.delete(id)?;
            }
        }
    }

    Ok(())
}

fn list_records<'a>(
    records: impl IntoIterator<Item = &'a RecordData<Object>>,
    ids: &[u32],
) -> Vec<&'a RecordData<Object>> {
    records
        .into_iter()
        .filter(move |record| ids.is_empty() || ids.contains(&record.id))
        .collect()
}

fn print_records<'a>(records: impl IntoIterator<Item = &'a RecordData<Object>>) -> io::Result<()> {
    let mut out = io::stdout();
    for record in records {
        serde_json::to_writer(&mut out, &record)?;
        writeln!(out)?;
        out.flush()?
    }
    Ok(())
}

fn run_jq_all<'a, T: 'a + Serialize, U: DeserializeOwned>(
    jq: &str,
    inputs: impl IntoIterator<Item = &'a T>,
) -> Result<Vec<U>, StdError> {
    let mut program = jq_rs::compile(jq).map_err(|err| format!("jq error: {err}"))?;

    let inputs = inputs
        .into_iter()
        .map(serde_json::to_string)
        .collect::<Result<Vec<_>, _>>()?;

    let outputs = inputs
        .into_iter()
        .map(|input| program.run(&input))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| format!("jq error: {err}"))?;

    let outputs = outputs
        .into_iter()
        .map(|output| serde_json::from_str(&output))
        .collect::<Result<Vec<_>, _>>()?;

    Ok(outputs)
}
