use serde::{Deserialize, Serialize};
use std::io::Cursor;

use crate::*;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct MyObject {
    a: String,
    b: i32,
    c: Option<i32>,
}

#[test]
fn read_test() {
    let database_contents = r#"
        {"id":1,"a":"foo","b":33,"c":99}
        {"id":2,"a":"bar","b":66}
        {"id":1,"a":"qwe","b":9}
        {"id":3,"a":"hello","b":0}
        {"id":2,"deleted":true}
    "#;

    let stream = Cursor::new(database_contents);
    let mut database = Database::<MyObject, _>::new(stream).unwrap();

    database.reload().unwrap();

    let entries = database.records().collect::<Vec<_>>();

    assert_eq!(
        entries,
        vec![
            &RecordData {
                id: 1,
                data: MyObject {
                    a: "qwe".into(),
                    b: 9,
                    c: None
                }
            },
            &RecordData {
                id: 3,
                data: MyObject {
                    a: "hello".into(),
                    b: 0,
                    c: None
                }
            }
        ],
    );
}

#[test]
fn partial_read_test() {
    let database_contents = r#"
        {"id":1,"a":"foo","b":33,"c":99}
        {"id":2,"a":"bar","b":66}
        {"id":1,"a":"
    "#;

    let stream = Cursor::new(database_contents);
    let mut database = Database::<MyObject, _>::new(stream).unwrap();

    assert!(database.reload().is_err());
}

#[test]
fn write_test() {
    let mut database_contents = Vec::from(
        br#"
        {"id":1,"a":"foo","b":33,"c":99}
        {"id":2,"a":"bar","b":66}
        {"id":1,"a":"qwe","b":9}
        {"id":3,"a":"hello","b":0}
        {"id":2,"deleted":true}
    "# as &[u8],
    );

    let stream = Cursor::new(&mut database_contents);
    let mut database = Database::<MyObject, _>::new(stream).unwrap();

    database.reload().unwrap();

    database
        .insert(MyObject {
            a: "beep".into(),
            b: 1,
            c: Some(2),
        })
        .unwrap();
    database.delete(1).unwrap();
    database
        .upsert(3, |data| {
            data.cloned().map(|data| MyObject {
                c: Some(123),
                ..data
            })
        })
        .unwrap();

    let entries = database.records().collect::<Vec<_>>();

    assert_eq!(
        entries,
        vec![
            &RecordData {
                id: 3,
                data: MyObject {
                    a: "hello".into(),
                    b: 0,
                    c: Some(123)
                }
            },
            &RecordData {
                id: 4,
                data: MyObject {
                    a: "beep".into(),
                    b: 1,
                    c: Some(2)
                }
            },
        ],
    );

    database.close().unwrap();

    let records = serde_json::Deserializer::from_slice(&database_contents)
        .into_iter()
        .collect::<Result<Vec<Record<MyObject>>, _>>()
        .unwrap();

    assert_eq!(
        records,
        vec![
            Record::upsert(
                1,
                MyObject {
                    a: "foo".into(),
                    b: 33,
                    c: Some(99)
                }
            ),
            Record::upsert(
                2,
                MyObject {
                    a: "bar".into(),
                    b: 66,
                    c: None
                }
            ),
            Record::upsert(
                1,
                MyObject {
                    a: "qwe".into(),
                    b: 9,
                    c: None
                }
            ),
            Record::upsert(
                3,
                MyObject {
                    a: "hello".into(),
                    b: 0,
                    c: None
                }
            ),
            Record::delete(2),
            Record::upsert(
                4,
                MyObject {
                    a: "beep".into(),
                    b: 1,
                    c: Some(2)
                }
            ),
            Record::delete(1),
            Record::upsert(
                3,
                MyObject {
                    a: "hello".into(),
                    b: 0,
                    c: Some(123)
                }
            )
        ]
    );
}

#[test]
fn file_test() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let path = tmp_dir.path().join("database.json");

    let mut database = Database::<MyObject, _>::open(&path).unwrap();
    database.reload().unwrap();
    assert_eq!(database.record_count(), 0);

    let obj = MyObject {
        a: "".into(),
        b: 0,
        c: None,
    };
    let id = database.insert(obj.clone()).unwrap();
    assert_eq!(database.record_count(), 1);

    database.close().unwrap();

    let mut database = Database::<MyObject, _>::open(&path).unwrap();
    database.reload().unwrap();
    assert_eq!(database.record_count(), 1);

    assert_eq!(
        database.get(id),
        Some(&RecordData {
            id,
            data: obj.clone()
        })
    );

    database.close().unwrap();
}

#[test]
fn parallel_write_test() {
    use std::sync::Barrier;

    // create path for temporary file
    let tmp_dir = tempfile::tempdir().unwrap();
    let path = tmp_dir.path().join("database.json");

    let barrier = Barrier::new(2);

    crossbeam::scope(|s| {
        // A thread
        s.spawn(|_| {
            // open database file
            let mut database = Database::<MyObject, _>::open(&path).unwrap();
            assert_eq!(database.record_count(), 0);
            barrier.wait(); // 1

            // insert record
            let id = database
                .insert(MyObject {
                    a: "a".into(),
                    b: 1,
                    c: None,
                })
                .unwrap();
            assert_eq!(id, 1);
            assert_eq!(database.record_count(), 1);
            barrier.wait();
            barrier.wait(); // 3

            // check that record has been deleted
            database.reload().unwrap();
            assert_eq!(database.record_count(), 0);
            barrier.wait();
            barrier.wait(); // 5

            database.close().unwrap();
        });

        // B thread
        s.spawn(|_| {
            // open database file
            let mut database = Database::<MyObject, _>::open(&path).unwrap();
            assert_eq!(database.record_count(), 0);
            barrier.wait();
            barrier.wait(); // 2

            // read record
            database.reload().unwrap();
            assert_eq!(database.record_count(), 1);
            assert_eq!(
                database.get(1),
                Some(&RecordData {
                    id: 1,
                    data: MyObject {
                        a: "a".into(),
                        b: 1,
                        c: None
                    }
                })
            );

            // delete record
            database.delete(1).unwrap();
            assert_eq!(database.record_count(), 0);
            barrier.wait();
            barrier.wait(); // 4

            database.close().unwrap();
            barrier.wait(); // 5
        });
    })
    .unwrap()
}
