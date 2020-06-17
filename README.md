# jsondb

**jsondb** is a simple JSON-based database format.

A jsondb file consists of a sequence of JSON objects, each representing a single change record. The records may be separated by zero or more whitespace.

Each change record has two reserved properties, and is otherwise an arbitrary JSON object:
* The `id` property contains a unique numeric ID for the object, between 1 and 2<sup>32</sup>-1 (inclusive). If multiple change records have the same `id`, only the last will be used. (Later records can overwrite earlier ones.)
* The `deleted` property may be set to indicate that the record represents a delete operation. If the property is `true`, then the object is deleted from the database, and all other properties should be ignored.

To be maximally compatible, a jsondb file should contain a single JSON change record per line, although implementations should accept any whitespace (or none) inside or between records.
