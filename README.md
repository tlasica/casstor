# CASStor

CASStor is a simple PoC of a cloud storage using Apache Cassandra.

## Features

1. It can store or restore files
2. Implements block deduplication
3. Scales horizontally with more storage / performance demand
4. Keeps configurable number of block copies
5. Space reclamation (removal of unused blocks) can be performed in a “cleanup” time (no writes accepted)

## Usage

```bash
./client.py write {local-file} {destination-id}
./client.py read {id} {destination-local-file}
```
## Read

1. [Design, initial implementation and performance results](BLOG.md)
2. [How to query Cassandra if block exists - performance analysis](QUERY_EXISTS.md)
