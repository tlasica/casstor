Essential query for deduplication is check, if particular block exists (is present in the storage).
To check which of the approaches can do better I decided to go with `cassandra-stress` in user mode.

There is a [good post](http://www.datastax.com/dev/blog/improved-cassandra-2-1-stress-tool-benchmark-any-schema) how to use it and very good video from Cassandra Summit 2016 [here](https://youtu.be/it4yqHXu4TE?list=PLm-EPIkBI3YoiA-02vufoEj4CgYvIQgIk).

## Questions to be answered

1. Should I query `blocks` directly or use `existing_blocks`?
2. Which approach should I take for querying existing_blocks table?
3. Should I use row cache?

## cassandra-stress

Cassandra stress let you test in fact one table at a time. So I decided to test `existing_blocks` table. In original CASStor approach it has only primary key and is used as a distributed set. Unfortunately `cassandra-stress` will fail with so simple table as it requires at least one non-pk column to be present.

My stress.existing_blocks.yaml file:
```yaml
keyspace: casstor_meta

table: existing_blocks_2

table_definition: |
    CREATE TABLE casstor_meta.existing_blocks_2 (
    block_hash text PRIMARY KEY, block_size int)

columnspec:
  - name: block_hash
    size: fixed(32)
  - name: block_size
    size: uniform(8192..262200)

insert:
    partitions: fixed(1)

queries:
    exists_one:
        cql: select block_hash, block_size from existing_blocks_2 where block_hash = ? limit 1

    exists_five:
        cql: select block_hash, block_size from existing_blocks_2 where block_hash in (?,?,?,?,?) limit 5

    exists_count:
        cql: select count(*) from existing_blocks_2 where block_hash = ? limit 1
```

As you see I consider three different approaches to check if block exists.




## Links
http://www.sestevez.com/sestevez/CassandraDataModeler/
https://tobert.github.io/pages/als-cassandra-21-tuning-guide.html
