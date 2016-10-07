# CASStor - scalable storage with deduplication on C*

## Introduction
I would like to show that implementing scalable cloud storage with hash based block deduplication 
is quite easy nowadays using existing tools such Cassandra and Spark. 

My goal was to prototype storage with following features:

1. It can store or restore files
2. Implements block deduplication
3. Scales horizontally with more storage / performance demand
4. Keeps configurable number of block copies
5. Space reclamation (removal of unused blocks) can be performed in a “cleanup” time (no writes accepted)

The limitation of having read-only period for cleaning up is important 
as implementing read-write space reclamation of unused blocks is a complex distributed task far beyond this prototype.

### Block deduplication
Basic idea is that each file stored in our system is divided into chunks of limited size. 
As we use smart chunking algorithm (rabin fingerprinting) we assume that some files can share chunks. 
For each chunk system calculates hash (as identifier) and if it is already present in our storage 
it is reused by multiple stored files.
Read more at: https://pibytes.wordpress.com/2013/02/02/deduplication-internals-part-1/

### Scaling and configuration
Scaling is the ability to add new nodes to the cluster to increase storage or performance.
Number of copies of metadata and data can be configured per system.
Each file and block has same number of copies.

### Space reclamation
When files are removed blocks of it are not used anymore. 
But same block (chunks) can be used by other files. 
So it is essential to safely remove blocks that are not used anymore at all.

### Why Cassandra?
I have decided for Apache Cassandra for following reasons:

1. It is scalable and has configurable number of copies / consistency
2. t is open source and easy to use

## Running Cassandra

For development and functional testing it is enoough to run cassandra on local machine using ccm (https://github.com/pcmanus/ccm).  This tool let you easily create and manage a multi-node cluster, start stop nodes, run cqlsh etc.

```bash
ccm create test -v 3.7.0 -n 3 -s  # create a 3 nodes cluster test with C* version 3.7.0 `
ccm status  # print cluster status
ccm node1 cqlsh  # run cqlsh on node1
ccm add node4 -i 127.0.0.4 -j 7400 -b  # add node to the cluster
ccm start node1  # start newly added node
```
## Data model

All the data of files stored in CASStor are kept in the Cassandra database. We need to store individual blocks (chunks) and how files are composed suing those blocks. We need to manage resiliency in case of a hdd or node failure.

### Resiliency

Cassandra keeps data with configurable resiliency using different replication strategies.
Strategy is configured per *keyspace*. 
By configuring keyspaces differently we can achieve different goals.

First of all it may be reasonable to keep data (blocks) and metadata (files) in different keyspaces:

1. we may need to keep different number of copies as the size of data is different or to speed up some operations
2. we may want to perform maintenance on data and metadata separately e.g. repair or calculate amount of data

So lets create two keyspaces to keep 3 replicas of metadata and 2 of data
```
create keyspace casstor_data with replication = {'class': 'SimpleStrategy', 'replication_factor': 3};
create keyspace casstor_meta with replication = {'class': 'SimpleStrategy', 'replication_factor': 2};
```
Now let's say we want to be super resilient and have 2 different data centers in separate locations.
We can ask Cassandra to keep 2 copies in local DC and 1 in remote:
```
create keyspace casstor_data with replication = {'class': 'NetworkTopologyStrategy', 'MAINDC': 3, 'BACKUPDC': 1};
create keyspace casstor_meta with replication = {'class': 'NetworkTopologyStrategy', 'MAINDC': 2, 'BACKUPDC': 1};
```

### Files
We need to describe files we keep in the storage. Each file consists of a list of blocks ordered by its offsets:
```sql
create table files( path text, block_offset bigint,
    block_hash text, block_size int,
    primary key (path, block_offset) );
```
Design decisions:

1. We do not keep separate list of files, it is enough to assume file is present if it has at least one block
2. Primary key (path, block_offset) identifies block in file at offset
3. Partition key path assures that all blocks of the file are within same partition and can be retrieved from 1 cassandra node
4. Clustering key block_offset assures that blocks are organized by offset within the file so we can read them in a good sequence even if we write them in random order
5. At each offset we keep hash of the block and it’s size (mostly for verification)

### Blocks

We have to keep blocks (chunks) and be able to easily find requested block by its ID.
```sql
create table blocks(block_hash text, block_size int, content blob, 
primary key(block_hash, block_size));
```
Design decisions:

1. Blocks are identified by its hash. 
2. Primary key consists of block_hash and block_size
3. At the moment there is no important reason to keep block_size as clustering key
4. C\* partitioner should deal correctly with partition key being already a hash

## Store / Restore Operations

### Write file (store)
```
client.py read {source} {casstor_file_id}
```
The goal of write is to read source file from local filesystem and write it into CASSTOR under casstor_file_id identifier.

Let's start with simple, sequential implementation:

```
chunk source file to list of blocks with (offset, size)
blocks = []
for each chunk:
block = read_chunk(source_file, chunk.size)
H = calculate_hash(block)
If not exist in C* (H):
	cassandra: add (H,b) to blocks table
blocks.append(chunk.offset, H)
cassandra: delete from files where path = destination
for each block in blocks:
	cassandra: add (destination, offset, H) to files table
```
Above implementation has some major performance drawbacks:

* source file is read twice: once for chunking and then for storing data
* cassandra operations can be parallelized to boost performance as the database can handle multiple concurrent writes

#### Chunking
The goal of chunking is to divide file into parts using rabin fingerint (https://en.wikipedia.org/wiki/Rabin_fingerprint) based on file content in the way, that if similar chunks exists in different files  they will be recognized. It takes source file as input and return a list of pairs (offset, size).  

Interesting feature of this algorithm is that there is some average but also maximum block size which let limit amount of memory used during processing data.

I found two libraries for python

* https://github.com/aitjcize/pyrabin
* https://github.com/cschwede/python-rabin-fingerprint

Decided to use (2) as slightly faster.

#### Hashing
For each block we need to calculate hash in the way that the possibility of collision (two blocks having different content will have exactly same hash) is very low. And we need this function to be fast.

I decided to use BLAKE2 (https://en.wikipedia.org/wiki/BLAKE_(hash_function)#BLAKE2)  algorithm mostly because I have not used it before.There is an implementation in python for this algorithm: https://pythonhosted.org/pyblake2/

More reading: https://en.wikipedia.org/wiki/Cryptographic_hash_function#Cryptographic_hash_algorithms

### Read file (restore)
```sql
client.py read {casstor_file_id} {destination_path}
```
Restoring the file from CASSTOR is the operation that takes file blocks from storage and write them to destination file in proper order.

Sequential operation is quite obvious:
```
file_blocks = cassandra: select * from files where path = casstor_file_id order by block_offset;
for each block in file_blocks:
    r = cassandra: select content from blocks where block_hash = block.hash
    write_block(destination_file, r)
```
Similar o write this implementation also has performance drawbacks as blcok retrievel from Cassandra database can be use different cluster nodes (with proper partitioning) and thus should be parallelized. We will get to it later.

