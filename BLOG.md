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

To create a 3 nodes cluster test with C* version 3.7.0 `ccm create test -v 3.7.0 -n 3 -s`

To run cqlsh against one of it’s nodes: `ccm node1 cqlsh`

To start a cluster: `ccm start`

To add node to the cluster: `ccm add node4 -i 127.0.0.4 -j 7400 -b`
