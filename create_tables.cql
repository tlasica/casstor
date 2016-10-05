create keyspace dedup with replication = {'class':'SimpleStrategy', 'replication_factor':2};

use dedup;

// we want to keep whole partition on same node, but
// wa also want to sort or filter by offset
create table files(path text, block_offset bigint, block_hash text, block_size int, primary key (path, block_offset) );

// blocks have unique hash and block_size
// we cannot keep counter in the same table as data (C* limitation)
create table blocks(block_hash text, block_size int, content blob, primary key(block_hash, block_size));
create table blocks_usage(block_hash text, block_size int, num_ref counter, primary key(block_hash, block_size));

// insert file blocks, CL=QUORUM
insert into files(path, block_offset, block_hash) values ('/home/tomasz/example.file', 0, 'somehash');
insert into files(path, block_offset, block_hash) values ('/home/tomasz/example.file', 1000, 'somehash1');
insert into files(path, block_offset, block_hash) values ('/home/tomasz/example.file', 2000, 'somehash2');

// how to check the file is present, CL=QUORUM
select * from files where path='/home/tomasz/example.file' and block_offset=0;

// how to remove the file, CL=QUORUM
delete from files where path='/home/tomasz/example.file'
