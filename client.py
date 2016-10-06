import os
import argparse
from rabin import chunksizes_from_filename as chunker
from pyblake2 import blake2b
from threading import Thread

from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
from cassandra import ConsistencyLevel
from collections import namedtuple
from contexttimer import timer
from Queue import Queue

# TODO: blocks stats = how much in duplicates etc
# TODO: time_stats
# TODO: parallel write_blocks / non-blocking after reading

Block = namedtuple('Block', ['offset', 'size', 'hash', 'is_new'])


class StorageClient(object):
    def __init__(self, cassandra_cluster):
        from cassandra.query import named_tuple_factory
        self.cluster = cassandra_cluster
        self.session = self.cluster.connect()
        self.session.set_keyspace('dedup')
        self.session.row_factory = named_tuple_factory
        self.prepared_insert_block = self.session.prepare(
            "insert into blocks(block_hash, block_size, content) values (?,?,?);")

    def maybe_store_block(self, block_hash, block_data):
        block_exists = self.block_exists(block_hash, block_size=len(block_data))
        if block_exists:
            self.inc_block_usage(block_hash, block_size=len(block_data))
            return False
        else:
            self.store_block(block_hash, block_data)
            return True

    def block_exists(self, block_hash, block_size):
        q = "select block_hash from blocks where block_hash='{h}' and block_size={s} limit 1;".format(h=block_hash,
                                                                                                      s=block_size);
        out = self.session.execute(q)
        return True if out.current_rows else False

    def inc_block_usage(self, block_hash, block_size):
        q = "update blocks_usage set num_ref = num_ref + 1 where block_hash='{h}' and block_size={s};".format(
            h=block_hash, s=block_size);
        out = self.session.execute(q)

    def store_block(self, block_hash, block_data):
        block_size = len(block_data)
        self.session.execute(self.prepared_insert_block, (block_hash, block_size, block_data))

    def store_file(self, dst_path, blocks):
        q = 'insert into files(path, block_offset, block_hash, block_size) values (?, ?, ?, ?);'
        prep_insert = self.session.prepare(q)
        batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
        for b in blocks:
            batch.add(prep_insert, (dst_path, b.offset, b.hash, b.size))
        self.session.execute(batch)

    def restore_file_blocks(self, path):
        q = "select block_offset, block_hash, block_size from files where path='{p}' order by block_offset asc;".format(
            p=path)
        out = self.session.execute(q)
        return [Block(b.block_offset, b.block_size, b.block_hash, None) for b in out.current_rows]

    def restore_blocks(self, blocks):
        prep_q = self.session.prepare('select content from blocks where block_hash=? and block_size=?')
        for b in blocks:
            out = self.session.execute(prep_q, (b.hash, b.size))
            assert len(out.current_rows) == 1
            yield out.current_rows[0].content


@timer()
def store_file(cass_client, src_path, dst_path):
    # get chunks as list of data sizes to read
    chunks = chunker(src_path)
    blocks = store_blocks(cass_client, src_path, chunks)
    store_file_descriptor(cass_client, dst_path, blocks)
    print_file_blocks_stats(blocks)
    return dst_path, len(chunks)


# TODO: connection pooling would be good idea for storage client
def store_blocks(cass_client, src_path, chunks, num_workers=8):
    ret = []
    # create queue witn N elements
    queue_size = num_workers
    queue = Queue(queue_size)
    # create storage Threads
    def worker():
        while True:
            offset, chunk, block = queue.get()
            h = blake2b(block, digest_size=32)
            bh = h.hexdigest()
            stored = cass_client.maybe_store_block(block_hash=bh, block_data=block)
            ret.append(Block(offset, chunk, bh, stored))
            queue.task_done()

    for i in range(num_workers):
        t = Thread(target=worker)
        t.daemon = True
        t.start()

    # start reading -> queue
    with open(src_path, 'rb') as src_file:
        read_file_in_chunks(src_file, chunks, queue)
    # wait until reading is finished
    queue.join()
    return ret


def read_file_in_chunks(file_obj, chunks, queue):
    offset = 0
    for chunk in chunks:
        data = file_obj.read(chunk)
        if not data:
            break
        x = (offset, chunk, data)
        queue.put(x)
        offset += chunk


def print_file_blocks_stats(blocks):
    size_new_blocks = sum([b.size for b in blocks if b.is_new is True])
    size_existing_blocks = sum([b.size for b in blocks if b.is_new is False])
    print "existing blocks [b]: ", size_existing_blocks
    print "new blocks [b]:", size_new_blocks


def store_file_descriptor(cass_client, dst_path, blocks):
    cass_client.store_file(dst_path, blocks)


@timer()
def restore_file(cass_client, src_path, dst_path):
    blocks = cass_client.restore_file_blocks(src_path)
    with open(dst_path, 'wb') as dst_file:
        for b in cass_client.restore_blocks(blocks):
            dst_file.write(b)


def storage_client():
    nodes = os.getenv('CASSTOR_NODES', '127.0.0.1').split(',')
    cluster = Cluster(nodes)
    return StorageClient(cluster)


#
# MAIN
#

parser = argparse.ArgumentParser(description='Dedup on C* client')
parser.add_argument('command', type=str, help='command: read or write')
parser.add_argument('src', type=str, help='source path')
parser.add_argument('dst', type=str, help='destination path')
args = parser.parse_args()

storage = storage_client()

if args.command == "write":
    store_file(storage, args.src, args.dst)
elif args.command == "read":
    restore_file(storage_client(), args.src, args.dst)
else:
    print "unrecognized command"
    parser.print_help()
