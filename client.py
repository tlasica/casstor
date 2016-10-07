#! /usr/bin/env python

import os
import time
import argparse
from rabin import chunksizes_from_filename as chunker
from pyblake2 import blake2b
from threading import Thread

from cassandra.cluster import Cluster
from cassandra.query import BatchStatement, SimpleStatement
from cassandra import ConsistencyLevel
from collections import namedtuple
from Queue import Queue, PriorityQueue


Block = namedtuple('Block', ['offset', 'size', 'hash', 'is_new', 'content'])


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
        if not block_exists:
            self.store_block(block_hash, block_data)
        self.inc_block_usage(block_hash, block_size=len(block_data))
        return not block_exists

    def block_exists(self, block_hash, block_size):
        q = "select block_hash from blocks where block_hash='{h}' and block_size={s} limit 1;".format(h=block_hash,
                                                                                                      s=block_size)
        out = self.session.execute(q)
        return True if out.current_rows else False

    def inc_block_usage(self, block_hash, block_size):
        q = "update blocks_usage set num_ref = num_ref + 1 where block_hash='{h}' and block_size={s};".format(
            h=block_hash, s=block_size)
        out = self.session.execute(q)

    def store_block(self, block_hash, block_data):
        block_size = len(block_data)
        self.session.execute(self.prepared_insert_block, (block_hash, block_size, block_data))

    def store_file(self, dst_path, blocks):
        q = 'insert into files(path, block_offset, block_hash, block_size) values (?, ?, ?, ?);'
        prep_insert = self.session.prepare(q)
        curr_batch_size = 0
        max_batch_size = 1001
        batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
        batch.add("delete from files where path='{0}';".format(dst_path))
        for b in blocks:
            batch.add(prep_insert, (dst_path, b.offset, b.hash, b.size))
            curr_batch_size += 1
            if curr_batch_size == max_batch_size:
                # execute current batch and start new one
                curr_batch_size = 0
                self.session.execute(batch)
                batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)

        if curr_batch_size > 0:
            self.session.execute(batch)

    def restore_file_blocks(self, path):
        q = "select block_offset, block_hash, block_size from files where path='{p}' order by block_offset asc;".format(
            p=path)
        for b in self.session.execute(q):
            yield Block(b.block_offset, b.block_size, b.block_hash, None, None)

    # TODO: is sharing prepare statement safe?
    def restore_blocks(self, blocks, output_queue, num_workers=1):
        # create queue and put all blocks as tasks
        tasks_queue = Queue()
        for bl in blocks:
            tasks_queue.put(bl)

        # start workers
        def worker():
            prep_q = self.session.prepare('select content from blocks where block_hash=? and block_size=?')
            prep_q.consistency_level = ConsistencyLevel.LOCAL_ONE
            while True:
                b = tasks_queue.get()
                out = self.session.execute(prep_q, (b.hash, b.size))
                assert len(out.current_rows) == 1
                q_item = Block(b.offset, b.size, b.hash, None, out.current_rows[0].content)
                tasks_queue.task_done()  # here or after output.put()?
                output_queue.put((b.offset, q_item))

        for i in range(num_workers):
            t = Thread(target=worker)
            t.setDaemon(True)
            t.start()

            # wait for all workers to stop
            # we cannot wait here as we should not block this function
            # tasks_queue.join()


def store_file(cass_client, src_path, dst_path):
    t0 = time.time()
    # get chunks as list of data sizes to read
    chunks = chunker(src_path)
    blocks = store_blocks(cass_client, src_path, chunks)
    store_file_descriptor(cass_client, dst_path, blocks)
    dur = time.time() - t0
    print_store_stats(blocks, dur)
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
            ret.append(Block(offset, chunk, bh, stored, None))
            queue.task_done()

    for i in range(num_workers):
        t = Thread(target=worker)
        t.setDaemon(True)
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


def print_store_stats(blocks, duration):
    size_new_blocks = sum([b.size for b in blocks if b.is_new is True])
    size_existing_blocks = sum([b.size for b in blocks if b.is_new is False])
    total_size = size_existing_blocks + size_new_blocks
    print "existing blocks [b]: ", size_existing_blocks
    print "new blocks [b]:", size_new_blocks
    print "total size [b]:", total_size
    print "duplication ratio:", 100 * float(size_existing_blocks)/total_size
    print "total duration [s]:", duration
    thru = total_size / duration / 1024.0 / 1024.0
    print "throughput MB/s:", thru


def store_file_descriptor(cass_client, dst_path, blocks):
    cass_client.store_file(dst_path, blocks)


def restore_file(cass_client, src_path, dst_path):
    t0 = time.time()
    # restore file blocks
    blocks = [b for b in cass_client.restore_file_blocks(src_path)]
    # create queue for blocks to be restored, in fact it should be probably priority queue to preserve order?
    # in this queue we put Block with all the description and content
    # WARNING: limiting output_queue size can lead to a deadlock
    output_queue = PriorityQueue()
    cass_client.restore_blocks(blocks, output_queue, num_workers=4)
    total_size_to_restore = sum([b.size for b in blocks])
    print "total size to restore:", total_size_to_restore
    print "numer of block to restore:", len(blocks)
    max_output_queue_size = 0
    offsets_to_write = set([b.offset for b in blocks])
    with open(dst_path, 'wb') as dst_file:
        max_output_queue_size = max(max_output_queue_size, output_queue.qsize())
        # now we wait for each block to be read
        for expected_block in blocks:
            while True:
                offset, block = output_queue.get()
                if offset == expected_block.offset:
                    assert block.content is not None
                    dst_file.write(block.content)
                    offsets_to_write.remove(offset)
                    break
                else:
                    output_queue.put((offset, block))
    assert len(offsets_to_write) == 0, "Missing offsets: " + str(offsets_to_write)
    print "max output queue size:", max_output_queue_size
    duration = time.time() - t0
    print "total duration [s]:", duration
    thru = total_size_to_restore / duration / 1024.0 / 1024.0
    print "throughput MB/s:", thru


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
