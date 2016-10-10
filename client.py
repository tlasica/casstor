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
from collections import namedtuple, deque
from Queue import Queue, PriorityQueue

from contexttimer import timer

Block = namedtuple('Block', ['offset', 'size', 'hash', 'is_new', 'content'])


class StorageClient(object):
    def __init__(self, cassandra_cluster, data_ks='casstor_data', meta_ks='casstor_meta'):
        from cassandra.query import named_tuple_factory
        self.cluster = cassandra_cluster
        self.session = self.cluster.connect()
        self.ks_data = data_ks
        self.ks_meta = meta_ks
        self.session.row_factory = named_tuple_factory
        self.prepared_insert_block = self.session.prepare(
            "insert into {ks}.blocks(block_hash, block_size, content) values (?,?,?);".format(ks=self.ks_data))
        self.prepared_check_block = self.session.prepare(
            "select count(*) from {ks}.blocks where block_hash=?;".format(ks=self.ks_data))

    def maybe_store_block(self, block_hash, block_data):
        block_exists = self.block_exists(block_hash, block_size=len(block_data))
        if not block_exists:
            self.store_block(block_hash, block_data)
        # self.inc_block_usage(block_hash, block_size=len(block_data))
        return not block_exists

    def block_exists(self, block_hash, block_size):
        out = self.session.execute(self.prepared_check_block, [block_hash])
        return True if out.current_rows[0].count > 0 else False

    def inc_block_usage(self, block_hash, block_size):
        q = "update {ks}.blocks_usage set num_ref = num_ref + 1 where block_hash='{h}' and block_size={s};".format(
            ks=self.ks_meta, h=block_hash, s=block_size)
        out = self.session.execute(q)

    def store_block(self, block_hash, block_data):
        block_size = len(block_data)
        self.session.execute(self.prepared_insert_block, (block_hash, block_size, block_data))

    def maybe_store_chunks(self, chunks):
        assert len(chunks) <= 5
        # let's check which of them exists
        q = "select block_hash, block_size from {ks}.blocks where block_hash in (?,?,?,?,?) limit 5;".format(
            ks=self.ks_data)
        prep_check_block = self.session.prepare(q)
        hashes = [c.hash for c in chunks]
        hashes = (hashes + ['0'] * 5)[:5]
        existing_blocks = {r.block_hash: r.block_size for r in self.session.execute(prep_check_block, hashes)}
        ret = []
        for c in chunks:
            exists = existing_blocks.get(c.hash) is not None
            if not exists:
                self.store_block(c.hash, c.content)
            ret.append(Block(c.offset, c.size, c.hash, not exists, None))
        return ret

    def store_file(self, dst_path, blocks):
        self.session.execute("delete from {ks}.files where path='{p}';".format(ks=self.ks_meta, p=dst_path))
        q = 'insert into files(path, {ks}.block_offset, block_hash, block_size) values (?, ?, ?, ?);'.format(
            ks=self.ks_meta)
        prep_insert = self.session.prepare(q)
        curr_batch_size = 0
        max_batch_size = 101
        batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
        for b in blocks:
            batch.add(prep_insert, (dst_path, b.offset, b.hash, b.size))
            curr_batch_size += 1
            if curr_batch_size >= max_batch_size:
                # execute current batch and start new one
                curr_batch_size = 0
                self.session.execute(batch)
                batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)

        if curr_batch_size > 0:
            self.session.execute(batch)

    def restore_file_blocks(self, path):
        q = "select block_offset, block_hash, block_size from {ks}.files where path='{p}' order by block_offset asc;".format(
            ks=self.ks_meta, p=path)
        for b in self.session.execute(q):
            yield Block(b.block_offset, b.block_size, b.block_hash, None, None)

    # TODO: is sharing prepare statement safe?
    def restore_blocks(self, blocks, output_queue, num_workers=1):
        # create queue and put all blocks as tasks
        tasks_queue = deque(blocks)

        def get_tasks_from_queue(n):
            ret = []
            try:
                for i in xrange(n):
                    ret.append(tasks_queue.pop())
            except IndexError:
                pass
            return ret

        # start workers
        def worker():
            prep_q = self.session.prepare(
                'select block_hash, content from {ks}.blocks where block_hash in (?,?,?,?,?) limit 5;'.format(ks=self.ks_data))
            prep_q.consistency_level = ConsistencyLevel.ONE
            batch_size = 5
            while True:
                # read N blocks
                tasks = get_tasks_from_queue(batch_size)
                if not tasks:
                    break
                hash_list = [t.hash for t in tasks]
                hash_list = (hash_list + ['0'] * batch_size)[:batch_size]  # trick to fill list with 0s
                out = self.session.execute(prep_q, hash_list)
                assert len(out.current_rows) == len(tasks)
                retrieved_blocks = {r.block_hash: r.content for r in out}
                # add retrieved blocks to output queue
                for b in tasks:
                    q_item = Block(b.offset, b.size, b.hash, None, retrieved_blocks.get(b.hash))
                    if q_item.content is not None:
                        output_queue.put((b.offset, q_item))
                # finish thread if no enough tasks in the queue
                if len(tasks) < batch_size:
                    break

        for i in range(num_workers):
            t = Thread(target=worker)
            t.setDaemon(True)
            t.start()

            # wait for all workers to stop
            # we cannot wait here as we should not block this function
            # tasks_queue.join()


@timer()
def check_block_exists(cass_client, blocks):
    for b in blocks:
        cass_client.block_exists(b.hash, b.size)


def store_file(cass_client, src_path, dst_path):
    t0 = time.time()
    chunks = chunker(src_path)  # get chunks as list of data sizes to read
    blocks = store_blocks(cass_client, src_path, chunks)
    store_file_descriptor(cass_client, dst_path, blocks)
    dur = time.time() - t0
    print_store_stats(blocks, dur)
    # check_block_exists(cass_client, blocks)
    return dst_path, len(chunks)


# TODO: connection pooling would be good idea for storage client
def store_blocks(cass_client, src_path, chunks, num_workers=4):
    ret = []
    # create queue with N elements
    queue_size = num_workers
    queue = Queue(queue_size)

    # create storage Threads
    def worker():
        while True:
            chunks = queue.get()
            # calculate hash for chunks
            hashes = []
            for c in chunks:
                h = blake2b(c.content, digest_size=32)
                hashes.append(h.hexdigest())
            # confirm exists or store blocks from chunks
            chunks_with_hashes = [Block(c.offset, c.size, h, None, c.content) for c, h in zip(chunks, hashes)]
            stored_blocks = cass_client.maybe_store_chunks(chunks_with_hashes)
            # add to results, work done
            ret.extend(stored_blocks)
            queue.task_done()

    for i in range(num_workers):
        t = Thread(target=worker)
        t.setDaemon(True)
        t.start()

    # start reading -> queue
    with open(src_path, 'rb') as src_file:
        read_file_in_chunks(src_file, chunks, queue, batch_size=5)
    # wait until reading is finished
    queue.join()
    return ret


def read_file_in_chunks(file_obj, chunks, queue, batch_size=1):
    offset = 0
    batch = []
    for chunk_size in chunks:
        data = file_obj.read(chunk_size)
        if not data:
            break
        batch.append(Block(offset, chunk_size, None, None, data))
        if len(batch) == batch_size:
            queue.put(batch)
            batch = []
        offset += chunk_size

    if batch:
        queue.put(batch)


def print_store_stats(blocks, duration):
    size_new_blocks = sum([b.size for b in blocks if b.is_new is True])
    size_existing_blocks = sum([b.size for b in blocks if b.is_new is False])
    total_size = size_existing_blocks + size_new_blocks
    print "existing blocks [b]: ", size_existing_blocks
    print "new blocks [b]:", size_new_blocks
    print "total size [b]:", total_size
    print "duplication ratio:", 100 * float(size_existing_blocks) / total_size
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
