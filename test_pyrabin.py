import time

try:
    from rabin import get_file_fingerprints as chunker
    print "using get_file_fingerprints()"
except:
    from rabin import chunksizes_from_filename as chunker
    print "using chunksizes_from_filename()"


def test(path):
    t0 = time.time()
    fingerprints = chunker(path)
    t1 = time.time()
    return t1-t0, fingerprints

d, f = test('/home/tomasz/Videos/P4212813.MOV')

print "duration:", d
print "fragments:", len(f)

print f
