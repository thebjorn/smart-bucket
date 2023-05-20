import datetime
import os
from dkfileutils.path import Path

from sbucket.filetree import LocalFileTree
from sbucket.s3bucket import S3Bucket
from sbucket.s3file import datetime_to_ns, LocalFile, S3File
from sbucket import environment
from sbucket.syncronize import sync_file_tree, sync_file

CURDIR = Path(__file__).parent
DATA = CURDIR / 'data'


def test_file_tree():
    # with DATA.cd():
    #     assert os.getcwd() == str(DATA)
    #     t = LocalFileTree('test1')
    #     assert t.exists
    #     assert list(t) == ['a']

    t = LocalFileTree(DATA / 'test1')
    assert list(t) == ['b']

    dt21 = datetime.datetime(2021, 1, 1, 0, 0, 0, 0)
    # for f in t:
    #     f.timestamp = datetime_to_ns(dt21)

    assert 1


def test_sync_file_tree():
    with DATA.cd():
        print("S3 BUCKET:", environment.S3_TEST_BUCKET_NAME)
        bucket = S3Bucket(environment.S3_TEST_BUCKET_NAME)
        print("FILES:", list(bucket.list_files()))
        ftree = LocalFileTree('test1')
        print("LOCAL FILES:", list(ftree))
        assert bucket.file('syncs3.py').exists
        sync_file(LocalFile('syncs3.py'), bucket.file('syncs3.py'))
        # sync_file_tree(ftree, bucket)
    assert 0

