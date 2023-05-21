import datetime
import json
import os

import boto3
from devtools import debug
from dkfileutils.path import Path

from sbucket.cache import BucketObject
from sbucket.filetree import LocalFileTree
from sbucket.s3bucket import S3Bucket
from sbucket.s3file import datetime_to_ns, S3File
from sbucket.localfile import LocalFile
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


def test_compare():
    bucket = S3Bucket(environment.S3_TEST_BUCKET_NAME)
    t = LocalFileTree(DATA / 'test1')
    assert bucket == t


def test_bucket_hash():
    b = S3Bucket(environment.S3_TEST_BUCKET_NAME)
    bucket = b.bucket
    for obj in bucket.objects.all():
        # obj2 = b.s3.ObjectSummary(b.bucket_name, obj.key)
        bobj = BucketObject(bucket, summary=obj)
        # bobj = BucketObject(bucket, obj.key)
        print(bobj.path, bobj.last_modified, bobj.hash)
    assert 0
    # assert [f.hash for f in bucket] == ['a', 'b']


def test_xor():
    bucket = S3Bucket(environment.S3_TEST_BUCKET_NAME)
    # assert bucket.info() == []
    t = LocalFileTree(DATA / 'test1')
    # assert t.info() == []
    # assert t ^ bucket == {}
    tmp = t.compare(bucket)
    debug(tmp)
    assert tmp == []
    assert 0
    # 10.06 seconds


def test_filetree_upload():
    bucket = S3Bucket(environment.S3_TEST_BUCKET_NAME)
    t = LocalFileTree(DATA / 'test1')
    t.upload(bucket)
    assert list(bucket) == list(t)


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


def test_repr():
    t = LocalFileTree(DATA / 'test1')
    print(repr(t))
    assert 0


def test_bucket_repr():
    bucket = S3Bucket(environment.S3_TEST_BUCKET_NAME)
    print(repr(bucket))
    assert 0


def test_repr_equal():
    bucket = S3Bucket(environment.S3_TEST_BUCKET_NAME)
    t = LocalFileTree(DATA / 'test1')
    assert repr(bucket) == repr(t)
