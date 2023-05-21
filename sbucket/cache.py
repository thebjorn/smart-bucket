"""
The bucket.objects.all() method returns a list of ObjectSummary objects, which
contains (some) metadata about the object, but not the object itself.

To get the metadata you need to call the bucket.Object(path).get() method.

    for osum in bucket.objects.all():           # 1 network hit per 1000 objects
        # osum is a ObjectSummary object
        # osum.key is the path of the object

        obj = bucket.Object(osum.key)           # 1 network hit per object
        json.loads(obj.metadata['tagging'])['mtime']

        # ...also obj.download_file(...) etc.

it is imperative to keep the network hits as low as possible.
"""
import json
from dataclasses import dataclass
import logging

log = logging.getLogger(__name__)

# import botocore


class BucketObject:
    def __init__(self, bucket, path=None, summary=None, object=None, mtime=None):
        self.bucket = bucket
        self._path = path
        self._summary = summary
        self._object = object
        self._mtime = mtime

        self._hash = None
        self._size = None
        self._last_modified = None

    def _fetch_obj(self):
        log.info("Fetching object: %s", self.path)
        self._object = self.bucket.Object(self.path)

    @property
    def path(self):
        if not self._path:
            if self._summary:
                self._path = self._summary.key
            else:
                if not self._object:
                    self._fetch_obj()
                self._path = self._object.key
        return self._path

    @property
    def hash(self):
        if not self._hash:
            if self._summary:
                self._hash = self._summary.e_tag[1:-1]
            else:
                if not self._object:
                    self._fetch_obj()
                self._hash = self._object.e_tag[1:-1]
        return self._hash

    @property
    def size(self):
        if self._size is None:
            if self._summary:
                self._size = self._summary.size
            else:
                if not self._object:
                    self._fetch_obj()
                self._size = self._object.content_length
        return self._size

    @property
    def last_modified(self):
        if self._last_modified is None:
            if self._summary:
                self._last_modified = self._summary.last_modified
            else:
                if not self._object:
                    self._fetch_obj()
                self._last_modified = self._object.last_modified
        return self._last_modified

    @property
    def mtime(self):
        if not self._mtime:
            if not self._object:
                self._fetch_obj()
            self._mtime = json.loads(self._object.metadata['tagging'])['mtime']
        return self._mtime

    # def exists(self):
    #     if self._summary:
    #         return True
    #     if not self._object:
    #         self._fetch_obj()
    #     try:
    #         self._object.load()
    #         return True
    #     except botocore.exceptions.ClientError as e:
    #         if e.response['Error']['Code'] == "404":
    #             return False
    #         else:
    #             raise


class BucketCache:
    def __init__(self, bucket):
        self.bucket = bucket
        self._cache = {}

    def get(self, path):
        if path not in self._cache:
            self._cache[path] = BucketObject(self.bucket, path)
        return self._cache[path]




# ObjectSummary = namedtuple('ObjectSummary', ['key', 'size', 'last_modified'])

@dataclass
class ObjectSummary:
    path: str
    hash: str
    size: int
    last_modified: str
