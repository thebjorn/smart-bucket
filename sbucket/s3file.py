import abc
import datetime
import json
import pytz
import os
import boto3
import botocore
import logging
from urllib import parse
from devtools import debug

log = logging.getLogger(__name__)


def hash_file(fname):
    import hashlib
    h = hashlib.md5()
    with open(fname, 'rb', buffering=0) as f:
        for b in iter(lambda: f.read(128 * 1024), b''):
            h.update(b)
    return h.hexdigest()


def local_timezone():
    """Return the local timezone.
    """
    return 'CET'


def get_timestamp(path) -> datetime.datetime:
    """Return the timestamp of the file at path with no fractional seconds.
    """
    timestamp = os.path.getmtime(path)
    # tz = pytz.timezone(local_timezone())
    tz = pytz.timezone('UTC')
    dt = datetime.datetime.fromtimestamp(timestamp)
    dt = datetime.datetime(dt.year, dt.month, dt.day, dt.hour, dt.minute,
                           dt.second)
    return tz.localize(dt)


class File(abc.ABC):
    def __init__(self, path) -> None:
        super().__init__()
        self.path = path

    @abc.abstractmethod
    def exists(self):
        pass

    @property
    @abc.abstractmethod
    def exists(self):
        pass

    @property
    @abc.abstractmethod
    def hash(self):
        pass

    @property
    @abc.abstractmethod
    def timestamp(self):
        pass

    @timestamp.setter
    @abc.abstractmethod
    def timestamp(self, timestamp):
        pass


class LocalFile(File):
    @property
    def exists(self) -> bool:
        return os.path.exists(self.path)

    @property
    def hash(self) -> str:
        return hash_file(self.path)

    @property
    def timestamp(self) -> int:
        s = os.stat(self.path)
        return s.st_mtime_ns

    @timestamp.setter
    def timestamp(self, timestamp):
        os.utime(self.path, ns=(timestamp, timestamp))


class S3File(File):
    def __init__(self, bucket: 'S3Bucket', path):
        super().__init__(path)

        self.bucket = bucket
        self.s3 = bucket.s3
        s3bucket = self.s3.Bucket(self.bucket.bucket_name)
        self._obj = s3bucket.Object(self.path)
        self._exists = None
        self._header = None

    @property
    def exists(self):
        if self._exists is None:
            try:
                self._exists = self._obj.last_modified is not None
            except botocore.exceptions.ClientError as e:
                if e.response['Error']['Code'] == "404":
                    self._exists = False
                else:
                    raise
        return self._exists

    def _ensure_exists(self):
        if not self.exists:
            raise FileNotFoundError

    @property
    def hash(self):
        self._ensure_exists()
        return self.e_tag[1:-1]

    @property
    def timestamp(self):
        self._ensure_exists()
        try:
            return json.loads(self._obj.metadata['tagging'])['mtime']
        except KeyError:
            ts = self._obj.last_modified
            return int(ts.timestamp() * 10**6) * 1000

    def download(self):
        self._ensure_exists()
        self._obj.download_file(self.path)
        localfile = LocalFile(self.path)
        localfile.timestamp = self.timestamp

    def update_tags(self, tags):
        self._ensure_exists()
        self._obj.put(Tagging=tags)

    def __getattr__(self, attr):
        if attr in self.__dict__:
            return self.__dict__[attr]
        if hasattr(self._obj, attr):
            return getattr(self._obj, attr)
        raise AttributeError(attr)
