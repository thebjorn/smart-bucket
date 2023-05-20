import abc
import datetime
import json
import pytz
import os
import botocore
import logging

log = logging.getLogger(__name__)


# def set_ntfs_extended_attributes(path, attrs):
#     """Set the NTFS extended attributes of the file at path.
#     """
#     import win32api
#     import win32security
#     import ntsecuritycon as con
#     import pywintypes
#
#     # Get the file's security descriptor
#     sd = win32security.GetFileSecurity(path, win32security.DACL_SECURITY_INFORMATION)
#
#     # Create a new security descriptor
#     sd_new = win32security.SECURITY_DESCRIPTOR()
#
#     # Get the DACL from the security descriptor
#     dacl = sd.GetSecurityDescriptorDacl()
#
#     # Add the ACEs to the DACL
#     for ace in attrs:
#         dacl.AddAccessAllowedAce(win32security.ACL_REVISION_DS, con.FILE_GENERIC_READ, ace)
#
#     # Set the DACL in the new security descriptor
#     sd_new.SetSecurityDescriptorDacl(1, dacl, 0)
#
#     # Set the new security descriptor in the file
#     win32security.SetFileSecurity(path, win32security.DACL_SECURITY_INFORMATION, sd_new)

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


def datetime_to_ns(dt):
    """Return the number of nanoseconds since the epoch.
    """
    return int(dt.timestamp() * 1e9)


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
    def __init__(self, path: str) -> None:
        super().__init__()
        self.path: str = str(path)

    def __str__(self):
        return self.path

    __repr__ = __str__

    @property
    @abc.abstractmethod
    def exists(self):
        pass

    @property
    def missing(self):
        return not self.exists

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
    def __init__(self, path, root=None) -> None:
        if root is not None:
            path = os.path.relpath(path, root)
        super().__init__(path)

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
    def __init__(self, bucket, path):
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
                log.info("Error checking if file (%s) exists: %s", self.path, e)
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

    def upload(self):
        self.bucket.copy_to_s3(self.path)
        self._exists = True

    def update_tags(self, tags):
        self._ensure_exists()
        self._obj.put(Tagging=tags)

    def __getattr__(self, attr):
        if attr in self.__dict__:
            return self.__dict__[attr]
        if hasattr(self._obj, attr):
            return getattr(self._obj, attr)
        raise AttributeError(attr)
