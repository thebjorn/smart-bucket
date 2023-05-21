import datetime
import os

from sbucket.baseobj import File, get_timestamp


def hash_file(fname):
    import hashlib
    h = hashlib.md5()
    with open(fname, 'rb', buffering=0) as f:
        for b in iter(lambda: f.read(128 * 1024), b''):
            h.update(b)
    return h.hexdigest()


class LocalFile(File):
    def __init__(self, path, tree) -> None:
        self.tree = tree
        super().__init__(path)

    def __lt__(self, other):
        other = other.path if isinstance(other, File) else other
        return self.path < other

    def __eq__(self, other):
        other = other.path if isinstance(other, File) else other
        return self.path == other

    def _getpath(self):
        return self.tree.root / self.path

    @property
    def exists(self) -> bool:
        return self._getpath().exists()

    @property
    def hash(self) -> str:
        return hash_file(self._getpath())

    @property
    def last_modified(self) -> datetime.datetime:
        return get_timestamp(self._getpath())
        # return datetime.datetime.fromtimestamp(self.timestamp)

    @property
    def timestamp(self) -> int:
        s = os.stat(self._getpath())
        return s.st_mtime_ns

    @timestamp.setter
    def timestamp(self, timestamp):
        os.utime(self._getpath(), ns=(timestamp, timestamp))

    def upload(self, bucket):
        bucket.copy_to_s3(self._getpath(), self.path)
