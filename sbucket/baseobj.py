import abc
import datetime
import os
from abc import ABC
from dataclasses import dataclass
from typing import Literal, Optional

import pytz

from sbucket.utils import files2tree, tree2yaml


class File(abc.ABC):
    def __init__(self, path: str) -> None:
        super().__init__()
        self.path: str = str(path)

    def __str__(self) -> str:
        return self.path

    __repr__ = __str__

    @property
    @abc.abstractmethod
    def exists(self) -> bool:
        pass

    @property
    def missing(self) -> bool:
        return not self.exists

    @property
    @abc.abstractmethod
    def hash(self) -> str:
        pass

    @property
    @abc.abstractmethod
    def last_modified(self) -> datetime.datetime:
        pass

    @property
    @abc.abstractmethod
    def timestamp(self) -> int:
        pass

    @timestamp.setter
    @abc.abstractmethod
    def timestamp(self, timestamp: int):
        pass


@dataclass
class FileInfo:
    path: str
    hash: str
    last_modified: Optional[datetime.datetime] = None
    timestamp: Optional[int] = None
    exists: Optional[bool] = None
    operation: Literal['<', '>', '=', '!'] = None


class FileTree(ABC):

    @abc.abstractmethod
    def __iter__(self):
        pass

    @abc.abstractmethod
    def file(self, path):
        pass

    @abc.abstractmethod
    def all(self) -> list[File]:
        """Should return a 'light' object.
        """
        pass

    def _info_simple(self):
        print(type(self))
        def normalize_path(path):
            return path.replace('\\', '/')

        res = {}
        for obj in self.all():
            res[normalize_path(obj.path)] = FileInfo(
                path=normalize_path(obj.path),
                hash=obj.hash,
                last_modified=obj.last_modified,
                # timestamp=f.timestamp
            )
        return res



    def compare(self, other):
        a = self._info_simple()
        b = other._info_simple()
        res = []

        for path in set(a) - set(b):
            res.append((
                '+B',
                FileInfo(
                    path=path,
                    hash=a[path].hash,
                    last_modified=a[path].last_modified,
                    exists=True,
                    operation='>'
                )
            ))
        for path in set(b) - set(a):
            res.append((
                '+A',
                FileInfo(
                    path=path,
                    hash=b[path].hash,
                    last_modified=b[path].last_modified,
                    exists=False,
                    operation='<'
                )
            ))
        for path in set(a) & set(b):
            if a[path].hash != b[path].hash:
            # if True:
                timediff = max(a[path].last_modified, b[path].last_modified) - min(a[path].last_modified, b[path].last_modified)
                tdiff = ('?', timediff)
                if timediff > datetime.timedelta(seconds=10):
                    if a[path].last_modified < b[path].last_modified:
                        tdiff = ('<', timediff)
                    else:
                        tdiff = ('>', timediff)

                res.append((
                    ('=' if a[path].hash == b[path].hash else '!='),
                    FileInfo(
                        path=path,
                        hash=a[path].hash,
                        last_modified=a[path].last_modified,
                        exists=True,
                        operation='!'
                    ),
                    FileInfo(
                        path=path,
                        hash=b[path].hash,
                        last_modified=b[path].last_modified,
                        exists=True,
                        operation='!'
                    ),
                    tdiff,
                    ('hash', ('=' if a[path].hash == b[path].hash else '!=')),
                ))

        return res

    def __xor__(self, other):
        """Return the operations needed to make self and other equal.
           Using the ``^`` operator.
        """
        a = self.info()
        b = other.info()
        res = {}

        return res

        def getop(f1, f2) -> Literal['<', '>', '=', '!']:
            if f1.hash == f2.hash:
                return '='
            if f1.timestamp == f2.timestamp:
                # different hash same timestamp
                return '!'
            elif f1.timestamp < f2.timestamp:
                # move left
                return '<'
            elif f1.timestamp > f2.timestamp:
                # move right
                return '>'

        for path in set(a) - set(b):
            res[path] = FileInfo(
                path=path,
                hash=a[path].hash,
                timestamp=a[path].timestamp,
                exists=True,
                operation='>'
            )
        for path in set(b) - set(a):
            res[path] = FileInfo(
                path=path,
                hash=b[path].hash,
                timestamp=b[path].timestamp,
                exists=False,
                operation='<'
            )
        for path in set(a) & set(b):
            res[path] = FileInfo(
                path=path,
                hash=a[path].hash,
                timestamp=a[path].timestamp,
                exists=True,
                operation=getop(a[path], b[path])
            )

        return {k : v for k, v in res.items() if v.operation != '='}

    def __repr__(self):
        return tree2yaml(files2tree(sorted(self)))

    def __eq__(self, other):
        return sorted(self) == sorted(other)


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
