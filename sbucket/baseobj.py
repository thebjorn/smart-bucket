import abc
from abc import ABC
from dataclasses import dataclass
from typing import Literal, Optional

from sbucket.utils import files2tree, tree2yaml


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


@dataclass
class FileInfo:
    path: str
    hash: str
    timestamp: int
    exists: Optional[bool] = None
    operation: Literal['<', '>', '=', '!'] = None


class FileTree(ABC):

    @abc.abstractmethod
    def __iter__(self):
        pass

    @abc.abstractmethod
    def file(self, path):
        pass

    def info(self):
        def normalize_path(path):
            return path.replace('\\', '/')

        res = {}
        for fname in self:
            f = self.file(fname)
            res[normalize_path(f.path)] = FileInfo(
                path=normalize_path(f.path),
                hash=f.hash,
                timestamp=f.timestamp
            )
        return res

    def __xor__(self, other):
        """Return the operations needed to make self and other equal.
           Using the ``^`` operator.
        """
        a = self.info()
        b = other.info()
        res = {}

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
