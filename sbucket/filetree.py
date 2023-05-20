import os
from abc import ABC, abstractmethod

from dkfileutils.path import Path

from sbucket.s3file import LocalFile


class FileTree(ABC):
    def __init__(self, root) -> None:
        super().__init__()
        self.root = root


class LocalFileTree(FileTree):
    def __init__(self, root) -> None:
        super().__init__(root)


    @property
    def exists(self) -> bool:
        return os.path.exists(self.root)

    def __str__(self):
        return self.root

    def local_file(self, path):
        return LocalFile(path)

    def __iter__(self):
        """Yield all the files in the tree.
        """
        for root, dirs, files in os.walk(self.root):
            r = Path(root)
            for file in sorted(files):
                yield LocalFile(r / file, self.root)

    @property
    def files(self):
        """Return a list of all the files in the tree.
        """
        return list(self)

    def timestamp(self, path) -> int:
        """Return the timestamp of the file at path in the S3 bucket.
        """
        return os.stat(path).st_mtime_ns

    def contents(self, path) -> str:
        """Return the contents of the file at path in the S3 bucket.
        """
        with open(path, 'r') as f:
            return f.read()



