import json
import os
import textwrap
from io import StringIO

from dkfileutils.path import Path
from yamldirs.yamldirs_cmd import directory2yaml

from sbucket.baseobj import FileTree
from sbucket.localfile import LocalFile
from sbucket.utils import files2tree, tree2yaml


class LocalFileTree(FileTree):
    def __init__(self, root) -> None:
        self.root = root

    @property
    def exists(self) -> bool:
        return os.path.exists(self.root)

    def __str__(self):
        return self.root

    # def __repr__(self):
    #     return tree2yaml(files2tree(sorted(self)))

    def upload(self, bucket):
        with Path(self.root).cd():
            for file in self:
                file.upload(bucket)

    def file(self, path):
        return LocalFile(path, self)

    def __iter__(self):
        """Yield all the files in the tree.
        """
        for root, dirs, files in os.walk(self.root):
            r = Path(root).relpath(self.root)
            for file in sorted(files):
                yield LocalFile(r / file, self)

    @property
    def files(self):
        """Return a list of all the files in the tree.
        """
        return list(self)

    # not a dir-op
    # def timestamp(self, path) -> int:
    #     """Return the timestamp of the file at path in the S3 bucket.
    #     """
    #     return os.stat(path).st_mtime_ns

    # not a dir-op
    # def contents(self, path) -> str:
    #     """Return the contents of the file at path in the S3 bucket.
    #     """
    #     with open(path, 'r') as f:
    #         return f.read()
