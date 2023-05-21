import json
import botocore
import logging

from sbucket.baseobj import File
from sbucket.localfile import LocalFile

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


def local_timezone():
    """Return the local timezone.
    """
    return 'CET'


def datetime_to_ns(dt):
    """Return the number of nanoseconds since the epoch.
    """
    return int(dt.timestamp() * 1e9)


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

    def update_tags(self, tags):
        self._ensure_exists()
        self._obj.put(Tagging=tags)

    def __getattr__(self, attr):
        if attr in self.__dict__:
            return self.__dict__[attr]
        if hasattr(self._obj, attr):
            return getattr(self._obj, attr)
        raise AttributeError(attr)
