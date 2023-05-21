import datetime
import json
import pytz
import os
import boto3
import logging

from yamldirs.yamldirs_cmd import files2yaml

from .baseobj import File, FileTree, get_timestamp
from .cache import BucketObject
from .environment import *
# from urllib import parse
# from devtools import debug
from .s3file import S3File
from .localfile import hash_file, LocalFile
from .utils import files2tree, tree2yaml

log = logging.getLogger(__name__)


class S3Bucket(FileTree):
    def __init__(self, bucket_name, *, use_aws_conf=False,
                 session=None, access_key=None, secret_key=None):
        self.bucket_name = bucket_name
        if use_aws_conf:
            self.session = boto3.Session()
        elif session is None and access_key is None and secret_key is None:
            self.session = boto3.Session(
                aws_access_key_id=AWS_ACCESS_KEY_ID,
                aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            )
        else:
            self.session = session or boto3.Session(
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
            )
        self.s3 = self.session.resource('s3')
        self.bucket = self.s3.Bucket(self.bucket_name)
        self._cache = {}

    def __str__(self):
        return self.bucket_name

    # def __repr__(self):
    #     return tree2yaml(files2tree(sorted(self)))

    def file(self, path):
        return S3File(self, str(path))

    def all(self):
        for obj in self.bucket.objects.all():
            yield BucketObject(self, summary=obj)

    def list_files(self):
        """Return a list of all files in the S3 bucket.
        """
        self._cache = {}
        for obj in self.bucket.objects.all():
            # XXX: obj is here a s3.ObjectSummary (e_tag, key, last_modified, size)
            self._cache[obj.key] = obj
            yield obj.key

    def __iter__(self):
        yield from self.list_files()

    def last_modified(self, path):
        """Return the last modified time of the file at path in the S3 bucket.
        """
        if path not in self._cache:
            self._cache[path] = self.bucket.Object(path)
        return self._cache[path].last_modified

    def contents(self, path):
        """Return the contents of the file at path in the S3 bucket.
        """
        return self.bucket.Object(path).get()['Body'].read().decode('utf-8')

    def exists(self, path):
        """Return True if the file at path exists in the S3 bucket.
        """
        if path not in self._cache:
            self._cache[path] = self.bucket.Object(path)
        return self._cache[path]

    def sync_file(self, path):
        """Synchronize the file at path in the S3 bucket.
        """
        s3file = S3File(self, path)
        localfile = LocalFile(path)

        if s3file.exists and not localfile.exists:
            # if we don't have the file, download it
            s3file.download()

        if localfile.exists and not s3file.exists:
            # if s3 doesn't have the file, upload it
            self.copy_to_s3(path)

        if localfile.hash == s3file.hash:
            # if the hashes match, we're done
            log.debug("Hashes match for %s", path)
            return

        if localfile.timestamp > s3file.timestamp:
            # if the local file is newer, upload it
            self.copy_to_s3(path)
        else:
            # if the s3 file is newer, download it
            s3file.download()

    def copy_from_s3(self, path):
        """Copy the file at path in the S3 bucket to the local directory.
        """
        s3file = S3File(self, path)
        s3file.download()

    def read_header(self, path):
        """Return the header of the file at path in the S3 bucket.
        """
        return self.s3.meta.client.head_object(Bucket=self.bucket_name, Key=path)

    def get_metadata(self, path):
        """Return the metadata of the file at path in the S3 bucket.
        """
        if path not in self._cache:
            self._cache[path] = self.bucket.Object(path)
        return self._cache[path].metadata
        # head = self.read_header(path)
        # return head['Metadata']

    def get_hash(self, path):
        """Return the hash of the file at path in the S3 bucket.
        """
        if path not in self._cache:
            self._cache[path] = self.bucket.Object(path)
        return self._cache[path].e_tag.strip('"')

    def get_tags(self, path):
        """Return the tags of the file at path in the S3 bucket.

           Ref. tags set in :meth:`copy_to_s3`.
        """
        return json.loads(self.get_metadata(path)['tagging'])

    def delete_all_files_from_bucket(self):
        """Delete all files from the S3 bucket.
        """
        self.bucket.objects.delete()

    def copy_to_s3(self, local_file, path=None):
        """Copy the file at path in the local directory to the S3 bucket.
        """
        if isinstance(local_file, File):
            fname = local_file.path
            mtime = local_file.timestamp
        else:
            fname = local_file
            mtime = os.stat(fname).st_mtime_ns

        if path is None:
            path = os.path.relpath(fname).replace('\\', '/')

        log.info("copy %s s3://%s/%s ", fname, self.bucket_name, path)

        self.bucket.upload_file(
            fname,
            path,
            ExtraArgs={
                'Metadata': {
                    'Tagging': json.dumps({
                        'mtime': mtime,
                    }, separators=(',', ':'))
                }
            }
        )

    def delete_file(self, path):
        """Delete the file at path in the S3 bucket.
        """
        self.bucket.Object(path).delete()

    def delete_files(self, paths):
        """Delete the files at paths in the S3 bucket.
        """
        self.bucket.delete_objects(
            Delete={'Objects': [{'Key': path} for path in paths]})

    def delete_all_files(self):
        """Delete all files in the S3 bucket.
        """
        self.bucket.objects.delete()

    def copy_files_to(self, paths, local_dir):
        """Copy the files at paths in the S3 bucket to the local directory.
        """
        for path in paths:
            self.bucket.download_file(path, local_dir)

    # def copy_files_from(self, paths, local_dir):
    #     """Copy the files at paths in the local directory to the S3 bucket.
    #     """
    #     for path in paths:
    #         self.bucket.upload_file(local_dir, path)
    #
    # def copy_all_files_to(self, local_dir):
    #     """Copy all files in the S3 bucket to the local directory.
    #     """
    #     for obj in bucket.objects.all():
    #         self.bucket.download_file(obj.key, local_dir)
    #
    # def copy_all_files_from(self, local_dir):
    #     """Copy all files in the local directory to the S3 bucket.
    #     """
    #     for obj in self.bucket.objects.all():
    #         self.bucket.upload_file(local_dir, obj.key)
    #
    # def list_files_in(self, path):
    #     """Return a list of all files in the S3 bucket at path.
    #     """
    #     s3 = self.session.resource('s3')
    #     bucket = s3.Bucket(self.bucket_name)
    #     for obj in bucket.objects.filter(Prefix=path):
    #         yield obj.key
    #
    # def local_timestamp(self, local_file):
    #     """Return the timestamp of the local file.
    #     """
    #     return get_timestamp(local_file)
    #
    # def compare_timestamps(self, local_file, path):
    #     """Return the number of seconds older the local file is than the file
    #        at path in the S3 bucket.
    #     """
    #     s3 = self.session.resource('s3')
    #     bucket = s3.Bucket(self.bucket_name)
    #     local_ts = get_timestamp(local_file)
    #     s3_ts = bucket.Object(path).last_modified
    #     if local_ts > s3_ts:
    #         diff = local_ts - s3_ts
    #     else:
    #         diff = s3_ts - local_ts
    #     secs = int(diff.total_seconds())
    #     direction = 1 if local_ts <= s3_ts else -1
    #     return direction * secs
    #
    # def s3_newer_than_local(self, path, local_file):
    #     """Return True if the file at path in the S3 bucket is newer than the local file.
    #     """
    #     s3 = self.session.resource('s3')
    #     bucket = s3.Bucket(self.bucket_name)
    #
    #     def mtime_to_datetime_with_tz(mtime):
    #         return datetime.datetime.fromtimestamp(mtime, pytz.utc)
    #
    #     # print("Mtime of local file: ", mtime_to_datetime_with_tz(os.path.getmtime(local_file)))
    #     # print("Mtime of S3 file   : ", bucket.Object(path).last_modified)
    #     return bucket.Object(path).last_modified > get_timestamp(local_file)
    #
    # def compare_file_to_s3(self, path, local_file):
    #     """Return True if the file at path in the S3 bucket is different from the local file.
    #     """
    #
    #     s3 = self.session.resource('s3')
    #     bucket = s3.Bucket(self.bucket_name)
    #     print("S3 hash: ", bucket.Object(path).e_tag)
    #     print("Local hash: ", '"{}"'.format(hash_file(local_file)))
    #     return bucket.Object(path).e_tag != '"{}"'.format(hash_file(local_file))
    #
    # def compare_file(self, path, local_file):
    #     """Return True if the file at path in the S3 bucket is the same as the local file.
    #     """
    #
    #     def hash_file(fname):
    #         import hashlib
    #         h = hashlib.sha256()
    #         with open(fname, 'rb', buffering=0) as f:
    #             for b in iter(lambda: f.read(128 * 1024), b''):
    #                 h.update(b)
    #         return h.hexdigest()
    #
    #     s3 = self.session.resource('s3')
    #     bucket = s3.Bucket(self.bucket_name)
    #     return bucket.Object(path).e_tag == '"{}"'.format(hash_file(local_file))
