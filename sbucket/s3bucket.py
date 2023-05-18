import datetime
import json
import pytz
import os
import boto3
import logging
# from urllib import parse
# from devtools import debug
from .s3file import S3File, LocalFile, get_timestamp, hash_file

log = logging.getLogger(__name__)


class S3Bucket:
    def __init__(self, bucket_name):
        self.bucket_name = bucket_name
        self.s3 = boto3.resource('s3')

    def __str__(self):
        return self.bucket_name

    def file(self, path):
        return S3File(self, path)

    def list_files(self):
        """Return a list of all files in the S3 bucket.
        """
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(self.bucket_name)
        for obj in bucket.objects.all():
            yield obj.key

    def timestamp(self, path):
        """Return the timestamp of the file at path in the S3 bucket.
        """
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(self.bucket_name)
        return bucket.Object(path).last_modified

    def contents(self, path):
        """Return the contents of the file at path in the S3 bucket.
        """
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(self.bucket_name)
        return bucket.Object(path).get()['Body'].read().decode('utf-8')

    def exists(self, path):
        """Return True if the file at path exists in the S3 bucket.
        """
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(self.bucket_name)
        return bucket.Object(path)  # .last_modified

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
        head = self.s3.meta.client.head_object(Bucket=self.bucket_name,
                                               Key=path)
        return head

    def get_metadata(self, path):
        """Return the metadata of the file at path in the S3 bucket.
        """
        head = self.read_header(path)
        return head['Metadata']

    def get_hash(self, path):
        """Return the hash of the file at path in the S3 bucket.
        """
        return self.read_header(path)['ETag']

    def get_tags(self, path):
        """Return the tags of the file at path in the S3 bucket.

           Ref. tags set in :meth:`copy_to_s3`.
        """
        return json.loads(self.get_metadata(path)['tagging'])

    def copy_to_s3(self, local_file, path=None):
        """Copy the file at path in the local directory to the S3 bucket.
        """
        if path is None:
            path = os.path.relpath(local_file).replace('\\', '/')

        log.info("copy %s s3://%s/%s ", local_file, self.bucket_name, path)
        s = os.stat(local_file)

        self.s3.meta.client.upload_file(
            local_file,
            self.bucket_name,
            path,
            ExtraArgs={
                'Metadata': {
                    'Tagging': json.dumps({
                        'mtime': s.st_mtime_ns,
                        'atime': s.st_atime_ns,
                    }, separators=(',', ':'))
                }
            }
        )

    def delete_file(self, path):
        """Delete the file at path in the S3 bucket.
        """
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(self.bucket_name)
        bucket.Object(path).delete()

    def delete_files(self, paths):
        """Delete the files at paths in the S3 bucket.
        """
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(self.bucket_name)
        bucket.delete_objects(
            Delete={'Objects': [{'Key': path} for path in paths]})

    def delete_all_files(self):
        """Delete all files in the S3 bucket.
        """
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(self.bucket_name)
        bucket.objects.all().delete()

    def copy_files_to(self, paths, local_dir):
        """Copy the files at paths in the S3 bucket to the local directory.
        """
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(self.bucket_name)
        for path in paths:
            bucket.download_file(path, local_dir)

    def copy_files_from(self, paths, local_dir):
        """Copy the files at paths in the local directory to the S3 bucket.
        """
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(self.bucket_name)
        for path in paths:
            bucket.upload_file(local_dir, path)

    def copy_all_files_to(self, local_dir):
        """Copy all files in the S3 bucket to the local directory.
        """
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(self.bucket_name)
        for obj in bucket.objects.all():
            bucket.download_file(obj.key, local_dir)

    def copy_all_files_from(self, local_dir):
        """Copy all files in the local directory to the S3 bucket.
        """
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(self.bucket_name)
        for obj in bucket.objects.all():
            bucket.upload_file(local_dir, obj.key)

    def list_files_in(self, path):
        """Return a list of all files in the S3 bucket at path.
        """
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(self.bucket_name)
        for obj in bucket.objects.filter(Prefix=path):
            yield obj.key

    def local_timestamp(self, local_file):
        """Return the timestamp of the local file.
        """
        return get_timestamp(local_file)

    def compare_timestamps(self, local_file, path):
        """Return the number of seconds older the local file is than the file
           at path in the S3 bucket.
        """
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(self.bucket_name)
        local_ts = get_timestamp(local_file)
        s3_ts = bucket.Object(path).last_modified
        if local_ts > s3_ts:
            diff = local_ts - s3_ts
        else:
            diff = s3_ts - local_ts
        secs = int(diff.total_seconds())
        direction = 1 if local_ts <= s3_ts else -1
        return direction * secs

    def s3_newer_than_local(self, path, local_file):
        """Return True if the file at path in the S3 bucket is newer than the local file.
        """
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(self.bucket_name)

        def mtime_to_datetime_with_tz(mtime):
            return datetime.datetime.fromtimestamp(mtime, pytz.utc)

        # print("Mtime of local file: ", mtime_to_datetime_with_tz(os.path.getmtime(local_file)))
        # print("Mtime of S3 file   : ", bucket.Object(path).last_modified)
        return bucket.Object(path).last_modified > get_timestamp(local_file)

    def compare_file_to_s3(self, path, local_file):
        """Return True if the file at path in the S3 bucket is different from the local file.
        """

        s3 = boto3.resource('s3')
        bucket = s3.Bucket(self.bucket_name)
        print("S3 hash: ", bucket.Object(path).e_tag)
        print("Local hash: ", '"{}"'.format(hash_file(local_file)))
        return bucket.Object(path).e_tag != '"{}"'.format(hash_file(local_file))

    def compare_file(self, path, local_file):
        """Return True if the file at path in the S3 bucket is the same as the local file.
        """

        def hash_file(fname):
            import hashlib
            h = hashlib.sha256()
            with open(fname, 'rb', buffering=0) as f:
                for b in iter(lambda: f.read(128 * 1024), b''):
                    h.update(b)
            return h.hexdigest()

        s3 = boto3.resource('s3')
        bucket = s3.Bucket(self.bucket_name)
        return bucket.Object(path).e_tag == '"{}"'.format(hash_file(local_file))
