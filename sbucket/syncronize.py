
import logging
log = logging.getLogger(__name__)


def sync_file(local_file, bucket_file):  # sourcery skip: remove-redundant-if
    """Synchronize the file at path in the S3 bucket.
    """
    log.info("sync_file: %s %s", local_file, bucket_file)
    if bucket_file.exists and local_file.missing:
        # if we don't have the file, download it
        log.info("downloading %s", bucket_file)
        bucket_file.download()
    elif bucket_file.missing and local_file.exists:
        # if we have the file but S3 doesn't, upload it
        log.info("uploading %s", bucket_file)
        bucket_file.upload()
    elif bucket_file.exists and local_file.exists:
        # if we have the file and S3 does too, check the timestamps
        if bucket_file.timestamp > local_file.timestamp:
            # if S3 is newer, download it
            log.info("downloading %s", bucket_file)
            bucket_file.download()
        elif bucket_file.timestamp < local_file.timestamp:
            # if local is newer, upload it
            log.info("uploading %s", bucket_file)
            bucket_file.upload()
    log.info("sync_file: %s %s done", local_file, bucket_file)


def sync_file_tree(local_file_tree, bucket):
    """Synchronize local_file_tree with bucket.
    """
    for local_file_path in local_file_tree:
        local_file = local_file_tree.local_file(local_file_path)
        bucket_file = bucket.file(local_file_path)
        sync_file(local_file, bucket_file)

    for s3file in bucket.list_files():
        local_file = local_file_tree.local_file(s3file)
        bucket_file = bucket.file(s3file)
        sync_file(local_file, bucket_file)
