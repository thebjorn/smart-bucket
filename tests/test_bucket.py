from sbucket import environment
from sbucket.s3bucket import S3Bucket


def test_list_files():
    """Test listing files in the S3 bucket.
    """
    b = S3Bucket(environment.S3_TEST_BUCKET_NAME)
    assert list(b.list_files()) == ['syncs3.py']

