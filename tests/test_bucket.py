import os

from sbucket import environment
from sbucket.s3bucket import S3Bucket


def test_list_files():
    """Test listing files in the S3 bucket.
    """
    os.environ['DKPASSWORDS'] = r'c:\srv\dkpasswords'
    print("S3 BUCKET:", environment.S3_TEST_BUCKET_NAME)
    b = S3Bucket(environment.S3_TEST_BUCKET_NAME)
    assert 'syncs3.py' in list(b.list_files())
    # b.copy_to_s3('test_bucket.py')
    syncs3 = b.file('syncs3.py')
    assert syncs3.exists
