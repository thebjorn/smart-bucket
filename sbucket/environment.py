import os
try:
    os.environ['DKPASSWORDS'] = r'c:\srv\dkpasswords'
    import dkpw

    def getenv(name):
        return dkpw.get(f'GH_{name}')

except ImportError:

    def getenv(name):
        return os.environ[name]


S3_TEST_BUCKET_NAME = getenv('S3_TEST_BUCKET_NAME')
AWS_ACCESS_KEY_ID = getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = getenv('AWS_SECRET_ACCESS_KEY')
AWS_DEFAULT_REGION = os.environ.get('AWS_DEFAULT_REGION', 'eu-central-1')
