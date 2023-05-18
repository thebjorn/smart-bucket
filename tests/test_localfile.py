from sbucket.s3file import LocalFile


def test_localfile():
    f = LocalFile('tests/test_localfile.py')
    assert f.exists
    assert f.hash
    assert f.timestamp

