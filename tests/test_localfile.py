import os
from sbucket.localfile import LocalFile

CURDIR = os.path.dirname(__file__)


def test_localfile():
    f = LocalFile(os.path.join(CURDIR, 'test_localfile.py'))
    assert f.exists
    assert f.hash
    assert f.timestamp

