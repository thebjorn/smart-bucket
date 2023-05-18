"""
Test that all modules are importable.
"""

import sbucket
import sbucket.main
import sbucket.s3bucket
import sbucket.s3file


def test_import_sbucket():
    """Test that all modules are importable.
    """
    
    assert sbucket
    assert sbucket
    assert sbucket.main
    assert sbucket.s3bucket
    assert sbucket.s3file
