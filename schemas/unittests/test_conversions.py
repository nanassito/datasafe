import json
import os
from pathlib import Path

import pytest

from schemas import FileMetadata, Signature


@pytest.mark.parametrize(
    ("spec", "expected"),
    [
        (  # Using the types
            (
                Path("/some/where"),
                Signature("signature"),
                42,
                os.stat_result(tuple(range(10))),
            ),
            FileMetadata(
                Path("/some/where"),
                Signature("signature"),
                42,
                os.stat_result(tuple(range(10))),
            ),
        ),
        (  # Using simple types used in serialization
            ("/some/where", "signature", 42, tuple(range(10))),
            FileMetadata(
                Path("/some/where"),
                Signature("signature"),
                42,
                os.stat_result(tuple(range(10))),
            ),
        ),
    ],
)
def test_file_metadata(spec, expected):
    original = FileMetadata(*spec)
    serialized = original.to_dict()
    result = FileMetadata(**serialized)

    assert expected == original == result
