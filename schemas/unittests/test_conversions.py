import os
from pathlib import Path

import pytest

import schemas


@pytest.mark.parametrize(
    ("original"),
    [
        schemas.FileMetadata(
            Path("/some/where"),
            schemas.Signature("signature"),
            42,
            os.stat_result(tuple(range(10))),
        ),
        schemas.Block(schemas.Signature("signature"), "https://url.com", False),
        schemas.FileData(
            schemas.FileMetadata(
                Path("/some/where"),
                schemas.Signature("signature"),
                42,
                os.stat_result(tuple(range(10))),
            ),
            [
                schemas.Block(schemas.Signature("signature"), "https://url.com", False)
                for _ in range(5)
            ],
        ),
        schemas.Registration(
            "rig",
            schemas.FileData(
                schemas.FileMetadata(
                    Path("/some/where"),
                    schemas.Signature("signature"),
                    42,
                    os.stat_result(tuple(range(10))),
                ),
                [
                    schemas.Block(
                        schemas.Signature("signature"), "https://url.com", False
                    )
                    for _ in range(5)
                ],
            ),
            "creds",
        ),
    ],
)
def test_serialization_identity(original):
    serialized = original.to_dict()
    result = type(original).from_dict(serialized)

    assert isinstance(serialized, dict)
    assert original == result
