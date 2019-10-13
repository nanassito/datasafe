import os
import time
from copy import deepcopy
from pathlib import Path
from tempfile import NamedTemporaryFile, TemporaryDirectory
from unittest.mock import Mock

import pytest
from pytest_mock import mocker

from client import datasafe


@pytest.mark.parametrize(
    ("MockFileMetadata", "file_read"),
    [
        (lambda p, h, l, s: datasafe.FileMetadata(p, h, l, s), False),  # Valid cache
        (lambda p, h, l, s: None, True),  # Cache miss
        (
            lambda p, h, l, s: datasafe.FileMetadata(p, h, l, s + (1, 2)),
            True,
        ),  # Invalid cache
    ],
)
def test_read_file_metadata(mocker, MockFileMetadata, file_read):
    with NamedTemporaryFile() as tmp_fd:
        tmp_fd.write(b"something")
        tmp_fd.flush()
        tmp_fd.seek(0)

        mock_open = mocker.patch("client.datasafe.open")
        mock_open.return_value.__enter__.return_value.read = mock_read = Mock(
            side_effect=tmp_fd.read
        )
        cached = MockFileMetadata(
            Path(tmp_fd.name),
            datasafe.Signature(
                "3fc9b689459d738f8c88a3a48aa9e33542016b7a4052e001aaa536fca74813cb"
            ),
            9,
            os.stat(tmp_fd.name),
        )

        result = datasafe.read_file_metadata(Path(tmp_fd.name), cached)
        assert result == datasafe.FileMetadata(
            Path(tmp_fd.name),
            datasafe.Signature(
                "3fc9b689459d738f8c88a3a48aa9e33542016b7a4052e001aaa536fca74813cb"
            ),
            9,
            os.stat(tmp_fd.name),
        )
        assert mock_read.called == file_read


@pytest.mark.parametrize(
    ("sources", "filesystem"),
    [
        (  # Finds all files
            ["./"],
            {
                "./subdir/file1": {
                    "before": lambda fm: fm,
                    "fs": object(),
                    "after": lambda fm: fm,
                },
                "./subdir/file2": {
                    "before": lambda fm: fm,
                    "fs": object(),
                    "after": lambda fm: fm,
                },
                "./file1": {
                    "before": lambda fm: fm,
                    "fs": object(),
                    "after": lambda fm: fm,
                },
            },
        ),
        (  # Overlapping sources
            ["./", "./subdir"],
            {
                "./subdir/file1": {
                    "before": lambda fm: fm,
                    "fs": object(),
                    "after": lambda fm: fm,
                },
                "./subdir/file2": {
                    "before": lambda fm: fm,
                    "fs": object(),
                    "after": lambda fm: fm,
                },
                "./file1": {
                    "before": lambda fm: fm,
                    "fs": object(),
                    "after": lambda fm: fm,
                },
            },
        ),
        (  # Cache hit and miss
            ["./"],
            {
                "./hit": {
                    "before": lambda fm: fm,
                    "fs": object(),
                    "after": lambda fm: fm,
                },
                "./miss": {
                    "before": lambda fm: None,
                    "fs": object(),
                    "after": lambda fm: fm,
                },
            },
        ),
        (  # Preserve too much cache
            ["./"],
            {
                "./disappeared": {
                    "before": lambda fm: 42,
                    "fs": None,
                    "after": lambda fm: 42,
                },
                "./file": {
                    "before": lambda fm: fm,
                    "fs": object(),
                    "after": lambda fm: fm,
                },
            },
        ),
    ],
)
def test_read_all_filesystem_metadata(mocker, sources, filesystem):
    with TemporaryDirectory() as tempdir:
        sources = [
            datasafe.Source(Path(os.path.join(tempdir, p)).absolute()) for p in sources
        ]
        filesystem = {
            Path(os.path.join(tempdir, p)).absolute(): s for p, s in filesystem.items()
        }
        expected_cache = {
            file: spec["after"](spec["fs"]) for file, spec in filesystem.items()
        }
        cache = {
            file: spec["before"](spec["fs"])
            for file, spec in filesystem.items()
            if spec["before"](spec["fs"])
        }
        mocker.patch(
            "client.datasafe.read_file_metadata",
            lambda p, c: filesystem.get(p, {}).get("fs", None),
        )
        fs_metadata_cache_mock = mocker.patch("client.datasafe.FsMetadataCache")
        fs_metadata_cache_mock.return_value.__enter__.return_value = cache

        for filepath, spec in filesystem.items():
            os.makedirs(os.path.split(filepath)[0], exist_ok=True)
            if spec["fs"] is not None:
                filepath.touch(exist_ok=True)

        results = set(datasafe.read_all_filesystem_metadata(sources))
        expected = {spec["fs"] for spec in filesystem.values() if spec["fs"]}

    assert results == expected
    assert cache == expected_cache


def test_read_all_filesystem_metadata_concurrency(mocker):
    time_per_file = 0.1
    num_files = 10
    mocker.patch(
        "client.datasafe.read_file_metadata",
        side_effect=lambda p, c: time.sleep(time_per_file),
    )
    fs_metadata_cache_mock = mocker.patch("client.datasafe.FsMetadataCache")
    fs_metadata_cache_mock.return_value.__enter__.return_value = {}

    with TemporaryDirectory() as tempdir:
        for i in range(num_files):
            Path(os.path.join(tempdir, str(i))).touch()
        before = time.monotonic()
        datasafe.read_all_filesystem_metadata([datasafe.Source(Path(tempdir))])
        after = time.monotonic()

    # This is not a great test because it doesn't account for the overhead of
    # the threading. So it might fail on less powerful machines.
    assert after - before < num_files * time_per_file / 2
