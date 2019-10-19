import os
from pathlib import Path
from tempfile import NamedTemporaryFile, TemporaryDirectory
from unittest.mock import Mock

import pytest
from pytest_mock import mocker  # noqa: F401

import datasafe_client


@pytest.mark.parametrize(
    ("MockFileMetadata", "file_read"),
    [
        (
            lambda p, h, l, s: datasafe_client.FileMetadata(p, h, l, s),
            False,
        ),  # Valid cache
        (lambda p, h, l, s: None, True),  # Cache miss
        (
            lambda p, h, l, s: datasafe_client.FileMetadata(p, h, l, s + (1, 2)),
            True,
        ),  # Invalid cache
    ],
)
def test_read_file_metadata(mocker, MockFileMetadata, file_read):  # noqa: F811
    with NamedTemporaryFile() as tmp_fd:
        tmp_fd.write(b"something")
        tmp_fd.flush()
        tmp_fd.seek(0)

        mock_open = mocker.patch("datasafe_client.open")
        mock_open.return_value.__enter__.return_value.read = mock_read = Mock(
            side_effect=tmp_fd.read
        )
        cached = MockFileMetadata(
            Path(tmp_fd.name),
            datasafe_client.Signature(
                "3fc9b689459d738f8c88a3a48aa9e33542016b7a4052e001aaa536fca74813cb"
            ),
            9,
            os.stat(tmp_fd.name),
        )

        result = datasafe_client.read_file_metadata(Path(tmp_fd.name), cached)
        assert result == datasafe_client.FileMetadata(
            Path(tmp_fd.name),
            datasafe_client.Signature(
                "3fc9b689459d738f8c88a3a48aa9e33542016b7a4052e001aaa536fca74813cb"
            ),
            9,
            os.stat(tmp_fd.name),
        )
        assert mock_read.called == file_read


def test_traverse_fs():
    fs_tree = ["file1", "dir1/file1", "dir1/file2", "dir2/file3"]
    with TemporaryDirectory() as tmp_dir:
        fs_tree = [Path(tmp_dir) / Path(n) for n in fs_tree]
        for node in fs_tree:
            node.parent.mkdir(parents=True, exist_ok=True)
            node.touch()

        src = datasafe_client.Source(Path(tmp_dir))
        assert sorted(fs_tree) == sorted(datasafe_client.traverse_fs_from_source(src))
