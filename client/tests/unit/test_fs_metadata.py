import os
from copy import deepcopy
from tempfile import NamedTemporaryFile
from unittest.mock import Mock

import pytest
from pytest_mock import mocker

from client.datasafe import (
    FileMetadata,
    FsMetadataCache,
    Path,
    Signature,
    read_file_metadata,
)


@pytest.mark.parametrize(
    ("MockFileMetadata", "file_read"),
    [
        (lambda p, h, l, s: FileMetadata(p, h, l, s), False),  # Valid cache
        (lambda p, h, l, s: None, True),  # Cache miss
        (lambda p, h, l, s: FileMetadata(p, h, l, s + (1, 2)), True),  # Invalid cache
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
            tmp_fd.name,
            Signature(
                "3fc9b689459d738f8c88a3a48aa9e33542016b7a4052e001aaa536fca74813cb"
            ),
            9,
            os.stat(tmp_fd.name),
        )

        result = read_file_metadata(tmp_fd.name, cached)
        assert result == FileMetadata(
            tmp_fd.name,
            Signature(
                "3fc9b689459d738f8c88a3a48aa9e33542016b7a4052e001aaa536fca74813cb"
            ),
            9,
            os.stat(tmp_fd.name),
        )
        assert mock_read.called == file_read
