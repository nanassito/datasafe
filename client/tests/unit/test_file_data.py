import pytest

from client.datasafe import FileData


@pytest.mark.parametrize(
    ("size", "block_size", "expected"),
    [
        (0, 4096, 0),  # Empty file
        (1024, 4096, 1),  # Some arbitrary value
        (4096, 4096, 1),  # Perfect block
        (4097, 4096, 2),  # Just over the limit
    ],
)
def test_num_block(mocker, size, block_size, expected):
    mocker.patch("client.datasafe.FileData._assert_block_qty")
    file = FileData("path", "signature", block_size, size, [])
    assert file.num_blocks == expected


@pytest.mark.parametrize(
    ("num_blocks", "expect_failure"),
    [
        (0, True),  # Missing a block
        (1, False),  # Correct number
        (2, True),  # Too many blocks
    ],
)
def test_enforce_num_block(num_blocks, expect_failure):
    params = ["path", "signature", 4096, 1, [object() for _ in range(num_blocks)]]
    if expect_failure:
        with pytest.raises(AssertionError):
            FileData(*params)
    else:
        FileData(*params)
