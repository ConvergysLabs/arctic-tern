from arctic_tern.filename import parse_file_name, construct_migration
import pytest
import os


@pytest.mark.parametrize("test_filename,expected", [
    ("1519669955912-first.sql", [1519669955912, 'first']),
    ("1519669956912Other.sql", [1519669956912, 'Other']),
    ("1Other_thing_to_do.sql", [1, 'Other_thing_to_do']),
    ("2018022601 - A-long-sentence.sql", [2018022601, 'A-long-sentence']),
    ("2018022601 - A longer sentence with spaces.sql", [2018022601, 'A longer sentence with spaces']),
    ("2018022601.sql", [2018022601, None]),
])
def test_hyphen(test_filename, expected):
    stamp, label = parse_file_name(test_filename)
    assert stamp == expected[0]
    assert label == expected[1]


@pytest.mark.parametrize("test_filename", [
    "Nope.sql",
    "Comment first - 20180226.sql",
])
def test_fails(test_filename):
    val = parse_file_name(test_filename)
    assert val is None


@pytest.mark.parametrize("file_, dir_, stamp, label, win_hash, posix_hash", [
    ("1-person-address.sql", "tests/scripts", 1, "person-address", "be51b1ac168191cb6d3741c717ed71492f5a913b144170f1ca9fed82", "920366b2c5321fc908b06c86047e30e9b63f39b2b240211645991782"),
    ("2-add-apartment.sql", "tests/other", 2, "add-apartment", "714ac7e2ae171eddcb0687fd5fc81af95d83b9e49c8c48edc3d51be0", "714ac7e2ae171eddcb0687fd5fc81af95d83b9e49c8c48edc3d51be0"),
    ("3-fail.sql", "tests/scripts", 3, "fail", "1ad019a8d4e278d5265b05ca19d9a6d17259f973a0ad053d3b4f2a19", "7b21676def0b49f10b949c194c3cb1cc0c5fb35077d6d3018fe1114b"),
])
def test_mf(file_, dir_, stamp, label, win_hash, posix_hash):
    mf = construct_migration(file_, dir_)
    assert mf.stamp == stamp
    assert mf.name == label
    assert os.path.split(mf.path) == (dir_, file_)
    assert mf.hash_ == win_hash or mf.hash_ == posix_hash
