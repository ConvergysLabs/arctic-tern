from arctic_tern.filename import parse_file_name
import pytest


@pytest.mark.parametrize("test_filename,expected", [
    ("1519669955912-first.sql", [1519669955912, 'first']),
    ("1519669956912Other.sql", [1519669956912, 'Other']),
    ("1Other_thing_to_do.sql", [1, 'Other_thing_to_do']),
    ("2018022601 - A-long-sentence.sql", [2018022601, 'A-long-sentence']),
    ("2018022601 - A longer sentence with spaces.sql", [2018022601, 'A longer sentence with spaces']),
    ("2018022601.sql", [2018022601, None]),
])
def test_hyphen(test_filename, expected):
    val = parse_file_name(test_filename)
    assert val[0] == expected[0]
    assert val[1] == expected[1]


@pytest.mark.parametrize("test_filename", [
    "Nope.sql",
    "Comment first - 20180226.sql",
])
def test_fails(test_filename):
    val = parse_file_name(test_filename)
    assert val is None
