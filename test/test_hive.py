from honeycomb.hive import _hive_check_valid_table_path


def test_that_blank_path_disallowed():
    assert not _hive_check_valid_table_path('')


def test_that_whitespace_disallowed():
    assert not _hive_check_valid_table_path(' ')
    assert not _hive_check_valid_table_path('\n')
    assert not _hive_check_valid_table_path('\t')
    assert not _hive_check_valid_table_path('\r')
    assert not _hive_check_valid_table_path('     ')
    assert not _hive_check_valid_table_path(' path')
    assert not _hive_check_valid_table_path('path ')
    assert not _hive_check_valid_table_path(' path ')
    assert not _hive_check_valid_table_path('pa th')


def test_backslash_disallowed():
    assert not _hive_check_valid_table_path('\\')
    assert not _hive_check_valid_table_path('\\ ')
    assert not _hive_check_valid_table_path(' \\')


def test_that_slashes_disallowed_before_words():
    assert not _hive_check_valid_table_path('/')
    assert not _hive_check_valid_table_path('/ ')
    assert not _hive_check_valid_table_path(' /')
    assert not _hive_check_valid_table_path('/path')


def test_that_dashes_disallowed_before_words():
    assert not _hive_check_valid_table_path('-')
    assert not _hive_check_valid_table_path('- ')
    assert not _hive_check_valid_table_path(' -')
    assert not _hive_check_valid_table_path('-path')


def test_misc_disallowed_characters():
    misc_chars = [
        '~', '`', '!', '@', '#', '$', '%', '^', '&', '*', '(', ')', '+',
        ',', '.', '<', '>', '?', ';', ':', '\'', '"', '{', '}', '|'
    ]
    for char in misc_chars:
        assert not _hive_check_valid_table_path(char)


def test_word_characters_allowed():
    assert _hive_check_valid_table_path('path')
    assert _hive_check_valid_table_path('pAtH')
    assert _hive_check_valid_table_path('1234')
    assert _hive_check_valid_table_path('its_a_path')
    assert _hive_check_valid_table_path('pAtH_0123')


def test_slashes_allowed_after_words():
    assert _hive_check_valid_table_path('path/')
    assert _hive_check_valid_table_path('path/folder')
    assert _hive_check_valid_table_path('path1234/folder/')


def test_dashes_allowed_after_words():
    assert _hive_check_valid_table_path('its-a-path')
