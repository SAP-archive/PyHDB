# -*- coding: utf-8 -*-

import pytest
from pyhdb.protocol import types
from pyhdb.exceptions import InterfaceError

def test_escape_unsupported_type():
    with pytest.raises(InterfaceError):
        types.escape(lambda *args: args)

def test_escape_simple_string():
    text = "Hello World"
    assert types.escape(text) == "'Hello World'"

def test_escape_simple_unicode():
    text = u"Hello World"
    assert types.escape(text) == u"'Hello World'"

def test_escape_string_with_apostrophe():
    text = "'Hello' \"World\""
    assert types.escape(text) == "'''Hello'' \"World\"'"

def test_escape_unicode_with_apostrophe():
    text = u"'Hüllö' \"Wörldß\""
    assert types.escape(text) == u"'''Hüllö'' \"Wörldß\"'"

def test_escape_list():
    assert types.escape(("a", "b")) == "('a', 'b')"

def test_escape_list_with_apostrophe():
    assert types.escape(("a'", "'b")) == "('a''', '''b')"

def test_escape_values_from_list():
    arguments = ["'Hello'", "World"]
    assert types.escape_values(arguments) == ("'''Hello'''", '\'World\'')

def test_escape_values_from_tuple():
    arguments = ("'Hello'", "World")
    assert types.escape_values(arguments) == ("'''Hello'''", '\'World\'')

def test_escape_values_from_dict():
    arguments = {
        "verb": "'Hello'",
        "to": "World"
    }
    assert types.escape_values(arguments) == {
        'verb': "'''Hello'''",
        'to': "'World'"
    }

def test_escape_values_raises_exception_with_wrong_type():
    with pytest.raises(InterfaceError):
        types.escape_values(None)

def test_escape_None():
    types.escape(None) == "NULL"
