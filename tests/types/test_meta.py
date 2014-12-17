# Copyright 2014 SAP SE.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http: //www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
# either express or implied. See the License for the specific
# language governing permissions and limitations under the License.

import pytest
from pyhdb.protocol import types
from pyhdb.exceptions import InterfaceError

def test_automated_mapping_by_type_code():
    class DummyType(types.Type):
        code = 127

    assert types.by_type_code[127] == DummyType
    assert DummyType not in types.by_python_type.values()

def test_automated_mapping_by_multiple_type_code():
    class DummyType(types.Type):
        code = (126, 127)

    assert types.by_type_code[126] == DummyType
    assert types.by_type_code[127] == DummyType
    assert DummyType not in types.by_python_type.values()

def test_invalid_automated_mapping_by_type_code():
    with pytest.raises(InterfaceError):
        class DummyType(types.Type):
            code = 999

def test_automated_mapping_by_python_type():
    class DummyType(types.Type):
        python_type = None

    assert types.by_python_type[None] == DummyType
    assert DummyType not in types.by_type_code.values()

def test_automated_mapping_by_multiple_python_type():
    class DummyType(types.Type):
        python_type = (int, None)

    assert types.by_python_type[int] == DummyType
    assert types.by_python_type[None] == DummyType
    assert DummyType not in types.by_type_code.values()

def test_type_mapping_is_a_weakref():
    class DummyType(types.Type):
        code = 125
        python_type = int

    assert types.by_type_code[125] == DummyType
    assert types.by_python_type[int] == DummyType

    del DummyType
    import gc
    gc.collect()

    assert 125 not in types.by_type_code
    assert int not in types.by_python_type

def test_all_types_with_code_has_method_from_resultset():
    for typ in types.by_type_code.values():
        assert hasattr(typ, "from_resultset")
        assert callable(typ.from_resultset)

def test_all_types_with_python_type_has_method_to_sql():
    for typ in types.by_python_type.values():
        assert hasattr(typ, "to_sql")
        assert callable(typ.to_sql)
