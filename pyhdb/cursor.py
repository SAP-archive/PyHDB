# Copyright 2014 SAP SE
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from collections import deque, namedtuple
import binascii
import re

from pyhdb.protocol.base import RequestSegment
from pyhdb.protocol.types import escape_values, by_type_code
from pyhdb.protocol.parts import Command, FetchSize, ResultSetId, StatementId, Parameters
from pyhdb.protocol.constants import message_types, function_codes, part_kinds
from pyhdb.exceptions import ProgrammingError, InterfaceError, DatabaseError
from pyhdb._compat import iter_range


FORMAT_OPERATION_ERRORS = [
    'not enough arguments for format string',
    'not all arguments converted during string formatting'
]


def format_operation(operation, parameters=None):
    if parameters is not None:
        e_values = escape_values(parameters)
        try:
            operation = operation % e_values
        except TypeError as msg:
            if str(msg) in FORMAT_OPERATION_ERRORS:
                # Python DBAPI expects a ProgrammingError in this case
                raise ProgrammingError(str(msg))
            else:
                # some other error message appeared, so just reraise exception:
                raise
    return operation


def format_named_operation(operation, parameters=None):
    # 1) split operation at quotes to leave string literals as they are
    # 2) iterate over non-literals and use a regex to replace named parameters (e.g. ':id')
    # 3) join string literals and non-literals back together

    if parameters is None:
        return operation

    escaped_parameters = escape_values(parameters)

    def matching_parameter(match):
        parameter_name = match.group(1)
        try:
            return escaped_parameters[parameter_name]
        except KeyError:  # no such key found in given parameters
            raise ProgrammingError("Named parameter not specified: %(parameter_name)s ")

    splitted = operation.split("'")
    for i in range(0, len(splitted), 2):  # iterate only over non-string-literals
        splitted[i] = re.sub(_NAMED_PARAM, matching_parameter, splitted[i])
    return "'".join(splitted)

_NAMED_PARAM = re.compile(r":([a-zA-Z]+)")


class PreparedStatement(object):

    def __init__(self, connection, statement_id, parameters, resultmetadata):
        self._connection = connection
        self._statement_id = statement_id
        self._parameters = parameters
        self._resultmetadata = resultmetadata
        if type(parameters[0][3]) == str:
            # named parameters
            self._param_values = {}
            for param in parameters:
                self._param_values[param[3]] = None
        else:
            # parameters by sequence
            # sequence starts from 1, 2 ...; 0 not used
            self._param_values = [None] * (1+len(parameters))

    @property
    def statement_id(self):
        """ statement id as byte array"""
        return self._statement_id

    @property
    def statement_xid(self):
        """ statement id as hex string"""
        return binascii.hexlify(bytearray(self._statement_id))

    @property
    def result_metadata(self):
        return self._resultmetadata

    def set_parameter_value(self, param_id, value):
        """ set parameter value"""
        self._param_values[param_id] = value

    @property
    def parameter_value(self, param_id):
        """ get parameter value"""
        return self._param_values[param_id]

    @property
    def parameters(self):
        """ get all parameters' values"""
        Parameter = namedtuple('Parameter', 'id datatype length value')
        _result = []
        for param in self._parameters:
            if type(param[3]) == int:
                value_id = param[3]+1
            else:
                value_id = param[3]
            _result.append(Parameter(param[3], param[1], param[4], self._param_values[value_id]))
        return _result

    def set_parameters(self, param_values):
        if type(param_values) is list:
            if (len(param_values) + 1) != len(self._param_values):
                raise ProgrammingError(
                    "Prepared statement parameters expected %d supplied %d." % (
                        len(self._param_values) - 1, len(param_values))
                )

            for param_id, value in enumerate(param_values):
                self._param_values[param_id + 1] = value
        # elif type(param_values) is dict:
        #    for param_id in param_values:
        #        self._param_values[param_id] = param_values[param_id]
        else:
            raise ProgrammingError(
                "Prepared statement parameters supplied as %s, shall be list."
                % str(type(param_values)))


class Cursor(object):

    def __init__(self, connection):
        self._connection = connection
        self._buffer = deque()
        self._received_last_resultset_part = False
        self._executed = None

        self.rowcount = -1
        self.description = None
        self.rownumber = None
        self.arraysize = 1
        self._prepared_statements = {}

    @property
    def prepared_statement_ids(self):
        return self._prepared_statements.keys()

    def prepared_statement(self, statement_id):
        return self._prepared_statements[statement_id]

    def prepare(self, statement):
        self._check_closed()

        response = self._connection.Message(
            RequestSegment(
                message_types.PREPARE,
                Command(statement)
            )
        ).send()

        _result = {}
        _result['result_metadata'] = None  # not sent for INSERT
        for _part in response.segments[0].parts:
            if _part.kind == part_kinds.STATEMENTID:
                statement_id = _part.statement_id
            elif _part.kind == part_kinds.STATEMENTCONTEXT:
                _result['stmt_context'] = _part
            elif _part.kind == part_kinds.PARAMETERMETADATA:
                _result['params_metadata'] = _part.values
            elif _part.kind == part_kinds.RESULTSETMETADATA:
                _result['result_metadata'] = _part

        # Handle case where parts-list was empty -> statement_id not set then!

        self._prepared_statements[statement_id] = PreparedStatement(
            self._connection, statement_id, _result['params_metadata'],
            _result['result_metadata'])

        return statement_id

    def execute_prepared(self, prepared_statement, parameters=None):
        self._check_closed()

        if parameters:
            prepared_statement.set_parameters(parameters)

        # Request resultset
        response = self._connection.Message(
            RequestSegment(
                message_types.EXECUTE,
                (StatementId(prepared_statement.statement_id),
                 Parameters(prepared_statement.parameters))
            )
        ).send()

        parts = response.segments[0].parts
        function_code = response.segments[0].function_code
        if function_code == function_codes.SELECT:
            self._handle_prepared_select(parts, prepared_statement.result_metadata)
        elif function_code in function_codes.DML:
            self._handle_prepared_insert(parts)
        elif function_code == function_codes.DDL:
            # No additional handling is required
            pass
        else:
            raise InterfaceError(
                "Invalid or unsupported function code received"
            )

    def execute(self, statement, parameters=None):
        """Execute statement on database

        In order to be compatible with Python's DBAPI five parameter styles
        must be supported.

        paramstyle     Meaning
        ---------------------------------------------------------
        1) qmark       Question mark style, e.g. ...WHERE name=?
        2) numeric     Numeric, positional style, e.g. ...WHERE name=:1
        3) named       Named style, e.g. ...WHERE name=:name
        4) format      ANSI C printf format codes, e.g. ...WHERE name=%s
        5) pyformat    Python extended format codes, e.g. ...WHERE name=%(name)s

        Hana's 'prepare statement' feature supports 1) and 2), while 4 and 5
        are handle by Python's own string expansion mechanism.
        Note that case 3 is not yet supported by this method!
        """
        self._check_closed()

        if not parameters:
            # Directly execute the statement, nothing else to prepare:
            self._execute(statement)
        else:
            # Parameters are given.
            # First try safer hana-style parameter expansion:
            try:
                statement_id = self.prepare(statement)
            except DatabaseError as msg:
                # Hana expansion failed, check message to be sure of reason:
                if 'incorrect syntax near "%"' in str(msg):
                    # Statement contained percentage char, so try Python style
                    # parameter expansion:
                    operation = format_operation(statement, parameters)
                elif 'cannot use parameter variable' in str(msg):
                    # Statement contained ':', so try named parameter replacement:
                    operation = format_named_operation(statement, parameters)
                else:
                    # Probably some other error than related to string expansion
                    raise
                self._execute(operation)
            else:
                # Continue with Hana style statement execution
                prepared_statement = self.prepared_statement(statement_id)
                self.execute_prepared(prepared_statement, parameters)

        # Return cursor object:
        return self

    def _execute(self, operation):
        """Execute statements which are not going through 'prepare_statement
        Because: Either their have no parameters, or Python's string expansion
                 has been applied to the SQL statement.
        """
        response = self._connection.Message(
            RequestSegment(
                message_types.EXECUTEDIRECT,
                Command(operation)
            )
        ).send()

        parts = response.segments[0].parts
        function_code = response.segments[0].function_code
        if function_code == function_codes.SELECT:
            self._handle_select(parts)
        elif function_code in function_codes.DML:
            self._handle_insert(parts)
        elif function_code == function_codes.DDL:
            # No additional handling is required
            pass
        else:
            raise InterfaceError(
                "Invalid or unsupported function code received"
            )

    def executemany(self, statement, parameters):
        # TODO: Prepare statement and use binary parameters transmission
        for _parameters in parameters:
            self.execute(statement, _parameters)

    def _handle_result_metadata(self, result_metadata):
        description = []
        column_types = []
        for column in result_metadata.columns:
            description.append(
                (column[8], column[1], None, column[3], column[2], None,
                 column[0] & 0b10)
            )

            if column[1] not in by_type_code:
                raise InterfaceError(
                    "Unknown column data type: %s" % column[1]
                )
            column_types.append(by_type_code[column[1]])

        return tuple(description), tuple(column_types)

    def _handle_prepared_select(self, parts, result_metadata):

        self.rowcount = -1

        # result metadata
        self.description, self._column_types = self._handle_result_metadata(result_metadata)

        for part in parts:
            if part.kind == part_kinds.RESULTSETID:
                self._id = part.value
            elif part.kind == part_kinds.RESULTSET:
                # Cleanup buffer
                del self._buffer
                self._buffer = deque()

                for row in self._unpack_rows(part.payload, part.rows):
                    self._buffer.append(row)

                self._received_last_resultset_part = part.attribute & 1
                self._executed = True
            elif part.kind == part_kinds.STATEMENTCONTEXT:
                pass
            else:
                raise InterfaceError(
                    "Prepared select statement response, unexpected part kind %d." % part.kind
                )

    def _handle_prepared_insert(self, parts):
        for part in parts:
            if part.kind == part_kinds.ROWSAFFECTED:
                self.rowcount = part.values[0]
            elif part.kind == part_kinds.TRANSACTIONFLAGS:
                pass
            elif part.kind == part_kinds.STATEMENTCONTEXT:
                pass
            else:
                raise InterfaceError(
                    "Prepared insert statement response, unexpected part kind %d." % part.kind
                )
        self._executed = True

    def _handle_select(self, parts):
        self.rowcount = -1

        # result metadata
        self.description, self._column_types = self._handle_result_metadata(parts[0])

        self._id = parts[1].value

        # Cleanup buffer
        del self._buffer
        self._buffer = deque()

        for row in self._unpack_rows(parts[3].payload, parts[3].rows):
            self._buffer.append(row)

        self._received_last_resultset_part = parts[3].attribute & 1
        self._executed = True

    def _handle_insert(self, parts):
        self.rowcount = parts[0].values[0]
        self.description = None

    def _unpack_rows(self, payload, rows):
        for i in iter_range(rows):
            yield tuple(
                typ.from_resultset(payload) for typ in self._column_types
            )

    def fetchmany(self, size=None):
        self._check_closed()
        if not self._executed:
            raise ProgrammingError("Require execute() first")
        if size is None:
            size = self.arraysize

        _result = []
        _missing = size

        while bool(self._buffer) and _missing > 0:
            _result.append(self._buffer.popleft())
            _missing -= 1

        if _missing == 0 or self._received_last_resultset_part:
            # No rows are missing or there are no additional rows
            return _result

        response = self._connection.Message(
            RequestSegment(
                message_types.FETCHNEXT,
                (ResultSetId(self._id), FetchSize(_missing))
            )
        ).send()

        if response.segments[0].parts[1].attribute & 1:
            self._received_last_resultset_part = True

        resultset_part = response.segments[0].parts[1]
        for row in self._unpack_rows(resultset_part.payload, resultset_part.rows):
            _result.append(row)
        return _result

    def fetchone(self):
        result = self.fetchmany(size=1)
        if result:
            return result[0]
        return None

    def fetchall(self):
        result = self.fetchmany()
        while bool(self._buffer) or not self._received_last_resultset_part:
            result = result + self.fetchmany()
        return result

    def close(self):
        self._connection = None

    def _check_closed(self):
        if self._connection is None or self._connection.closed:
            raise ProgrammingError("Cursor closed")
