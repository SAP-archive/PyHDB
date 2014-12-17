from collections import deque

from pyhdb.protocol.base import RequestSegment
from pyhdb.protocol.types import escape_values, by_type_code
from pyhdb.protocol.parts import Command, FetchSize, ResultSetId
from pyhdb.protocol.constants import message_types, function_codes
from pyhdb.exceptions import ProgrammingError, InterfaceError
from pyhdb._compat import iter_range

def format_operation(operation, parameters=None):
    if parameters is not None:
        operation = operation % escape_values(parameters)
    return operation

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

    def execute(self, operation, parameters=None):
        self._check_closed()

        operation = format_operation(operation, parameters)

        # Request resultset
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

    def _handle_select(self, parts):
        self.rowcount = -1

        description = []
        column_types = []
        for column in parts[0].columns:
            description.append(
                (column[8], column[1], None, column[3], column[2], None,
                 column[0] & 0b10)
            )

            if column[1] not in by_type_code:
                raise InterfaceError(
                    "Unknown column data type: %s" % column[1]
                )
            column_types.append(by_type_code[column[1]])

        self.description = tuple(description)
        self._column_types = tuple(column_types)

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
