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

import struct
from collections import namedtuple
import pyhdb.protocol.constants.part_kinds
from pyhdb.protocol import types
from pyhdb.protocol import constants
from pyhdb.protocol.base import Part, PartMeta
from pyhdb.exceptions import InterfaceError, DatabaseError
from pyhdb._compat import is_text, iter_range, with_metaclass
from pyhdb.protocol.headers import ReadLobHeader


class Fields(object):

    @staticmethod
    def pack_data(fields):
        payload = struct.pack('<H', len(fields))
        for field in fields:
            if is_text(field):
                field = field.encode('cesu-8')

            size = len(field)
            if size >= 250:
                payload += b'\xFF' + struct.pack('H', size) + field
            else:
                payload += struct.pack('b', size) + field
        return payload

    @staticmethod
    def unpack_data(payload):
        length = struct.unpack('<H', payload.read(2))[0]
        fields = []

        for _ in iter_range(0, length):
            size = payload.read(1)
            if size == b"\xFF":
                size = struct.unpack('H', payload.read(2))[0]
            else:
                size = struct.unpack('b', size)[0]

            fields.append(payload.read(size))
        return fields


class OptionPartMeta(PartMeta):

    def __new__(cls, name, bases, attrs):
        part_class = super(OptionPartMeta, cls).__new__(
            cls, name, bases, attrs
        )
        if hasattr(part_class, "option_definition"):
            part_class.option_identifier = dict([
                (i[1][0], i[0]) for i in part_class.option_definition.items()
            ])
        return part_class


class OptionPart(with_metaclass(OptionPartMeta, Part)):
    """
    The multi-line option part format is a common format to
    transmit collections of options (typed key-value pairs).
    """

    __metaclass__ = OptionPartMeta

    def __init__(self, options):
        self.options = options

    def pack_data(self):
        payload = b""
        arguments = 0
        for option, value in self.options.items():
            try:
                key, typ = self.option_definition[option]
            except KeyError:
                raise InterfaceError("Unknown option identifier %s" % option)

            if value is None:
                continue

            if typ == 1:
                value = struct.pack('B', value)
            elif typ == 2:
                value = struct.pack('h', value)
            elif typ == 3:
                value = struct.pack('i', value)
            elif typ == 4:
                value = struct.pack('l', value)
            elif typ == 28:
                value = struct.pack('?', value)
            elif typ == 29 or typ == 30:
                value = value.encode('utf-8')
                value = struct.pack('h', len(value)) + value
            else:
                raise Exception("Unknown option type %s" % typ)

            arguments += 1
            payload += struct.pack('bb', key, typ) + value
        return arguments, payload

    @classmethod
    def unpack_data(cls, argument_count, payload):
        options = {}
        for _ in iter_range(argument_count):
            key, typ = struct.unpack('bb', payload.read(2))

            if key not in cls.option_identifier:
                key = 'Unknown_%d' % key
            else:
                key = cls.option_identifier[key]

            if typ == 1:
                value = struct.unpack('B', payload.read(1))[0]
            elif typ == 2:
                value = struct.unpack('h', payload.read(2))[0]
            elif typ == 3:
                value = struct.unpack('i', payload.read(4))[0]
            elif typ == 4:
                value = struct.unpack('l', payload.read(8))[0]
            elif typ == 28:
                value = struct.unpack('?', payload.read(1))[0]
            elif typ == 29 or typ == 30:
                length = struct.unpack('h', payload.read(2))[0]
                value = payload.read(length).decode('utf-8')
            elif typ == 24:
                # TODO: Handle type 24
                continue
            else:
                raise Exception("Unknown option type %s" % typ)

            options[key] = value

        return (options,)


class Command(Part):
    """
    This part contains the text of an SQL command.
    The text is encoded in CESU-8.
    """

    kind = constants.part_kinds.COMMAND

    def __init__(self, sql_statement):
        self.sql_statement = sql_statement

    def pack_data(self):
        payload = self.sql_statement.encode('cesu-8')
        return 1, payload

    @classmethod
    def unpack_data(cls, argument_count, payload):
        sql_statement = payload.read()
        return sql_statement.decode('cesu-8')


class ResultSet(Part):
    """
    This part contains the raw result data but without
    structure informations the unpacking is not possible. In a
    later step we will unpack the data.
    """

    kind = constants.part_kinds.RESULTSET

    def __init__(self, payload, rows):
        self.payload = payload
        self.rows = rows

    @classmethod
    def unpack_data(cls, argument_count, payload):
        return payload, argument_count


class Error(Part):

    kind = constants.part_kinds.ERROR
    struct = struct.Struct("iiib")

    def __init__(self, errors):
        self.errors = errors

    @classmethod
    def unpack_data(cls, argument_count, payload):
        errors = []
        for _ in iter_range(argument_count):
            code, position, textlength, level = cls.struct.unpack(
                payload.read(13)
            )
            # sqlstate = payload.read(5)
            errortext = payload.read(textlength).decode('utf-8')

            errors.append(DatabaseError(errortext, code))
        return tuple(errors),


class StatementId(Part):

    kind = constants.part_kinds.STATEMENTID

    def __init__(self, statement_id):
        self.statement_id = statement_id

    def pack_data(self):
        payload = bytearray(self.statement_id)
        return 1, payload

    @classmethod
    def unpack_data(cls, argument_count, payload):
        return payload.read(8),


class RowsAffected(Part):

    kind = constants.part_kinds.ROWSAFFECTED

    def __init__(self, values):
        self.values = values

    @classmethod
    def unpack_data(cls, argument_count, payload):
        values = []
        for _ in iter_range(argument_count):
            values.append(struct.unpack("<i", payload.read(4))[0])
        return tuple(values),


class ResultSetId(Part):
    """
    This part contains the identifier of a result set.
    """

    kind = constants.part_kinds.RESULTSETID

    def __init__(self, value):
        self.value = value

    def pack_data(self):
        return 1, self.value

    @classmethod
    def unpack_data(cls, argument_count, payload):
        value = payload.read()
        return (value,)


class TopologyInformation(Part):

    kind = constants.part_kinds.TOPOLOGYINFORMATION

    def __init__(self, *args):
        pass

    @classmethod
    def unpack_data(cls, argument_count, payload):
        # TODO
        return tuple()


class ReadLobRequest(Part):

    kind = constants.part_kinds.READLOBREQUEST
    part_struct = struct.Struct(b'<8sQI4s')

    def __init__(self, locator_id, readoffset, readlength):
        self.locator_id = locator_id
        self.readoffset = readoffset
        self.readlength = readlength

    def pack_data(self):
        """Pack data. readoffset has to be increased by one, seems like HANA starts from 1, not zero."""
        payload = self.part_struct.pack(self.locator_id, self.readoffset + 1, self.readlength, '    ')
        # print repr(payload)
        return 4, payload


class ReadLobReply(Part):

    kind = constants.part_kinds.READLOBREPLY
    part_struct_p1 = struct.Struct(b'<8sB')
    part_struct_p2 = struct.Struct(b'<I3s')

    def __init__(self, data, isDataIncluded, isLastData, isNull):
        # print 'realobreply called with args', args
        self.data = data
        self.isDataIncluded = isDataIncluded
        self.isLastData = isLastData
        self.isNull = isNull

    @classmethod
    def unpack_data(cls, argument_count, payload):
        locator_id, options = cls.part_struct_p1.unpack(payload.read(cls.part_struct_p1.size))
        isNull = options & ReadLobHeader.LOB_OPTION_ISNULL
        if isNull:
            # returned LOB is NULL
            lobdata = isDataIncluded = isLastData = None
            isNull = True
        else:
            chunklength, filler = cls.part_struct_p2.unpack(payload.read(cls.part_struct_p2.size))
            isDataIncluded = options & ReadLobHeader.LOB_OPTION_DATAINCLUDED
            if isDataIncluded:
                lobdata = payload.read()
            else:
                lobdata = ''
            isLastData = options & ReadLobHeader.LOB_OPTION_LASTDATA
            assert len(lobdata) == chunklength
        # print 'realobreply unpack data called with args', len(lobdata), isDataIncluded, isLastData
        return lobdata, isDataIncluded, isLastData, isNull


class Parameters(Part):
    """
    Prepared statement parameters' data
    """

    kind = constants.part_kinds.PARAMETERS

    def __init__(self, parameters):
        self.parameters = parameters

    def pack_data(self):
        payload = ''
        for parameter in self.parameters:
            type_code, value = parameter[1], parameter[3]
            try:
                if type_code in types.String.type_code:
                    pfield = types.by_type_code[type_code].prepare(value, type_code)
                elif value is None:
                    pfield = types.NoneType.prepare(type_code)
                else:
                    pfield = types.by_type_code[type_code].prepare(value)
            except KeyError:
                raise InterfaceError("Prepared statement parameter datatype not supported: %d" % type_code)
            payload += pfield
        return 1, payload


class Authentication(Part):

    kind = constants.part_kinds.AUTHENTICATION

    def __init__(self, user, methods):
        self.user = user
        self.methods = methods

    def pack_data(self):
        # Flat dict of methods
        fields = [self.user]
        for method_data in self.methods.items():
            fields = fields + list(method_data)

        payload = Fields.pack_data(fields)
        return 1, payload

    @classmethod
    def unpack_data(cls, argument_count, payload):
        fields = Fields.unpack_data(payload)

        methods = dict(zip(fields[0::2], fields[1::2]))
        return None, methods


class ClientId(Part):
    # Part not documented.

    kind = constants.part_kinds.CLIENTID

    def __init__(self, client_id):
        self.client_id = client_id

    def pack_data(self):
        payload = self.client_id.encode('utf-8')
        return 1, payload

    @classmethod
    def unpack_data(cls, argument_count, payload):
        client_id = payload.read(2048)
        return client_id.decode('utf-8')


class StatementContext(Part):

    kind = constants.part_kinds.STATEMENTCONTEXT

    def __init__(self, *args):
        pass

    @classmethod
    def unpack_data(cls, argument_count, payload):
        return tuple()


class ConnectOptions(OptionPart):

    kind = constants.part_kinds.CONNECTOPTIONS

    option_definition = {
        # Identifier, (Value, Type)
        "connection_id": (1, 3),
        "complete_array_execution": (2, 28),
        "client_locale": (3, 29),
        "supports_large_bulk_operations": (4, 28),
        "large_number_of_parameters_support": (10, 28),
        "system_id": (11, 29),
        "data_format_version": (12, 3),
        "select_for_update_supported": (14, 28),
        "client_distribution_mode": (15, 3),
        "engine_data_format_version": (16, 3),
        "distribution_protocol_version": (17, 3),
        "split_batch_commands": (18, 28),
        "use_transaction_flags_only": (19, 28),
        "row_and_column_optimized_format": (20, 28),
        "ignore_unknown_parts": (21, 28),
        "data_format_version2": (23, 3)
    }


class FetchSize(Part):

    kind = constants.part_kinds.FETCHSIZE
    struct = struct.Struct('i')

    def __init__(self, size):
        self.size = size

    def pack_data(self):
        return 1, self.struct.pack(self.size)

    @classmethod
    def unpack_data(cls, argument_count, payload):
        return cls.struct.unpack(payload.read())


class ParameterMetadata(Part):

    kind = constants.part_kinds.PARAMETERMETADATA

    def __init__(self, values):
        self.values = values

    @classmethod
    def unpack_data(cls, argument_count, payload):
        ParamMetadata = namedtuple('ParameterMetadata', 'options datatype mode id length fraction')
        values = []
        for _ in iter_range(argument_count):
            param = struct.unpack("bbbbIhhI", payload.read(16))
            if param[4] == 0xffffffff:
                # no parameter name given
                param_id = _
            else:
                # offset of the parameter name set
                payload.seek(param[4], 0)
                length, = struct.unpack('B', payload.read(1))
                param_id = payload.read(length).decode('utf-8')

            # replace name offset with param name, if parameter names supplied,
            # or parameter position (integer), if names not supplied)
            param_metadata = list(param)
            param_metadata[4] = param_id
            # remove unused fields
            del param_metadata[3]
            del param_metadata[6]

            options, datatype, mode, name, length, fraction = param_metadata
            param_metadata = ParamMetadata(options, datatype, mode, name, length, fraction)

            values.append(param_metadata)
        return tuple(values),


class ResultSetMetaData(Part):

    kind = constants.part_kinds.RESULTSETMETADATA

    def __init__(self, columns):
        self.columns = columns

    @classmethod
    def unpack_data(cls, argument_count, payload):
        columns = []
        for _ in iter_range(argument_count):
            meta = list(struct.unpack('bbhhhIIII', payload.read(24)))
            columns.append(meta)

        content_start = payload.tell()
        for column in columns:
            for i in iter_range(5, 9):
                if column[i] == 4294967295:
                    column[i] = None
                    continue

                payload.seek(content_start+column[i], 0)
                length, = struct.unpack('B', payload.read(1))
                column[i] = payload.read(length).decode('utf-8')

        columns = tuple([tuple(x) for x in columns])
        return columns,


class TransactionFlags(OptionPart):

    kind = constants.part_kinds.TRANSACTIONFLAGS

    option_definition = {
        # Identifier, (Value, Type)
        "rolledback": (0, 28),
        "commited": (1, 28),
        "new_isolation_level": (2, 3),
        "ddl_commit_mode_changed": (3, 28),
        "write_transaction_started": (4, 28),
        "no_write_transaction_started": (5, 28),
        "session_closing_transaction_error": (6, 28)
    }
