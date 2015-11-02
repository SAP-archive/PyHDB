# Copyright 2014, 2015 SAP SE
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

# Enable absolute import, otherwise the 'types' module of stdlib will not be found (conflicts with pyhdb types.py)
from __future__ import absolute_import
import collections

import io
import struct
import logging
from collections import namedtuple
from weakref import WeakValueDictionary
###
import pyhdb
from pyhdb.lib.stringlib import humanhexlify
from pyhdb.protocol import types
from pyhdb.protocol import constants
from pyhdb.protocol.types import by_type_code
from pyhdb.exceptions import InterfaceError, DatabaseError, DataError, IntegrityError
from pyhdb.compat import is_text, iter_range, with_metaclass, string_types, byte_type
from pyhdb.protocol.headers import ReadLobHeader, PartHeader, WriteLobHeader
from pyhdb.protocol.constants import parameter_direction

logger = logging.getLogger('pyhdb')
debug = logger.debug

PART_MAPPING = WeakValueDictionary()


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


class PartMeta(type):
    """
    Meta class for part classes which also add them into PART_MAPPING.
    """
    def __new__(mcs, name, bases, attrs):
        part_class = super(PartMeta, mcs).__new__(mcs, name, bases, attrs)
        if part_class.kind:
            if not -128 <= part_class.kind <= 127:
                raise InterfaceError("%s part kind must be between -128 and 127" % part_class.__name__)
            # Register new part class is registry dictionary for later lookup:
            PART_MAPPING[part_class.kind] = part_class
        return part_class


class Part(with_metaclass(PartMeta, object)):

    header_struct = struct.Struct('<bbhiii')  # 16 bytes
    header_size = header_struct.size
    attribute = 0
    kind = None
    bigargumentcount = 0  # what is this useful for? Seems to be always zero ...
    header = None
    trace_header = trace_payload = ''

    # Attribute to get source of part
    source = 'client'
    __tracing_attrs__ = ['header', 'trace_header', 'trace_payload']

    def pack(self, remaining_size):
        """Pack data of part into binary format"""
        arguments_count, payload = self.pack_data(remaining_size - self.header_size)
        payload_length = len(payload)

        # align payload length to multiple of 8
        if payload_length % 8 != 0:
            payload += b"\x00" * (8 - payload_length % 8)

        self.header = PartHeader(self.kind, self.attribute, arguments_count, self.bigargumentcount,
                                 payload_length, remaining_size)
        hdr = self.header_struct.pack(*self.header)
        if pyhdb.tracing:
            self.trace_header = humanhexlify(hdr, 30)
            self.trace_payload = humanhexlify(payload, 30)
        return hdr + payload

    def pack_data(self, remaining_size):
        raise NotImplemented()

    @classmethod
    def unpack_from(cls, payload, expected_parts):
        """Unpack parts from payload"""

        for num_part in iter_range(expected_parts):
            hdr = payload.read(cls.header_size)
            try:
                part_header = PartHeader(*cls.header_struct.unpack(hdr))
            except struct.error:
                raise InterfaceError("No valid part header")

            if part_header.payload_size % 8 != 0:
                part_payload_size = part_header.payload_size + 8 - (part_header.payload_size % 8)
            else:
                part_payload_size = part_header.payload_size
            pl = payload.read(part_payload_size)
            part_payload = io.BytesIO(pl)
            try:
                _PartClass = PART_MAPPING[part_header.part_kind]
            except KeyError:
                raise InterfaceError("Unknown part kind %s" % part_header.part_kind)

            debug('%s (%d/%d): %s', _PartClass.__name__, num_part+1, expected_parts, str(part_header))
            debug('Read %d bytes payload for part %d', part_payload_size, num_part + 1)

            init_arguments = _PartClass.unpack_data(part_header.argument_count, part_payload)
            debug('Part data: %s', init_arguments)
            part = _PartClass(*init_arguments)
            part.header = part_header
            part.attribute = part_header.part_attributes
            part.source = 'server'
            if pyhdb.tracing:
                part.trace_header = humanhexlify(hdr[:part_header.payload_size])
                part.trace_payload = humanhexlify(pl, 30)
            yield part


class Command(Part):
    """
    This part contains the text of an SQL command.
    The text is encoded in CESU-8.
    """

    kind = constants.part_kinds.COMMAND
    __tracing_attrs__ = Part.__tracing_attrs__ + ['sql_statement']

    def __init__(self, sql_statement):
        self.sql_statement = sql_statement

    def pack_data(self, remaining_size):
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
    __tracing_attrs__ = Part.__tracing_attrs__ + ['num_rows']

    def __init__(self, payload, num_rows):
        self.payload = payload
        self.num_rows = num_rows

    @classmethod
    def unpack_data(cls, argument_count, payload):
        return payload, argument_count

    def unpack_rows(self, column_types, connection):
        """Unpack rows for data (from a select statement) from payload and yield a single row at a time.
        :param column_types: a tuple of column descriptors
               e.g. (<class 'pyhdb.protocol.types.String'>, <class 'pyhdb.protocol.types.ClobType'>)
        :param connection: a db connection object
        :returns: a generator object
        """
        for _ in iter_range(self.num_rows):
            yield tuple(typ.from_resultset(self.payload, connection) for typ in column_types)


class OutputParameters(Part):
    """
    This part contains the raw result data but without
    structure informations the unpacking is not possible. In a
    later step we will unpack the data.
    """
    kind = constants.part_kinds.OUTPUTPARAMETERS
    __tracing_attrs__ = Part.__tracing_attrs__ + ['num_rows']

    def __init__(self, payload, num_rows):
        self.payload = payload
        self.num_rows = num_rows

    @classmethod
    def unpack_data(cls, argument_count, payload):
        return payload, argument_count

    def unpack_rows(self, parameters_metadata, connection):
        """Unpack output or input/output parameters from the stored procedure call result
        :parameters_metadata: a stored procedure parameters metadata
        :returns: parameter values
        """
        values = []
        for param in parameters_metadata:
            # Unpack OUT or INOUT parameters' values
            if param.iotype != parameter_direction.IN:
                values.append( by_type_code[param.datatype].from_resultset(self.payload) )
        yield tuple(values)


class Error(Part):

    kind = constants.part_kinds.ERROR
    part_struct = struct.Struct("iIIB5s")
    __tracing_attrs__ = Part.__tracing_attrs__ + ['errors']

    def __init__(self, errors):
        self.errors = errors

    @classmethod
    def unpack_data(cls, argument_count, payload):
        errors = []
        for _ in iter_range(argument_count):
            code, position, textlength, level, sqlstate = cls.part_struct.unpack(payload.read(cls.part_struct.size))
            errortext = payload.read(textlength).decode('utf-8')
            if code == 301:
                # Unique constraint violated
                errors.append(IntegrityError(errortext, code))
            else:
                errors.append(DatabaseError(errortext, code))
        return tuple(errors),


class StatementId(Part):

    kind = constants.part_kinds.STATEMENTID
    __tracing_attrs__ = Part.__tracing_attrs__ + ['statement_id']

    def __init__(self, statement_id):
        self.statement_id = statement_id

    def pack_data(self, remaining_size):
        payload = bytearray(self.statement_id)
        return 1, payload

    @classmethod
    def unpack_data(cls, argument_count, payload):
        return payload.read(8),


class RowsAffected(Part):

    kind = constants.part_kinds.ROWSAFFECTED
    __tracing_attrs__ = Part.__tracing_attrs__ + ['values']

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
    __tracing_attrs__ = Part.__tracing_attrs__ + ['value']

    def __init__(self, value):
        self.value = value

    def pack_data(self, remaining_size):
        return 1, self.value

    @classmethod
    def unpack_data(cls, argument_count, payload):
        value = payload.read()
        return value,


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
    __tracing_attrs__ = Part.__tracing_attrs__ + ['locator_id', 'readoffset', 'readlength']

    def __init__(self, locator_id, readoffset, readlength):
        self.locator_id = locator_id
        self.readoffset = readoffset
        self.readlength = readlength

    def pack_data(self, remaining_size):
        """Pack data. readoffset has to be increased by one, seems like HANA starts from 1, not zero."""
        payload = self.part_struct.pack(self.locator_id, self.readoffset + 1, self.readlength, b'    ')
        return 4, payload


class ReadLobReply(Part):

    kind = constants.part_kinds.READLOBREPLY
    part_struct_p1 = struct.Struct(b'<8sB')
    part_struct_p2 = struct.Struct(b'<I3s')
    __tracing_attrs__ = Part.__tracing_attrs__ + ['is_data_included', 'is_last_data', 'is_null']

    def __init__(self, data, is_data_included, is_last_data, is_null):
        self.data = data
        self.is_data_included = is_data_included
        self.is_last_data = is_last_data
        self.is_null = is_null

    @classmethod
    def unpack_data(cls, argument_count, payload):
        locator_id, options = cls.part_struct_p1.unpack(payload.read(cls.part_struct_p1.size))
        is_null = bool(options & ReadLobHeader.LOB_OPTION_ISNULL)
        if is_null:
            # returned LOB is NULL
            lobdata = is_data_included = is_last_data = None
            is_null = True
        else:
            chunklength, filler = cls.part_struct_p2.unpack(payload.read(cls.part_struct_p2.size))
            is_data_included = bool(options & ReadLobHeader.LOB_OPTION_DATAINCLUDED)
            if is_data_included:
                lobdata = payload.read()
            else:
                lobdata = ''
            is_last_data = bool(options & ReadLobHeader.LOB_OPTION_LASTDATA)
            assert len(lobdata) == chunklength
        return lobdata, is_data_included, is_last_data, is_null


class WriteLobRequest(Part):
    """Write/Send LOB data to server"""
    kind = constants.part_kinds.WRITELOBREQUEST
    part_struct = struct.Struct(b'<8sBQI')  # B[8] I1 I8 I4
    __tracing_attrs__ = Part.__tracing_attrs__ + ['lob_buffers']

    def __init__(self, lob_buffers):
        """
        :param lob_buffers: a deque containing lob buffers to put into lob write requests
        """
        self.lob_buffers = lob_buffers
        assert lob_buffers, 'List of lob buffers must include at least one lob'

    # noinspection PyUnboundLocalVariable
    def pack_data(self, max_payload_size):
        offset = 0  # a value of zero means: append lob data to existing LOB data in DB
        num_lobs = 0
        max_payload_size -= self.part_struct.size  # reduce by struct header size to simply math below
        payload = io.BytesIO()

        while payload.tell() < max_payload_size and self.lob_buffers:
            lb = self.lob_buffers.popleft()
            remaining_payload_size = max_payload_size - payload.tell()
            num_bytes_to_write = min(lb.num_bytes_to_write, remaining_payload_size)

            lob_options = WriteLobHeader.LOB_OPTION_DATAINCLUDED
            if num_bytes_to_write == lb.num_bytes_to_write:
                # i.e. the entire (rest of the) LOB will be written, so set LASTDATA flag:
                lob_options |= WriteLobHeader.LOB_OPTION_LASTDATA

            payload.write(self.part_struct.pack(lb.locator_id, lob_options, offset, num_bytes_to_write))
            payload.write(lb.encoded_data.read(num_bytes_to_write))
            num_lobs += 1

        # Since loop above was run at least once both 'lob_options' and 'lb' will be defined
        if not lob_options & WriteLobHeader.LOB_OPTION_LASTDATA:
            # last lob object was not written entirely -> put it back into lob_buffers for next round of writing:
            self.lob_buffers.appendleft(lb)
        return num_lobs, payload.getvalue()


class WriteLobReply(Part):
    """This part is received from HAHA after inserting a partial LOB through an insert or select statement"""
    kind = constants.part_kinds.WRITELOBREPLY
    part_struct = struct.Struct(b'<8sQI4s')
    __tracing_attrs__ = Part.__tracing_attrs__ + ['locator_ids']

    def __init__(self, locator_ids):
        self.locator_ids = locator_ids

    @classmethod
    def unpack_data(cls, argument_count, payload):
        """Unpack payload by splitting up the raw payload into list of locator_ids
        :param argument_count: number of locator_ids in payload is equal to argument_count
        :param payload: BytesIO instance with list of concatenated locator_ids, where each locator_id is 8 bytes long
        """
        pl = payload.read()
        locator_ids = [pl[start:start+8] for start in range(0, len(pl), 8)]
        return locator_ids,


class LobBuffer(object):
    def __init__(self, orig_data, DataType, lob_header_pos):
        self.orig_data = orig_data
        # Lob data can be either an instance of a Lob-class, or a string/unicode object, Encode properly:
        if isinstance(orig_data, byte_type):
            enc_data = orig_data
        elif isinstance(orig_data, string_types):
            enc_data = DataType.encode_value(orig_data)
        else:
            # assume a LOB instance:
            enc_data = orig_data.encode()
        self.encoded_data = io.BytesIO(enc_data)
        self.DataType = DataType
        self.lob_header_pos = lob_header_pos
        self.encoded_lob_size = len(enc_data)
        self.locator_id = None

    @property
    def num_bytes_to_write(self):
        """Return number of bytes of lobs which still have to be written"""
        return self.encoded_lob_size - self.encoded_data.tell()


class Parameters(Part):
    """Prepared statement parameters' data """

    kind = constants.part_kinds.PARAMETERS
    __tracing_attrs__ = Part.__tracing_attrs__ + ['parameters']

    def __init__(self, parameters):
        """Initialize parameter part
        :param parameters: A generator producing lists (1 per row) of named tuples containing parameter meta
                          data and values (usually an instance of class 'cursor.PreparedStatement')
               Example: [Parameter(id=0, datatype=9, length=255, value='row2'), Parameter(id=1, ...), ]
        :returns: tuple (arguments_count, payload)
        """
        self.parameters = parameters
        self.unwritten_lobs = []

    def pack_data(self, remaining_size):
        payload = io.BytesIO()
        num_rows = 0

        for row_parameters in self.parameters:
            # Loop over all input row parameters.
            # Memorize start position of row in buffer if it has to be removed in case that
            # the maximum message size will be exceeded (see below)
            row_header_start_pos = payload.tell()
            row_lobs = []
            row_lob_size_sum = 0

            for parameter in row_parameters:
                # 'parameter' is a named tuple, created in PreparedStatement.prepare_parameters()
                type_code, value = parameter.type_code, parameter.value
                try:
                    _DataType = types.by_type_code[type_code]
                except KeyError:
                    raise InterfaceError("Prepared statement parameter datatype not supported: %d" % type_code)

                if value is None:
                    pfield = types.NoneType.prepare(type_code)
                elif type_code in types.String.type_code:
                    pfield = _DataType.prepare(value, type_code)
                else:
                    pfield = _DataType.prepare(value)

                if type_code in (types.BlobType.type_code, types.ClobType.type_code, types.NClobType.type_code):
                    # In case of value being a lob its actual data is not yet included in 'pfield' generated above.
                    # Instead the lob data needs to be appended at the end of the packed row data.
                    # Memorize the position of the lob header data (the 'pfield'):
                    lob_header_pos = payload.tell()
                    lob_buffer = LobBuffer(value, _DataType, lob_header_pos)
                    # Add length of lob data to the sum so we can see whether all data fits into a segment below:
                    row_lob_size_sum += lob_buffer.encoded_lob_size
                    # Append lob data so it can be appended once all data for the row is packed:
                    row_lobs.append(lob_buffer)

                payload.write(pfield)

            if payload.tell() >= remaining_size:
                # Last row (even without lobs) does not fit anymore into the current message! Remove it from payload
                # by resetting payload pointer to former position and truncate away last row data:
                payload.seek(row_header_start_pos)
                payload.truncate()
                self.parameters.back()  # make generator to go one step back, so same item will be delivered again

                # Check for case that a row does not fit at all into a part block (i.e. it is the first one):
                if num_rows == 0:
                    raise DataError('Parameter row too large to fit into execute statement.'
                                    'Got: %d bytes, allowed: %d bytes' %
                                    (payload.tell() + row_lob_size_sum, remaining_size))
                break  # jump out of loop - no more rows to be added!
            else:
                # Keep row data.
                num_rows += 1
                # Now append as much as possible of actual binary lob data after the end of all parameters of this row.
                # All those LOBs which were not or only partially written to the payload will be collected in
                # 'unwritten_lobs' for further LOBWRITEREQUESTs.
                self.unwritten_lobs = self.pack_lob_data(remaining_size, payload, row_header_start_pos, row_lobs)

                if payload.tell() >= remaining_size:
                    # all the rest of the segment is filled with lob data, no more rows can be added:
                    break

        return num_rows, payload.getvalue()

    @staticmethod
    def pack_lob_data(remaining_size, payload, row_header_start_pos, row_lobs):
        """
        After parameter row has been written, append the lobs and update the corresponding lob headers
        with lob position and lob size:
        :param payload: payload object (io.BytesIO instance)
        :param row_header_start_pos: absolute position of start position of row within payload
        :param row_lobs: list of row buffer instance (containing binary encoded lob data, header position and DataType)
        """
        unwritten_lobs = collections.deque()
        for lob_buffer in row_lobs:
            # Calculate relative position of lob within the binary packed parameter row.
            # Add +1, Hana counts from 1, not 0!
            rel_lob_pos = payload.tell() - row_header_start_pos + 1

            # Calculate how much space is left in message for lob data:
            max_data_to_write = min(lob_buffer.encoded_lob_size, remaining_size - payload.tell())
            payload.write(lob_buffer.encoded_data.read(max_data_to_write))
            is_last_data = max_data_to_write == lob_buffer.encoded_lob_size
            if not is_last_data:
                # lob has not been written (partially or entirely) into message -> register for further write requests
                unwritten_lobs.append(lob_buffer)

            # Write position and size of lob data into lob header block:
            payload.seek(lob_buffer.lob_header_pos)
            payload.write(lob_buffer.DataType.prepare(None, length=max_data_to_write,
                                                      position=rel_lob_pos, is_last_data=is_last_data))
            # Set pointer back to end for further writing
            payload.seek(0, io.SEEK_END)
        return unwritten_lobs


class Authentication(Part):

    kind = constants.part_kinds.AUTHENTICATION
    __tracing_attrs__ = Part.__tracing_attrs__ + ['user', 'methods']

    def __init__(self, user, methods):
        self.user = user
        self.methods = methods

    def pack_data(self, remaining_size):
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
    __tracing_attrs__ = Part.__tracing_attrs__ + ['client_id']

    def __init__(self, client_id):
        self.client_id = client_id

    def pack_data(self, remaining_size):
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


class FetchSize(Part):

    kind = constants.part_kinds.FETCHSIZE
    struct = struct.Struct('i')
    __tracing_attrs__ = Part.__tracing_attrs__ + ['size']

    def __init__(self, size):
        self.size = size

    def pack_data(self, remaining_size):
        return 1, self.struct.pack(self.size)

    @classmethod
    def unpack_data(cls, argument_count, payload):
        return cls.struct.unpack(payload.read())


class ParameterMetadata(Part):

    kind = constants.part_kinds.PARAMETERMETADATA
    __tracing_attrs__ = Part.__tracing_attrs__ + ['values']

    def __init__(self, values):
        self.values = values

    @classmethod
    def unpack_data(cls, argument_count, payload):
        values = []
        param_md_tuple = namedtuple('ParameterMetadata', 'mode datatype iotype id length fraction')
        text_offset = 16 * argument_count
        # read parameter metadata
        for i in iter_range(argument_count):
            mode, datatype, iotype, filler1, name_offset, length, fraction, filler2 = \
                struct.unpack("bbbbIhhI", payload.read(16))
            param_metadata = param_md_tuple(mode, datatype, iotype, name_offset, length, fraction)
            if name_offset == 0xffffffff:
                # param id is parameter position
                param_id = i
            else:
                # read parameter name
                current_pos = payload.tell()
                payload.seek(text_offset + name_offset)
                length = ord(payload.read(1))
                param_id = payload.read(length).decode('utf-8')
                payload.seek(current_pos)
            values.append(param_md_tuple(mode, datatype, iotype, param_id, length, fraction))
        return tuple(values),


class ResultSetMetaData(Part):

    kind = constants.part_kinds.RESULTSETMETADATA
    __tracing_attrs__ = Part.__tracing_attrs__ + ['columns']

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


class OptionPartMeta(PartMeta):

    def __new__(mcs, name, bases, attrs):
        part_class = super(OptionPartMeta, mcs).__new__(
            mcs, name, bases, attrs
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

    def pack_data(self, remaining_size):
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
                value = struct.pack('q', value)
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
                value = struct.unpack('q', payload.read(8))[0]
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

        return options,


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
