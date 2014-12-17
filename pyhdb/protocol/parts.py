import struct
import pyhdb.cesu8
from pyhdb.protocol.base import Part, PartMeta
from pyhdb.exceptions import InterfaceError, DatabaseError
from pyhdb._compat import is_text, iter_range, with_metaclass

class Fields():

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

        for i in iter_range(0, length):
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

            if typ == 3:
                value = struct.pack('i', value)
            elif typ == 4:
                value = struct.pack('l', value)
            elif typ == 28:
                value = struct.pack('?', value)
            elif typ == 29:
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
        for i in iter_range(argument_count):
            key, typ = struct.unpack('bb', payload.read(2))
            if key not in cls.option_identifier:
                continue
            key = cls.option_identifier[key]

            if typ == 3:
                value = struct.unpack('i', payload.read(4))[0]
            elif typ == 4:
                value = struct.unpack('l', payload.read(8))[0]
            elif typ == 28:
                value = struct.unpack('?', payload.read(1))[0]
            elif typ == 29:
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

    kind = 3

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
    structur informations the unpacking is not possible. In a
    later step we will unpack the data.
    """

    kind = 5

    def __init__(self, payload, rows):
        self.payload = payload
        self.rows = rows

    @classmethod
    def unpack_data(cls, argument_count, payload):
        return payload, argument_count

class Error(Part):

    kind = 6
    struct = struct.Struct("iiib")

    def __init__(self, errors):
        self.errors = errors

    @classmethod
    def unpack_data(cls, argument_count, payload):
        errors = []
        for i in iter_range(argument_count):
            code, position, textlength, level = cls.struct.unpack(
                payload.read(13)
            )
            sqlstate = payload.read(5)
            errortext = payload.read(textlength).decode('utf-8')

            errors.append(DatabaseError(errortext, code))
        return tuple(errors),

class RowsAffected(Part):

    kind = 12

    def __init__(self, values):
        self.values = values

    @classmethod
    def unpack_data(cls, argument_count, payload):
        values = []
        for i in iter_range(argument_count):
            values.append(struct.unpack("<i", payload.read(4))[0])
        return tuple(values),

class ResultSetId(Part):
    """
    This part contains the identifier of a result set.
    """

    kind = 13

    def __init__(self, value):
        self.value = value

    def pack_data(self):
        return 1, self.value

    @classmethod
    def unpack_data(cls, argument_count, payload):
        value = payload.read()
        return (value,)

class TopologyInformation(Part):

    kind = 15

    def __init__(self, *args):
        pass

    @classmethod
    def unpack_data(cls, argument_count, payload):
        # TODO
        return tuple()

class Authentication(Part):

    kind = 33

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

    kind = 35

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

    kind = 39

    def __init__(self, *args):
        pass

    @classmethod
    def unpack_data(cls, argument_count, payload):
        return tuple()

class ConnectOptions(OptionPart):

    kind = 42

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

    kind = 45
    struct = struct.Struct('i')

    def __init__(self, size):
        self.size = size

    def pack_data(self):
        return 1, self.struct.pack(self.size)

    @classmethod
    def unpack_data(cls, argument_count, payload):
        return self.struct.unpack(payload.read())

class ResultSetMetaData(Part):

    kind = 48

    def __init__(self, columns):
        self.columns = columns

    @classmethod
    def unpack_data(cls, argument_count, payload):
        columns = []
        for i in iter_range(argument_count):
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

    kind = 64

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
