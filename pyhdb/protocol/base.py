import struct
from io import BytesIO
from weakref import WeakValueDictionary

from pyhdb.exceptions import InterfaceError
from pyhdb._compat import with_metaclass

MAX_MESSAGE_SIZE = 2**17
MESSAGE_HEADER_SIZE = 32

MAX_SEGMENT_SIZE = MAX_MESSAGE_SIZE - MESSAGE_HEADER_SIZE
SEGMENT_HEADER_SIZE = 24

PART_HEADER_SIZE = 16


class Message(object):

    # Documentation Notation:
    # I8 I4 UI4 UI4 I2 I1 B[9]
    struct = struct.Struct('liIIhb9B')

    _session_id = None
    _packet_count = None

    def __init__(self, connection, segments=None):
        self.connection = connection

        if segments is None:
            self.segments = []
        elif isinstance(segments, (list, tuple)):
            self.segments = segments
        else:
            self.segments = [segments,]

    @property
    def session_id(self):
        """
        Identifer for session.
        """
        if self._session_id is not None:
            return self._session_id
        return self.connection.session_id

    @property
    def packet_count(self):
        """
        Sequence number for message inside of session.
        """
        if self._packet_count is None:
            self._packet_count = self.connection.get_next_packet_count()
        return self._packet_count

    @property
    def payload(self):
        """
        Build payload of message.
        """
        _payload = b""
        for segment in self.segments:
            _payload += segment.pack(commit=self.connection.autocommit)
        return _payload

    def pack(self):
        """
        Pack message to binary stream.
        """
        payload = self.payload

        packet_length = len(payload)
        total_space = MAX_MESSAGE_SIZE - MESSAGE_HEADER_SIZE
        count_of_segments = len(self.segments)

        return self.struct.pack(
            self.session_id,
            self.packet_count,
            packet_length,
            total_space,
            count_of_segments,
            *[0] * 10    # Reserved
        ) + payload

    def send(self):
        """
        Send message over connection and returns the reply message.
        """
        return self.connection._send_message(self.pack())

    @classmethod
    def unpack_reply(cls, connection, header, payload):
        """
        Takes already unpacked header and binary payload of received
        reply and creates Message object.
        """
        reply = Message(
            connection,
            tuple(ReplySegment.unpack_from(
                payload, expected_segments=header[4]
            ))
        )
        reply._session_id = header[0]
        reply._packet_count = header[1]
        return reply

class BaseSegment(object):

    # I4 I4 I2 I2 I1
    header_struct = struct.Struct('<iihhb')

    def __init__(self, parts=None):
        if parts is None:
            self.parts = []
        elif isinstance(parts, (list, tuple)):
            self.parts = parts
        else:
            self.parts = [parts,]

    def payload(self):
        remaining_size = MAX_SEGMENT_SIZE - SEGMENT_HEADER_SIZE

        payload = b""
        for parts in self.parts:
            payload += parts.pack(remaining_size)
            remaining_size -= len(payload)

        return payload

    @property
    def offset(self):
        return 0

    @property
    def number(self):
        return 1

    @property
    def kind(self):
        raise NotImplemented

    def pack(self, **kwargs):
        payload = self.payload()

        return self.header_struct.pack(
            SEGMENT_HEADER_SIZE + len(payload),
            self.offset,
            len(self.parts),
            self.number,
            self.kind
        ) + self.pack_additional_header(**kwargs) + payload

class RequestSegment(BaseSegment):

    kind = 1
    # I1 I1 I1 B[8]
    request_header_struct = struct.Struct('bbb8x')

    def __init__(self, message_type, parts=None):
        super(RequestSegment, self).__init__(parts)
        self.message_type = message_type

    @property
    def command_options(self):
        return 0

    def pack_additional_header(self, **kwargs):
        return self.request_header_struct.pack(
            self.message_type,
            int(kwargs.get('commit', 0)),
            self.command_options
        )

class ReplySegment(BaseSegment):

    kind = 2
    # I1 I2 B[8]
    reply_header_struct = struct.Struct('<bh8B')

    def __init__(self, function_code, parts=None):
        super(ReplySegment, self).__init__(parts)
        self.function_code = function_code

    @classmethod
    def unpack_from(cls, payload, expected_segments):
        num_segments = 0

        while num_segments < expected_segments:
            try:
                base_segment_header = cls.header_struct.unpack(
                    payload.read(13)
                )
            except struct.error:
                raise Exception("No valid segment header")

            # Read additional header fields
            try:
                segment_header = \
                    base_segment_header + cls.reply_header_struct.unpack(
                        payload.read(11)
                    )
            except struct.error:
                raise Exception("No valid reply segment header")

            if expected_segments == 1:
                # If we just expects one segment than we can take the full
                # payload. This also a workaround of an internal bug.
                segment_payload_size = -1
            else:
                segment_payload_size = segment_header[0] - SEGMENT_HEADER_SIZE

            # Determinate segment payload
            segment_payload = BytesIO(payload.read(segment_payload_size))

            num_segments += 1

            if base_segment_header[4] == 2:
                yield ReplySegment.unpack(segment_header, segment_payload)
            elif base_segment_header[4] == 5:
                error = ReplySegment.unpack(segment_header, segment_payload)
                raise error.parts[0].errors[0]
            else:
                raise Exception("Invalid reply segment")

    @classmethod
    def unpack(cls, header, payload):
        """
        Takes unpacked header and payload of segment and
        create ReplySegment object.
        """

        return cls(
            header[6],
            tuple(Part.unpack_from(payload, expected_parts=header[2]))
        )

    def pack(self):
        raise NotImplemented

part_mapping = WeakValueDictionary()

class PartMeta(type):
    """
    Meta class for part classes which also add them into part_mapping.
    """

    def __new__(cls, name, bases, attrs):
        part_class = super(PartMeta, cls).__new__(cls, name, bases, attrs)
        if not hasattr(part_class, "kind"):
            return part_class

        if not -128 <= part_class.kind <= 127:
            raise InterfaceError(
                "%s part kind must be between -128 and 127" %
                part_class.__name__
            )

        part_mapping[part_class.kind] = part_class
        return part_class

class Part(with_metaclass(PartMeta, object)):

    header_struct = struct.Struct('<bbhiii')
    attribute = 0

    # Attribute to get source of part
    source = 'client'

    def pack(self, remaining_size):
        arguments_count, payload = self.pack_data()
        payload_length = len(payload)

        # align payload length to multiple of 8
        if payload_length % 8 != 0:
            payload += b"\x00" * (8 - payload_length % 8)

        return self.header_struct.pack(
            self.kind, self.attribute, arguments_count, 0,
            payload_length, remaining_size
        ) + payload

    @classmethod
    def unpack_from(cls, payload, expected_parts):
        """Unpack parts from payload"""
        num_parts = 0

        while expected_parts > num_parts:
            try:
                part_header = cls.header_struct.unpack(
                    payload.read(16)
                )
            except struct.error:
                raise InterfaceError("No valid part header")

            if part_header[4] % 8 != 0:
                part_payload_size = part_header[4] + 8 - (part_header[4] % 8)
            else:
                part_payload_size = part_header[4]
            part_payload = BytesIO(payload.read(part_payload_size))

            try:
                PartClass = part_mapping[part_header[0]]
            except KeyError:
                raise InterfaceError(
                    "Unknown part kind %s" % part_header[0]
                )

            init_arguments = PartClass.unpack_data(
                part_header[2], part_payload
            )

            part = PartClass(*init_arguments)
            part.attribute = part_header[1]
            part.source = 'server'

            num_parts += 1
            yield part
