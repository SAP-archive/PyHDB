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

import codecs
from pyhdb.compat import PY2, unichr


class IncrementalDecoder(codecs.BufferedIncrementalDecoder):
    # Decoder inspired by python-ftfy written by Rob Speer
    # https://github.com/LuminosoInsight/python-ftfy/blob/master/ftfy/bad_codecs/utf8_variants.py

    def _buffer_decode(self, input, errors, final):
        decoded_segments = []
        position = 0

        while True:
            decoded, consumed = self._buffer_decode_step(
                input[position:], errors, final
            )

            if consumed == 0:
                break

            decoded_segments.append(decoded)
            position += consumed

        if final and position != len(input):
            raise Exception("Final decoder doesn't decoded all bytes")

        return u''.join(decoded_segments), position

    def _buffer_decode_step(self, input, errors, final):
        # If begin of CESU-8 sequence
        if input.startswith(b'\xed'):
            if len(input) < 6:
                if final:
                    # There aren't six bytes to decode.
                    return codecs.utf_8_decode(input, errors, final)
                else:
                    # Stream is not done yet
                    return u'', 0
            elif input[3] == 237 or (PY2 and input[3] == b"\xed"):
                if PY2:
                    bytenums = [ord(b) for b in input[:6]]
                else:
                    bytenums = input

                codepoint = (
                    ((bytenums[1] & 0x0f) << 16) +
                    ((bytenums[2] & 0x3f) << 10) +
                    ((bytenums[4] & 0x0f) << 6) +
                    (bytenums[5] & 0x3f) +
                    0x10000
                )
                return unichr(codepoint), 6

        # Fallback to UTF-8
        return codecs.utf_8_decode(input, errors, final)


class IncrementalEncoder(codecs.BufferedIncrementalEncoder):

    def _buffer_encode(self, input, errors, final=False):
        encoded_segments = []
        position = 0
        input_length = len(input)

        while position + 1 <= input_length:
            encoded, consumed = self._buffer_encode_step(
                input[position], errors, final
            )

            if consumed == 0:
                break

            encoded_segments.append(encoded)
            position += consumed

        if final and position != len(input):
            raise Exception("Final encoder doesn't encode all characters")

        return b''.join(encoded_segments), position

    def _buffer_encode_step(self, char, errors, final):
        codepoint = ord(char)
        if codepoint < 65535:
            return codecs.utf_8_encode(char, errors)
        else:
            seq = bytearray(6)
            seq[0] = 0xED
            seq[1] = 0xA0 | (((codepoint & 0x1F0000) >> 16) - 1)
            seq[2] = 0x80 | (codepoint & 0xFC00) >> 10
            seq[3] = 0xED
            seq[4] = 0xB0 | ((codepoint >> 6) & 0x3F)
            seq[5] = 0x80 | (codepoint & 0x3F)
            return bytes(seq), 1


def encode(input, errors='strict'):
    return IncrementalEncoder(errors).encode(input, final=True), len(input)


def decode(input, errors='strict'):
    return IncrementalDecoder(errors).decode(input, final=True), len(input)


class StreamWriter(codecs.StreamWriter):
    encode = encode


class StreamReader(codecs.StreamReader):
    decode = decode


CESU8_CODEC_INFO = codecs.CodecInfo(
    name="cesu-8",
    encode=encode,
    decode=decode,
    incrementalencoder=IncrementalEncoder,
    incrementaldecoder=IncrementalDecoder,
    streamreader=StreamReader,
    streamwriter=StreamWriter,
)


def search_function(encoding):
    if encoding == 'cesu-8':
        return CESU8_CODEC_INFO
    else:
        return None

codecs.register(search_function)
