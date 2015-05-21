# Copyright 2014, 2015 SAP SE.
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

import pyhdb
from pyhdb.protocol.message import RequestMessage
from pyhdb.protocol.segments import RequestSegment
from pyhdb.protocol.parts import Command
from pyhdb.protocol.constants import message_types
from pyhdb.protocol.headers import MessageHeader
from pyhdb.lib.tracing import trace

TRACE_MSG = '''RequestMessage = {
    header = [
        session_id = 5,
        packet_count = 3,
        payload_length = 500,
        varpartsize = 500,
        num_segments = 1,
        packet_options = 0
    ],
    segments = [
        RequestSegment = {
            header = None,
            parts = [
                Command = {
                    header = None,
                    trace_header = '',
                    trace_payload = '',
                    sql_statement = 'select * from dummy'
                }
            ]
        }
    ]
}'''


def test_tracing_output():
    msg_header = MessageHeader(session_id=5, packet_count=3, payload_length=500, varpartsize=500,
                               num_segments=1, packet_options=0)
    request = RequestMessage(
        session_id=msg_header.session_id,
        packet_count=msg_header.packet_count,
        segments=RequestSegment(
            message_types.EXECUTE,
            Command('select * from dummy')
        ),
        header=msg_header
    )

    pyhdb.tracing = True
    try:
        trace_msg = trace(request)
    finally:
        pyhdb.tracing = False

    assert trace_msg == TRACE_MSG
