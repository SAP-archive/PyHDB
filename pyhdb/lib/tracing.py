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
from __future__ import print_function
import io
import pyhdb


def trace(trace_obj):
    """Print recursive trace of given network protocol object
    :param trace_obj: either a message, segement, or kind object
    """
    if pyhdb.tracing:
        t = TraceLogger()
        tr = t.trace(trace_obj)
        print(tr)
        return tr


class TraceLogger(object):
    """Trace logger class for dumping header and binary data of messages, segments, and parts"""
    _indent_incr = 4

    def __init__(self):
        self._indent_level = 0
        self._indent_level_is_first = {0: True}
        self._buffer = io.StringIO()

    def trace(self, trace_obj):
        """
        Trace given trace_obj (usuall a Message instance) recursively.
        :param trace_obj:
        :return: a string with properly formatted tracing information
        """
        tracer = self
        tracer.writeln(u'%s = ' % trace_obj.__class__.__name__)
        tracer.incr('{')
        for attr_name in trace_obj.__tracing_attrs__:
            attr = getattr(trace_obj, attr_name)
            if isinstance(attr, tuple) and hasattr(attr, '_fields'):
                # probably a namedtuple instance
                tracer.writeln(u'%s = ' % (attr_name,))
                tracer.incr('[')
                for k, v in attr._asdict().items():
                    # _asdict() creates an OrderedDict, so elements are still in order
                    tracer.writeln(u'%s = %s' % (k, v))
                tracer.decr(']')
            elif isinstance(attr, (list, tuple)):
                if attr:
                    tracer.writeln(u'%s = ' % (attr_name,))
                    tracer.incr('[')
                    for elem in attr:
                        if hasattr(elem, '__tracing_attrs__'):
                            self.trace(elem)
                        else:
                            # some other plain list element, just print it as it is:
                            tracer.writeln(u'%s' % repr(elem))
                    tracer.decr(']')
                else:
                    tracer.writeln(u'%s = []' % (attr_name,))
            else:
                # a plain attribute object, just print it as it is
                tracer.writeln(u'%s = %s' % (attr_name, repr(attr)))
        tracer.decr('}')
        return self.getvalue()

    def incr(self, brace):
        self._buffer.write(u'%s\n' % brace)
        self._indent_level += self._indent_incr
        self._indent_level_is_first[self._indent_level] = True

    def decr(self, brace):
        assert self._indent_level > 0, 'Indentation level cannot be decremented any further'
        self._buffer.write(u'\n')
        self._indent_level -= self._indent_incr
        self._buffer.write(u' ' * self._indent_level)
        self._buffer.write(u'%s' % brace)

    def writeln(self, line):
        if self._indent_level_is_first[self._indent_level]:
            self._indent_level_is_first[self._indent_level] = False
        else:
            self._buffer.write(u',\n')

        self._buffer.write(u' ' * self._indent_level)
        self._buffer.write(line)

    def getvalue(self):
        return self._buffer.getvalue()
