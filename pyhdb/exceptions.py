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

# Enable absolute import, otherwise the 'exceptions' module of stdlib will not be found
from __future__ import absolute_import


class Error(Exception):
    pass

class Warning(Warning):
    pass

class InterfaceError(Error):
    pass


class DatabaseError(Error):

    def __init__(self, message, code=None):
        super(DatabaseError, self).__init__(message)
        self.code = code


class InternalError(DatabaseError):
    pass


class OperationalError(DatabaseError):
    pass


class ConnectionTimedOutError(OperationalError):

    def __init__(self, message=None):
        super(ConnectionTimedOutError, self).__init__(message)


class ProgrammingError(DatabaseError):
    pass


class IntegrityError(DatabaseError):
    pass


class DataError(DatabaseError):
    pass


class NotSupportedError(DatabaseError):
    pass
