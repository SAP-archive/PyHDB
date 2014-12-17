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