import types

class ResultRow(object):
    """
    Result Row returned by fetch*()
    """
    def __init__(self, column_names=(), column_values=()):
        self.__update(column_names, column_values)

    def __repr__(self):
        return str(self.column_values)

    def __update(self, column_names, column_values):
        self.column_names = list(column_names)
        self.column_values = column_values

    def __setitem__(self, key, value):
        if type(key) == types.IntType:
            self.column_values[key] = value
        elif type(key) == types.StringType:
            try:
                ind = self.column_names.index(key.upper())
                self.column_values[ind] = value
            except ValueError:
                raise KeyError("\'%s\' is not found" % key.upper())
        else:
            raise TypeError("%s is not supported as a key" % str(type(key)))

    def __getitem__(self, key):
        if type(key) == types.IntType:
            return self.column_values[key]
        elif type(key) == types.SliceType:
            return self.column_values[key.start:key.stop:key.step]
        elif type(key) == types.StringType:
            try:
                ind = self.column_names.index(key.upper())
                return self.column_values[ind]
            except ValueError:
                raise KeyError("\'%s\' is not found" % key.upper())
        else:
            raise TypeError("%s is not supported as a key" % str(type(key)))

    def __len__(self):
        return len(self.column_values)


    def __iter__(self):
        for value in self.column_values:
            yield value

    def __cmp__(self, other):
        if not isinstance(other, ResultRow):
            raise TypeError("%s is not a result row fetched by pyhdb" % (other,))
        if self.column_values < other.column_values:
            return -1
        elif self.column_values == other.column_values:
            return 0
        else:  # self > other
            return 1
