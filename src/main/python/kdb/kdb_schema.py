from datetime import datetime
from collections import OrderedDict
from enum import Enum


class KDBType(Enum):
    SYMBOL = 'symbol'
    STRING = 'string'
    TIMESTAMP = 'timestamp'
    INT = 'int'
    FLOAT = 'float'


class KDBData(object):
    def __init__(self):
        self.kdb_type = None


class KDBStr(KDBData):
    def __init__(self, inner_str: str):
        super().__init__()
        self.kdb_type = KDBType.STRING
        self.inner_str = inner_str

    def __str__(self):
        return '"{0}"'.format(self.inner_str)


class KDBSymbol(KDBData):
    def __init__(self, inner_str: str):
        super().__init__()
        self.kdb_type = KDBType.SYMBOL
        self.inner_str = inner_str

    def __str__(self):
        return '`{0}'.format(self.inner_str)


class KDBInt(KDBData):
    def __init__(self, inner_int: int):
        super().__init__()
        self.kdb_type = KDBType.INT
        self.inner_int = inner_int

    def __str__(self):
        return str(self.inner_int)


class KDBFloat(KDBData):
    def __init__(self, inner_float: float):
        super().__init__()
        self.kdb_type = KDBType.FLOAT
        self.inner_float = inner_float

    def __str__(self):
        return str(float(self.inner_float))
class KDBTimestamp(KDBData):
    def __init__(self, inner_timestamp: datetime):
        super().__init__()
        self.kdb_type = KDBType.TIMESTAMP
        self.inner_timestamp = inner_timestamp

    def __str__(self):
        return self.inner_timestamp.strftime('%Y.%m.%dT%H:%M:%S.000')


class KDBSchema(object):
    def __init__(self, table_name):
        self.cols = []
        self.types = []
        self.table_name = table_name
        self.forbidden_column_name = ['type']

    def add_column(self, column_name: str, type: KDBType):
        if column_name in self.forbidden_column_name:
            raise ValueError("illegal column name: " + column_name)
        self.cols.append(column_name)
        self.types.append(type)

    def columns(self):
        ret_dict = OrderedDict()
        for i in range(0, len(self.cols)):
            ret_dict[self.cols[i]] = self.types[i]
        return ret_dict
