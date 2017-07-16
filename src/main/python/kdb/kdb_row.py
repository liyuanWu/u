import logging

from kdb.kdb_schema import KDBSchema, KDBData, KDBType


class KDBRow(object):
    def __init__(self, kdb_schema: KDBSchema):
        self.logger = logging.getLogger(__name__)
        self.datas = []
        self.kdb_schema = kdb_schema

    def prepare_statement(self, *args):
        columns = self.kdb_schema.columns()
        columns_value = columns.values()

        if len(columns) != len(args):
            self.logger.error("args length not match to schema:" + str(columns) + " (args: " + str(args) + ")")
            return False

        for i in range(0, len(args)):
            if not isinstance(args[i], KDBData):
                self.logger.error("invalid data pass to kdb row " + str(args))
                return False
            data: KDBData = args[i]
            column_type:KDBType = list(columns_value)[i]
            if data.kdb_type != column_type:
                self.datas = []
                self.logger.error("column type not match" + str(columns) + " (args:" + str(args) + ")")
                return False
            self.datas.append(data)

    def get_row(self):
        return self.datas
