import datetime
import logging
from qpython import qconnection
import atexit

from qpython.qcollection import QList, QTable

from kdb.kdb_row import KDBRow
from kdb.kdb_schema import KDBSchema


def in_qlist(element, qlist: QList):
    for e in qlist:
        if e == element:
            return True
    return False


class KDBConnectionEngine(object):
    HOST = 'localhost'
    PORT = 5000

    def __init__(self):
        self.logger = logging.getLogger(__name__);
        self.q = qconnection.QConnection(host=KDBConnectionEngine.HOST, port=KDBConnectionEngine.PORT, timeout=3.0)
        self.db_file_path = "/db"
        self.connect()

    def connect(self):
        self.q.open()
        atexit.register(self.close_connection)

    def create_table_by_schema(self, kdb_schema:KDBSchema):
        table_name = kdb_schema.table_name
        columns = kdb_schema.columns()
        self.create_table(table_name, *[str(c) for c in columns.keys()])

    def create_table(self, table_name: str, *column_names):
        if self.is_table_exist(table_name):
            self.logger.error(table_name + " already exist")
        column_names_statement = []
        for column_name in column_names:
            column_names_statement.append(column_name)
            column_names_statement.append(':()')
            column_names_statement.append(';')
        del column_names_statement[-1]

        statement = table_name + ':([]' + ''.join(column_names_statement) + ')'
        ret = self.q(statement)
        if ret is None:
            self.logger.error('Statement Execution Failure, Statement: ' + statement)
            return False
        if ret[0] != 0:
            self.logger.error('KDB Return Error: ' + ret[0], 'Statement: ' + statement)
            return False
        self.logger.info('create table ' + table_name)
        return True

    def is_table_exist(self, table_name: str):
        existing_tables = self.q('tables[]')
        if isinstance(existing_tables, QList) and in_qlist(bytes(table_name, encoding='UTF-8'), existing_tables):
            return True
        return False

    def remove_table(self, table_name: str):
        if not self.is_table_exist(table_name):
            self.logger.error(table_name + " not exist")
            return False
        self.q('delete ' + table_name + ' from `.')
        self.logger.info('delete ' + table_name)
        return True

    def insert_row(self, kdb_row: KDBRow):
        kdb_schema = kdb_row.kdb_schema
        table_name = kdb_schema.table_name
        data = kdb_row.get_row()
        self.insert_data(table_name, *data)


    def insert_data(self, table_name: str, *datas):
        if not self.is_table_exist(table_name):
            self.logger.error(table_name + " not exist")
            return False
        columns: QList = self.q('cols ' + table_name)
        if len(datas) != len(columns):
            self.logger.error(table_name + " columns not match update data " + datas)
            return False
        statement_data = []
        for data in datas:
            statement_data.append(str(data))
        statement = '`' + table_name + ' insert (' + ','.join(statement_data) + ')'
        self.logger.debug("execute kdb insert data:" + statement)
        self.q(statement)
        return True

    def list_all_data(self, table_name: str):
        if not self.is_table_exist(table_name):
            self.logger.error(table_name + " not exist")
            return False
        return self.q(table_name)

    def save_to_file(self, table_name: str, date: datetime.date):
        statement = "`:" + self.db_file_path + "/" + date.strftime("%Y.%m.%d") + "/" + table_name + "/ upsert .Q.en[`:. ;" + table_name + "]"
        self.logger.info("execute kdb statement: " + statement)
        self.q(statement)
        return True

    def remove_all_content(self, table_name:str, date: datetime.date):
        next_day = date + datetime.timedelta(days=1)
        statement = "delete from `" + table_name + " where " + table_name + "[`timestamp]<" + next_day.strftime("%Y.%m.%d")
        self.q(statement)
        return True

    def close_connection(self):
        if self.q.is_connected():
            self.q.close()
