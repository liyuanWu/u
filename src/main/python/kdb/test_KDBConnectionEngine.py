import datetime
from unittest import TestCase

from kdb.kdb_connection_engine import KDBConnectionEngine


class TestKDBConnectionEngine(TestCase):
    def test_add_table(self):
        kdbConnectionEngine = KDBConnectionEngine()
        print('create table', kdbConnectionEngine.create_table('table_name4', 'column_A', 'column_B'))
        print('is exist', kdbConnectionEngine.is_table_exist('table_name4'))
        #print('insert data', kdbConnectionEngine.insert_data('table_name4', KDBString('a'), KDBStr('b')))
        print('list all data', kdbConnectionEngine.list_all_data('table_name4'))
        print('remove table', kdbConnectionEngine.remove_table('table_name4'))
        pass

    def test_save(self):
        kdbConnectionEngine = KDBConnectionEngine()
        kdbConnectionEngine.save_to_file('spot_depth_ltc', datetime.date(2017, 7, 16))

    def test_remove(self):
        kdbConnectionEngine = KDBConnectionEngine()
        kdbConnectionEngine.remove_all_content('spot_depth_ltc', datetime.date(2017, 7, 16))

