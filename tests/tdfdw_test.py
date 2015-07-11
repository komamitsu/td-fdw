import unittest
from tdfdw.tdfdw import TreasureDataFdw
from multicorn import Qual, ANY, ALL

class TreasureDataFdwTest(unittest.TestCase):

    def setUp(self):
        self.td_fdw = TreasureDataFdw({
            'apikey': 'apikey1234',
            'database': 'foodb',
            'table': 'bartbl'
        }, ['name', 'age'])

    def test_init(self):
        td_fdw = self.td_fdw
        self.assertEqual(td_fdw.apikey, 'apikey1234')
        self.assertEqual(td_fdw.database, 'foodb')
        self.assertEqual(td_fdw.table, 'bartbl')
        self.assertEqual(td_fdw.query_engine, 'presto')
        self.assertEqual(td_fdw.columns, ['name', 'age'])

        td_fdw = TreasureDataFdw({
            'apikey': 'apikey1234',
            'database': 'foodb',
            'query': 'SELECT code, COUNT(1) from access_log GROUP BY code',
            'query_engine': 'hive',
        }, ['name', 'age'])
        self.assertEqual(td_fdw.apikey, 'apikey1234')
        self.assertEqual(td_fdw.database, 'foodb')
        self.assertEqual(td_fdw.query,
                'SELECT code, COUNT(1) from access_log GROUP BY code')
        self.assertEqual(td_fdw.query_engine, 'hive')
        self.assertEqual(td_fdw.columns, ['name', 'age'])

        td_fdw = TreasureDataFdw({}, [])
        self.assertEqual(td_fdw.apikey, None)
        self.assertEqual(td_fdw.database, None)
        self.assertEqual(td_fdw.query, None)
        self.assertEqual(td_fdw.query_engine, 'presto')
        self.assertEqual(td_fdw.columns, [])

    def test_create_cond_string(self):
        quals = [Qual('name', '=', 'alice')]
        self.assertEqual(self.td_fdw.create_cond(quals), "(name = 'alice')")
        quals = [Qual('name', '!=', 'alice')]
        self.assertEqual(self.td_fdw.create_cond(quals), "(name != 'alice')")
        quals = [Qual('name', '<>', 'alice')]
        self.assertEqual(self.td_fdw.create_cond(quals), "(name <> 'alice')")
        quals = [Qual('name', '>', 'alice')]
        self.assertEqual(self.td_fdw.create_cond(quals), "(name > 'alice')")
        quals = [Qual('name', '>=', 'alice')]
        self.assertEqual(self.td_fdw.create_cond(quals), "(name >= 'alice')")
        quals = [Qual('name', '<', 'alice')]
        self.assertEqual(self.td_fdw.create_cond(quals), "(name < 'alice')")
        quals = [Qual('name', '<=', 'alice')]
        self.assertEqual(self.td_fdw.create_cond(quals), "(name <= 'alice')")
        quals = [Qual('name', '~~', '%alice%')]
        self.assertEqual(self.td_fdw.create_cond(quals), "(name LIKE '%alice%')")
        quals = [Qual('name', ('=', True), ['alice', 'bob'])]
        self.assertEqual(self.td_fdw.create_cond(quals),
                "(name = 'alice' OR name = 'bob')")
        quals = [Qual('name', ('<', False), ['alice', 'bob'])]
        self.assertEqual(self.td_fdw.create_cond(quals),
                "(name < 'alice' AND name < 'bob')")
        quals = [Qual('name', '=', None)]
        self.assertEqual(self.td_fdw.create_cond(quals), "(name IS NULL)")
        quals = [Qual('name', '<>', None)]
        self.assertEqual(self.td_fdw.create_cond(quals), "(name IS NOT NULL)")
        quals = [Qual('name', '=', "al'ice")]
        self.assertEqual(self.td_fdw.create_cond(quals), "(name = 'al''ice')")
        
    def test_create_cond_int(self):
        quals = [Qual('age', '=', 42)]
        self.assertEqual(self.td_fdw.create_cond(quals), "(age = 42)")
        quals = [Qual('age', '!=', 42)]
        self.assertEqual(self.td_fdw.create_cond(quals), "(age != 42)")
        quals = [Qual('age', '<>', 42)]
        self.assertEqual(self.td_fdw.create_cond(quals), "(age <> 42)")
        quals = [Qual('age', '>', 42)]
        self.assertEqual(self.td_fdw.create_cond(quals), "(age > 42)")
        quals = [Qual('age', '>=', 42)]
        self.assertEqual(self.td_fdw.create_cond(quals), "(age >= 42)")
        quals = [Qual('age', '<', 42)]
        self.assertEqual(self.td_fdw.create_cond(quals), "(age < 42)")
        quals = [Qual('age', '<=', 42)]
        self.assertEqual(self.td_fdw.create_cond(quals), "(age <= 42)")
        quals = [Qual('age', ('=', True), [42, 99])]
        self.assertEqual(self.td_fdw.create_cond(quals),
                "(age = 42 OR age = 99)")
        quals = [Qual('age', ('<', False), [42, 99])]
        self.assertEqual(self.td_fdw.create_cond(quals),
                "(age < 42 AND age < 99)")
        quals = [Qual('age', '=', None)]
        self.assertEqual(self.td_fdw.create_cond(quals), "(age IS NULL)")
        quals = [Qual('age', '<>', None)]
        self.assertEqual(self.td_fdw.create_cond(quals), "(age IS NOT NULL)")
        
    def test_create_cond_multi_col(self):
        quals = [Qual('name', '=', 'alice'), Qual('age', '<', 42)]
        self.assertEqual(self.td_fdw.create_cond(quals), "(name = 'alice') AND (age < 42)")
        quals = [Qual('name', '=', None), Qual('age', ('=', True), [42, 99])]
        self.assertEqual(self.td_fdw.create_cond(quals),
                "(name IS NULL) AND (age = 42 OR age = 99)")
        quals = [Qual('name', ('~~', False), ['%alice%', '%bob%']), Qual('age', '<>', None)]
        self.assertEqual(self.td_fdw.create_cond(quals),
                "(name LIKE '%alice%' AND name LIKE '%bob%') AND (age IS NOT NULL)")
