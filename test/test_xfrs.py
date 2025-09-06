import unittest
import os
import sys
import sqlite3

# add `xfrs` source tree into PYTHONPATH
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import xfrs

class TestSyntaxElements(unittest.TestCase):

    def setUp(self):
        self.dbh = sqlite3.connect(':memory:')

    def tearDown(self):
        if self.dbh is not None:
            self.dbh.close()
            self.dbh = None

    def getTableRecordsCount(self, tableName):
        cursor = self.dbh.cursor()

        # test first if table exists
        stmt = f"select name from sqlite_master where type='table' and name='{tableName}';"
        cursor.execute(stmt)
        rows = cursor.fetchall()
        if len(rows) == 0: return 0

        # get the number of records in the table
        stmt = f'select count(id) from {tableName};'
        cursor.execute(stmt)
        rows = cursor.fetchall()
        if len(rows) == 0:
            return 0
        else:
            return rows[0][0]


    def test_addRecord_buyValid(self):
        """Tests adding a new *buy* record to DB."""

        # check empty DB
        self.assertEqual( self.getTableRecordsCount('xfrs'), 0)

        # add record
        r = [ 'buy', '2025-09-06', 10, 239.69, 'USD', None, 'AAPL', 5.0, 'USD' ]
        xfrs.addRecord(self.dbh, r)
        self.assertEqual(self.getTableRecordsCount('xfrs'), 1)

        # get the record from DB and test values
        stmt = f"select {','.join(xfrs.REC_INDEXES.keys())} from xfrs;"
        cursor = self.dbh.cursor()
        cursor.execute(stmt)
        rows = cursor.fetchall()
        for i in xfrs.REC_INDEXES.values():
            if r[i] is not None:
                self.assertEqual(rows[0][i], r[i])


    def test_cacheQuote_valid(self):
        """Tests adding a new quote record to DB."""

        # check empty DB
        self.assertEqual( self.getTableRecordsCount('quotes'), 0)

        # add record
        q = {
            'date': '2025-09-06',
            'type': 'test',
            'price': 239.69,
            'currency': 'USD',
            }
        xfrs.cacheQuote(self.dbh, 'AAPL', q)
        self.assertEqual(self.getTableRecordsCount('quotes'), 1)

        # get the record from DB and test values
        keyMap = {
            'date': 'date',
            'type': 'type',
            'price': 'price',
            'curr': 'currency',
            }
        keys = list(keyMap.keys())
        stmt = f"select {','.join(keys)} from quotes;"
        cursor = self.dbh.cursor()
        cursor.execute(stmt)
        rows = cursor.fetchall()
        for i in range(len(keys)):
            self.assertEqual(rows[0][i], q[keyMap[keys[i]]])


    def test_getStockCurrency_valid_usdXfers(self):
        """Tests getting a currency of a stock symbol when correct transaction records in DB."""

        # add records
        recs = [
            [ 'buy', '2024-01-01', 1, 190.914, 'USD', None, 'AAPL', 5.0, 'USD' ],
            [ 'buy', '2025-01-01', 1, 249.534, 'USD', None, 'AAPL', 5.0, 'USD' ],
            ]
        for r in recs:
            xfrs.addRecord(self.dbh, r)

        self.assertEqual(self.getTableRecordsCount('xfrs'), 2)
        self.assertEqual(self.getTableRecordsCount('quotes'), 0)

        # test getting the currency
        curr = xfrs.getStockCurrency(self.dbh, 'AAPL')
        self.assertEqual(curr, 'USD')


    def test_getStockCurrency_valid_eurXfers(self):
        """Tests getting a currency of a stock symbol when correct transaction records in DB."""

        # add records
        recs = [
            [ 'buy',  '2024-01-01', 1, 172.681, 'EUR', None, 'AAPL', 6.0, 'EUR' ],
            [ 'sell', '2025-01-01', 1, 239.795, 'EUR', None, 'AAPL', 3.0, 'EUR' ],
            ]
        for r in recs:
            xfrs.addRecord(self.dbh, r)

        self.assertEqual(self.getTableRecordsCount('xfrs'), 2)
        self.assertEqual(self.getTableRecordsCount('quotes'), 0)

        # test getting the currency
        curr = xfrs.getStockCurrency(self.dbh, 'AAPL')
        self.assertEqual(curr, 'EUR')


    def test_getStockCurrency_invalid_xfers(self):
        """Tests getting a currency of a stock symbol when transaction records
        in DB with mixed currencies."""

        # add records
        recs = [
            [ 'buy', '2024-01-01', 1, 172.681, 'EUR', None, 'AAPL', 6.0, 'EUR' ],
            [ 'buy', '2025-01-01', 1, 249.534, 'USD', None, 'AAPL', 5.0, 'USD' ],
            ]
        for r in recs:
            xfrs.addRecord(self.dbh, r)

        self.assertEqual(self.getTableRecordsCount('xfrs'), 2)
        self.assertEqual(self.getTableRecordsCount('quotes'), 0)

        # test getting the currency
        curr = xfrs.getStockCurrency(self.dbh, 'AAPL')
        self.assertEqual(curr, None)


    def test_getStockCurrency_valid_usdQuotes(self):
        """Tests getting a currency of a stock symbol when correct quote records in DB."""

        # add records
        quotes = [
            {
                'date': '2025-01-01',
                'type': 'test1',
                'price': 249.534,
                'currency': 'USD',
                },
            {
                'date': '2024-01-01',
                'type': 'test2',
                'price': 190.914,
                'currency': 'USD',
                },
            ]
        for q in quotes:
            xfrs.cacheQuote(self.dbh, 'AAPL', q)

        self.assertEqual(self.getTableRecordsCount('xfrs'), 0)
        self.assertEqual(self.getTableRecordsCount('quotes'), 2)

        # test getting the currency
        curr = xfrs.getStockCurrency(self.dbh, 'AAPL')
        self.assertEqual(curr, 'USD')


    def test_getStockCurrency_valid_eurQuotes(self):
        """Tests getting a currency of a stock symbol when correct quote records in DB."""

        # add records
        quotes = [
            {
                'date': '2025-01-01',
                'type': 'test1',
                'price': 239.795,
                'currency': 'EUR',
                },
            {
                'date': '2024-01-01',
                'type': 'test2',
                'price': 172.681,
                'currency': 'EUR',
                },
            ]
        for q in quotes:
            xfrs.cacheQuote(self.dbh, 'AAPL', q)

        self.assertEqual(self.getTableRecordsCount('xfrs'), 0)
        self.assertEqual(self.getTableRecordsCount('quotes'), 2)

        # test getting the currency
        curr = xfrs.getStockCurrency(self.dbh, 'AAPL')
        self.assertEqual(curr, 'EUR')


    def test_getStockCurrency_valid(self):
        """Tests getting a currency of a stock symbol when correct records in DB."""

        # add transaction records
        recs = [
            [ 'buy',  '2024-01-01', 1, 172.681, 'EUR', None, 'AAPL', 6.0, 'EUR' ],
            [ 'sell', '2025-01-01', 1, 239.795, 'EUR', None, 'AAPL', 3.0, 'EUR' ],
            ]
        for r in recs:
            xfrs.addRecord(self.dbh, r)

        # add quote records
        quotes = [
            {
                'date': '2025-01-01',
                'type': 'test1',
                'price': 239.795,
                'currency': 'EUR',
                },
            {
                'date': '2024-01-01',
                'type': 'test2',
                'price': 172.681,
                'currency': 'EUR',
                },
            ]
        for q in quotes:
            xfrs.cacheQuote(self.dbh, 'AAPL', q)

        self.assertEqual(self.getTableRecordsCount('xfrs'), 2)
        self.assertEqual(self.getTableRecordsCount('quotes'), 2)

        # test getting the currency
        curr = xfrs.getStockCurrency(self.dbh, 'AAPL')
        self.assertEqual(curr, 'EUR')


    def test_getStockCurrency_emptyDb(self):
        """Tests getting a currency of a stock symbol when no records at all in DB."""

        self.assertEqual(self.getTableRecordsCount('xfrs'), 0)
        self.assertEqual(self.getTableRecordsCount('quotes'), 0)

        # test getting the currency
        curr = xfrs.getStockCurrency(self.dbh, 'AAPL')
        self.assertEqual(curr, None)


    def test_getStockCurrency_notInDb(self):
        """Tests getting a currency of a stock symbol when no records of it in DB."""

        # add transaction records
        recs = [
            [ 'buy',  '2024-01-01', 1, 172.681, 'EUR', None, 'AAPL', 6.0, 'EUR' ],
            [ 'sell', '2025-01-01', 1, 239.795, 'EUR', None, 'AAPL', 3.0, 'EUR' ],
            ]
        for r in recs:
            xfrs.addRecord(self.dbh, r)

        # add quote records
        quotes = [
            {
                'date': '2025-01-01',
                'type': 'test1',
                'price': 239.795,
                'currency': 'EUR',
                },
            {
                'date': '2024-01-01',
                'type': 'test2',
                'price': 172.681,
                'currency': 'EUR',
                },
            ]
        for q in quotes:
            xfrs.cacheQuote(self.dbh, 'AAPL', q)

        self.assertEqual(self.getTableRecordsCount('xfrs'), 2)
        self.assertEqual(self.getTableRecordsCount('quotes'), 2)

        # test getting the currency
        curr = xfrs.getStockCurrency(self.dbh, 'MSFT')
        self.assertEqual(curr, None)


    def test_getStockCurrency_invalid(self):
        """Tests getting a currency of a stock symbol when mixed currency records in DB."""

        # add transaction records
        recs = [
            [ 'buy', '2025-01-01', 1, 249.534, 'USD', None, 'AAPL', 5.0, 'USD' ],
            ]
        for r in recs:
            xfrs.addRecord(self.dbh, r)

        # add quote records
        quotes = [
            {
                'date': '2024-01-01',
                'type': 'test2',
                'price': 172.681,
                'currency': 'EUR',
                },
            ]
        for q in quotes:
            xfrs.cacheQuote(self.dbh, 'AAPL', q)

        self.assertEqual(self.getTableRecordsCount('xfrs'), 1)
        self.assertEqual(self.getTableRecordsCount('quotes'), 1)

        # test getting the currency
        curr = xfrs.getStockCurrency(self.dbh, 'AAPL')
        self.assertEqual(curr, None)



if __name__ == '__main__':
    unittest.main()
