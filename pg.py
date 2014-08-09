"""Module for storing coinmarketcap data in the database."""
import codecs
import coinmarketcap
from datetime import datetime
from decimal import Decimal
import os
import psycopg2 as pg2
import psycopg2.extras as pg2ext
import random
import unittest

# Configuration variables
batchLimit = 1000
tables = {
    "currency": "currency",
    "currency_historical": "currency_historical",
    "market_cap_365": "market_cap_365",
    "market_cap_180": "market_cap_180",
    "market_cap_90": "market_cap_90",
    "market_cap_30": "market_cap_30",
    "market_cap_7": "market_cap_7",
    "trade_volume_usd": "trade_volume_usd"
}

# Pull in postgres configuration information
# Pull in postgres configuration information
dbcFile = open('.pgpass', 'r')
dbcRaw = dbcFile.readline().strip().split(':')
dbcParams = {
    'database': dbcRaw[2],
    'user': dbcRaw[3],
    'password': dbcRaw[4],
    'host': dbcRaw[0],
    'port': dbcRaw[1]
}
dbcFile.close()

# Connection variable
conn = None


def connect():
    """Connect to the database."""
    global conn
    if conn is not None:
        return conn
    else:
        conn = pg2.connect(**dbcParams)
        return conn


def cursor():
    """"Pull a cursor from the connection."""
    return connect().cursor()


def dictCursor():
    """"Pull a dictionary cursor from the connection."""
    return connect().cursor(cursor_factory=pg2ext.RealDictCursor)


def _createStaging(tableName, cursor):
    """Create staging table."""
    stagingTable = "{0}_{1}".format(
        tableName, str(int(pow(10, random.random()*10))).zfill(10))
    cursor.execute("""CREATE TABLE {0} (LIKE {1}
        INCLUDING DEFAULTS)""".format(stagingTable, tableName))
    return stagingTable


def _dropStaging(tableName, cursor):
    """Drop staging table."""
    cursor.execute("""
        DROP TABLE {0}""".format(tableName))


def insertCurrencyList(data, withHistory=True):
    """Insert parsed currency list."""
    cursor = dictCursor()
    targetTable = tables['currency']

    # Create staging table
    stagingTable = _createStaging(targetTable, cursor)
    cursor.execute("""ALTER TABLE {0}
        DROP COLUMN id""".format(stagingTable))

    # Move data into staging table
    cursor.executemany("""
        INSERT INTO {0} (
            name, symbol, slug, explorer_link)
        VALUES (
            %(name)s,
            %(symbol)s,
            %(slug)s,
            %(explorer_link)s
        )""".format(stagingTable), data)

    # Update any altered currencies
    cursor.execute("""
        UPDATE {0} tgt
        SET name = stg.name, symbol = stg.symbol,
            explorer_link = stg.explorer_link,
            db_update_time = stg.db_update_time
        FROM {1} stg
        WHERE tgt.slug = stg.slug
        AND (tgt.name <> stg.name OR
            tgt.symbol <> stg.symbol OR
            tgt.explorer_link <> stg.explorer_link)""".format(
        targetTable, stagingTable))

    # Merge any new currencies into target table
    cursor.execute("""
        INSERT INTO {0} (
            name, symbol, slug, explorer_link, db_update_time)
        (SELECT stg.*
        FROM {1} stg
        LEFT JOIN {0} tgt ON tgt.slug = stg.slug
        WHERE tgt.name IS NULL)""".format(
        targetTable, stagingTable))

    # If requested, merge data into the historical table
    if withHistory:
        historicalTable = tables['currency_historical']
        cursor.execute("""
            INSERT INTO {0} (
                name, symbol, slug, explorer_link, db_update_time)
            (SELECT stg.*
            FROM {1} stg
            LEFT JOIN {0} tgt ON
                tgt.name = stg.name AND
                tgt.symbol = stg.symbol AND
                tgt.slug = stg.slug AND
                tgt.explorer_link = stg.explorer_link
            WHERE tgt.name IS NULL)""".format(
            historicalTable, stagingTable))

    # Drop staging table
    _dropStaging(stagingTable, cursor)

    # Commit
    cursor.execute("""COMMIT""")


def _insertMarketCap(data, targetTable):
    """Insert market cap data (private)."""
    cursor = dictCursor()
    if len(data) == 0:
        return True
    fields = data[0].keys()

    # Create staging table
    stagingTable = _createStaging(targetTable, cursor)

    # Move data into staging table
    batchCount = 0
    while batchCount*batchLimit < len(data):
        cursor.executemany("""INSERT INTO {0} ({1}) VALUES ({2})""".format(
            stagingTable,
            ",".join(fields),
            ",".join(["%({0})s".format(field) for field in fields])
            ), data[(batchCount*batchLimit):((batchCount+1)*batchLimit)])
        batchCount += 1

    # Delete out rows with content similar to what we are about to insert
    cursor.execute("""
        DELETE FROM {0} as tgt
        USING {1} as stg
        WHERE tgt.currency = stg.currency
        AND tgt.time = stg.time""".format(targetTable, stagingTable))

    # Insert the new data into the target table
    cursor.execute("""
        INSERT INTO {0}
        (SELECT *
        FROM {1})""".format(targetTable, stagingTable))

    # Drop staging table
    _dropStaging(stagingTable, cursor)

    # Commit
    cursor.execute("""COMMIT""")

    # Return
    return True


def insertMarketCap(data, lookbackDays):
    """Insert the non-volume market cap data."""
    return _insertMarketCap(
        data, tables["market_cap_{0}".format(lookbackDays)])


def insertMarketCapVolume(data):
    """Insert the volume market cap data."""
    return _insertMarketCap(
        data, tables["trade_volume_usd"])


def selectCurrencyId(slug):
    """Select the ID associated with the passed slug."""
    cur = cursor()
    cur.execute("""SELECT id FROM {0} WHERE slug = '{1}'""".format(
        tables['currency'], slug))
    rows = cur.fetchall()
    if len(rows) == 0:
        raise Exception(
            "Couldn't find any currency ID matching slug '{0}'").format(
            slug)
    elif len(rows) > 1:
        raise Exception(
            "DB Error. Found >1 currency IDs for slug '{0}'").format(
            slug)
    else:
        return rows[0][0]


class PgTest(unittest.TestCase):

    """Testing suite for pg module."""

    def setUp(self):
        """Setup tables for test."""
        # Swap and sub configuration variables
        global tables
        self.tablesOriginal = tables
        tables = {}
        for key, table in self.tablesOriginal.iteritems():
            tables[key] = "{0}_test".format(table)
        global batchLimit
        self.batchLimitOriginal = batchLimit
        batchLimit = 20

        # Create test tables
        cur = cursor()
        for key, table in tables.iteritems():
            cur.execute("""CREATE TABLE IF NOT EXISTS
                {0} (LIKE {1} INCLUDING ALL)""".format(
                table, self.tablesOriginal[key]))
        cur.execute("""CREATE SEQUENCE {0}_id_seq""".format(
            tables['currency']))
        cur.execute("""ALTER TABLE {0}
            ALTER COLUMN id DROP DEFAULT""".format(
            tables['currency']))
        cur.execute("""ALTER TABLE {0}
            ALTER COLUMN id SET DEFAULT
            nextval('{0}_id_seq'::regclass)""".format(
            tables['currency']))
        cur.execute("""COMMIT""")

    def tearDown(self):
        """Teardown test tables."""
        # Drop test tables
        global tables
        cur = cursor()
        for table in tables.values():
            cur.execute("""DROP TABLE IF EXISTS
                {0}""".format(table))
        cur.execute("""DROP SEQUENCE {0}_id_seq""".format(
            tables['currency']))
        cur.execute("""COMMIT""")

        # Undo swap / sub
        tables = self.tablesOriginal
        global batchLimit
        batchLimit = self.batchLimitOriginal

    def testInsertCurrencyList(self):
        """Test loadCurrencyList function."""
        fileString = "{0}/example/currencylist.html"
        f = codecs.open(fileString.format(
            os.path.dirname(os.path.abspath(__file__))), 'r', 'utf-8')
        html = f.read()
        f.close()
        data = coinmarketcap.parseCurrencyListAll(html)
        insertCurrencyList(data)

        # Test out some basic count statistics
        cur = dictCursor()
        cur.execute("""SELECT COUNT(*) cnt FROM {0}""".format(
            tables['currency']))
        row = cur.fetchone()
        self.assertEqual(row['cnt'], 452)
        cur.execute("""SELECT COUNT(*) cnt FROM {0}""".format(
            tables['currency_historical']))
        row = cur.fetchone()
        self.assertEqual(row['cnt'], 452)

        # Test out contents of first and last row
        expectedFirst = {
            'name': 'Bitcoin',
            'slug': 'bitcoin',
            'symbol': 'BTC',
            'explorer_link': 'http://blockchain.info'
        }
        cur.execute("""SELECT name, symbol, slug, explorer_link
            FROM {0}
            ORDER BY id
            ASC LIMIT 1""".format(
            tables['currency']))
        datumFirst = cur.fetchone()
        self.assertEqual(datumFirst, expectedFirst)
        expectedLast = {
            'name': 'Marscoin',
            'slug': 'marscoin',
            'symbol': 'MRS',
            'explorer_link': 'http://explore.marscoin.org/chain/Marscoin/'
        }
        cur.execute("""SELECT name, symbol, slug, explorer_link
            FROM {0}
            ORDER BY id
            DESC LIMIT 1""".format(
            tables['currency']))
        datumLast = cur.fetchone()
        self.assertEqual(datumLast, expectedLast)

        # Update the data in a way that modifies what's in the DB
        updatedDatum = {
            'name': 'XXBitCoinXXX',
            'slug': 'bitcoin',
            'symbol': 'BTC',
            'explorer_link': 'http://blockchain.info'
        }
        insertCurrencyList([updatedDatum])
        cur.execute("""SELECT COUNT(*) cnt FROM {0}""".format(
            tables['currency']))
        row = cur.fetchone()
        self.assertEqual(row['cnt'], 452)
        cur.execute("""SELECT COUNT(*) cnt FROM {0}""".format(
            tables['currency_historical']))
        row = cur.fetchone()
        self.assertEqual(row['cnt'], 453)
        cur.execute("""SELECT name, symbol, slug, explorer_link
            FROM {0}
            ORDER BY id
            ASC LIMIT 1""".format(
            tables['currency']))
        newDatumFirst = cur.fetchone()
        self.assertEqual(newDatumFirst, updatedDatum)

    def testSelectCurrencyId(self):
        """Test selectCurrencyId function."""
        datum = {
            'name': 'XXBitCoinXXX',
            'slug': 'bitcoin',
            'symbol': 'BTC',
            'explorer_link': 'http://blockchain.info'
        }
        insertCurrencyList([datum], withHistory=False)
        self.assertEqual(selectCurrencyId('bitcoin'), 1)

    def testInsertMarketCap(self):
        """Test insertMarketCap and insertMarketCapVolume functions."""
        f = open("{0}/example/marketcap_navajo_7d.json".format(
            os.path.dirname(os.path.abspath(__file__))), 'r')
        jsonDump = f.read()
        f.close()
        data, volData = coinmarketcap.parseMarketCap(
            jsonDump, 9, includeVolume=True)
        insertMarketCap(data, 7)

        # Basic count
        cur = dictCursor()
        cur.execute("""SELECT COUNT(*) cnt FROM {0}""".format(
            tables['market_cap_7']))
        row = cur.fetchone()
        self.assertEqual(row['cnt'], 287)

        # Integrity of first and last rows
        cur.execute("""SELECT currency, time, market_cap_by_available_supply,
                market_cap_by_total_supply, price_usd, price_btc,
                est_available_supply, est_total_supply
            FROM {0}
            ORDER BY currency, time
            ASC LIMIT 1""".format(
            tables['market_cap_7']))
        datumFirst = cur.fetchone()
        expectedFirst = {
            'currency': 9,
            'time': datetime.utcfromtimestamp(1406855058),
            'market_cap_by_available_supply': Decimal('196545.14489832715'),
            'market_cap_by_total_supply': Decimal('196545.14489832715'),
            'price_usd': Decimal('0.00344855'),
            'price_btc': Decimal('.00000588286'),
            'est_available_supply': Decimal('56993561.0324128'),
            'est_total_supply': Decimal('56993561.0324128')
        }
        self.assertEqual(datumFirst, expectedFirst)
        cur.execute("""SELECT currency, time, market_cap_by_available_supply,
                market_cap_by_total_supply, price_usd, price_btc,
                est_available_supply, est_total_supply
            FROM {0}
            ORDER BY currency, time
            DESC LIMIT 1""".format(
            tables['market_cap_7']))
        datumLast = cur.fetchone()
        expectedLast = {
            'currency': 9,
            'time': datetime.utcfromtimestamp(1407458053),
            'market_cap_by_available_supply': Decimal('124991.3258020573'),
            'market_cap_by_total_supply': Decimal('124991.3258020573'),
            'price_usd': Decimal('0.00219195'),
            'price_btc': Decimal('.00000372172'),
            'est_available_supply': Decimal('57022890.942794'),
            'est_total_supply': Decimal('57022890.942794')
        }
        self.assertEqual(datumLast, expectedLast)

        # Volume Data
        insertMarketCapVolume(volData)
        cur.execute("""SELECT COUNT(*) cnt FROM {0}""".format(
            tables['trade_volume_usd']))
        row = cur.fetchone()
        self.assertEqual(row['cnt'], 7)
        cur.execute("""SELECT currency, time, volume
            FROM {0}
            ORDER BY currency, time
            ASC LIMIT 1""".format(
            tables['trade_volume_usd']))
        datumVolFirst = cur.fetchone()
        expectedVolFirst = {
            'currency': 9,
            'time': datetime.utcfromtimestamp(1406855058),
            'volume': Decimal('2447.37')
        }
        self.assertEqual(datumVolFirst, expectedVolFirst)
        cur.execute("""SELECT currency, time, volume
            FROM {0}
            ORDER BY currency, time
            DESC LIMIT 1""".format(
            tables['trade_volume_usd']))
        datumVolLast = cur.fetchone()
        expectedVolLast = {
            'currency': 9,
            'time': datetime.utcfromtimestamp(1407375855),
            'volume': Decimal('477.609')
        }
        self.assertEqual(datumVolLast, expectedVolLast)

if __name__ == "__main__":
    unittest.main()
