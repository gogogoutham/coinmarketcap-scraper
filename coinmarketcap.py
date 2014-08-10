""" Module for requesting data from coinmarketcap.org and parsing it. """
import codecs
from datetime import datetime
from datetime import time
from decimal import Decimal
import json
import logging
import lxml.html
import requests
import os
from random import random
import sys
import time
import unittest

baseUrl = "http://coinmarketcap.com"
countRequested = 0
interReqTime = 1
lastReqTime = None


def _request(payloadString):
    """Private method for requesting an arbitrary query string."""
    global countRequested
    global lastReqTime
    if lastReqTime is not None and time.time() - lastReqTime < interReqTime:
        timeToSleep = random()*(interReqTime-time.time()+lastReqTime)*2
        logging.info("Sleeping for {0} seconds before request.".format(
            timeToSleep))
        time.sleep(timeToSleep)
    logging.info("Issuing request for the following payload: {0}".format(
        payloadString))
    r = requests.get("{0}/{1}".format(baseUrl, payloadString))
    lastReqTime = time.time()
    countRequested += 1
    if r.status_code == requests.codes.ok:
        return r.text
    else:
        raise Exception("Could not process request. \
            Received status code {0}.".format(r.status_code))


def requestCurrencyList(view):
    """Request a currency list."""
    """CAVEAT: Parse is currently built for only the 'all' view."""
    return _request("currencies/views/{0}/".format(view))


def requestCurrency(currencySlug):
    """Request the page for a specific currency."""
    """CAVEAT: There is currently no corresponding parser for this data."""
    return _request("currencies/{0}/".format(currencySlug))


def requestMarketCap(currencySlug, numDays):
    """Request market cap data for a given currency slug."""
    return _request(
        "static/generated_pages/currencies/datapoints/{0}-{1}d.json".format(
            currencySlug, numDays))


def parseCurrencyListAll(html):
    """Parse the information returned by requestCurrencyList for view 'all'."""
    data = []

    docRoot = lxml.html.fromstring(html)
    currencyRows = docRoot.cssselect(
        "table#currencies-all > tbody > tr")

    for currencyRow in currencyRows:
        datum = {}
        currencyFields = currencyRow.cssselect("td")

        # Name and slug
        nameField = currencyFields[1].cssselect("a")[0]
        datum['name'] = nameField.text_content().strip()
        datum['slug'] = nameField.attrib['href'].replace(
            '/currencies/', '').replace('/', '').strip()

        # Symbol
        datum['symbol'] = currencyFields[2].text_content().strip()

        # Explorer link
        supplyFieldPossible = currencyFields[5].cssselect("a")
        if len(supplyFieldPossible) > 0:
            datum['explorer_link'] = supplyFieldPossible[0].attrib['href']
        else:
            datum['explorer_link'] = ''

        data.append(datum)

    return data


def parseMarketCap(jsonDump, currency, includeVolume=False):
    """Parse the supply and price information returned by requestMarketCap."""
    data = []
    rawData = json.loads(jsonDump)

    # Covert data in document to wide format
    dataIntermediate = {}
    targetFields = [str(key.replace('_data', '')) for key in rawData.keys()]
    targetFields.remove('x_min')
    targetFields.remove('x_max')
    targetFields.remove('volume')
    for field, fieldData in rawData.iteritems():
        if field == 'x_min' or field == 'x_max' or field == 'volume_data':
            continue
        targetField = str(field.replace('_data', ''))
        for row in fieldData:
            time = int(row[0]/1000)
            if time not in dataIntermediate:
                dataIntermediate[time] = dict(zip(
                    targetFields, [None]*len(targetFields)))
            dataIntermediate[time][targetField] = row[1]

    # Generate derived data & alter format
    times = sorted(dataIntermediate.keys())
    for time in times:
        datum = dataIntermediate[time]
        datum['currency'] = currency
        datum['time'] = datetime.utcfromtimestamp(time)

        if (datum['market_cap_by_available_supply'] is not None and
                datum['price_usd'] is not None):
            datum['est_available_supply'] = float(
                datum['market_cap_by_available_supply'] / datum['price_usd'])
        else:
            datum['est_available_supply'] = None

        if (datum['market_cap_by_total_supply'] is not None and
                datum['price_usd'] is not None):
            datum['est_total_supply'] = float(
                datum['market_cap_by_total_supply'] / datum['price_usd'])
        else:
            datum['est_available_supply'] = None

        data.append(datum)

    # Section for handling volume data if specified (has different time scale!)
    if not includeVolume:
        return data
    else:
        volData = []
        volDataRaw = sorted(
            rawData['volume_data'], key=lambda x: x[0])
        for vdr in volDataRaw:
            datum = {}
            datum['currency'] = currency
            datum['time'] = datetime.utcfromtimestamp(int(vdr[0]/1000))
            datum['volume'] = vdr[1]
            volData.append(datum)
        return data, volData


class CoinmarketcapTest(unittest.TestCase):

    """"Testing suite for coinmarketcap module."""

    def testRequestCurrencyList(self):
        """Test requestCurrencyList."""
        html = requestCurrencyList("all")
        f = codecs.open("{0}/data/test_currencylist.html".format(
            os.path.dirname(os.path.abspath(__file__))), 'w', 'utf-8')
        f.write(html)
        f.close()
        docRoot = lxml.html.fromstring(html)
        currencyRows = docRoot.cssselect(
            "table#currencies-all > tbody > tr")
        self.assertEqual(len(currencyRows) > 101, True)

    def testRequestCurrency(self):
        """Test requestCurrency."""
        html = requestCurrency("navajo")
        f = codecs.open("{0}/data/test_currency_navajo.html".format(
            os.path.dirname(os.path.abspath(__file__))), 'w', 'utf-8')
        f.write(html)
        f.close()
        docRoot = lxml.html.fromstring(html)
        currency = docRoot.cssselect(
            "div.container > div > div > h1.text-large"
            )[0].text_content().strip()
        self.assertEqual(currency, "Navajo (NAV)")

    def testRequestMarketCap(self):
        """Test requestMarketCap."""
        jsonDump = requestMarketCap("navajo", 7)
        f = open("{0}/data/test_marketcap_navajo_7d.json".format(
            os.path.dirname(os.path.abspath(__file__))), 'w')
        f.write(jsonDump)
        f.close()
        data = json.loads(jsonDump)
        headingsExpected = set([
            "market_cap_by_available_supply_data",
            "market_cap_by_total_supply_data",
            "price_btc_data",
            "price_usd_data",
            "volume_data",
            "x_max",
            "x_min"
        ])
        self.assertEqual(set(data.keys()), headingsExpected)

    def testParseCurrencyListAll(self):
        """Test parseCurrencyListAll."""
        f = codecs.open("{0}/example/currencylist.html".format(
            os.path.dirname(os.path.abspath(__file__))), 'r', 'utf-8')
        html = f.read()
        f.close()
        data = parseCurrencyListAll(html)
        self.assertEqual(len(data), 452)
        expectedFirst = {
            'name': 'Bitcoin',
            'slug': 'bitcoin',
            'symbol': 'BTC',
            'explorer_link': 'http://blockchain.info'
        }
        self.assertEqual(data[0], expectedFirst)
        expectedLast = {
            'name': 'Marscoin',
            'slug': 'marscoin',
            'symbol': 'MRS',
            'explorer_link': 'http://explore.marscoin.org/chain/Marscoin/'
        }
        self.assertEqual(data[-1], expectedLast)

    def testParseMarketCap(self):
        """Test parseMarketCap."""
        f = open("{0}/example/marketcap_navajo_7d.json".format(
            os.path.dirname(os.path.abspath(__file__))), 'r')
        jsonDump = f.read()
        f.close()

        data = parseMarketCap(jsonDump, 'navajo')
        self.assertEqual(len(data), 287)
        self.maxDiff = None
        expectedFirst = {
            'currency': 'navajo',
            'time': datetime.utcfromtimestamp(1406855058),
            'market_cap_by_available_supply': 196545.14489832715,
            'market_cap_by_total_supply': 196545.14489832715,
            'price_usd': 0.00344855,
            'price_btc': .00000588286,
            'est_available_supply': 56993561.0324128,
            'est_total_supply': 56993561.0324128
        }
        self.assertEqual(data[0], expectedFirst)
        expectedLast = {
            'currency': 'navajo',
            'time': datetime.utcfromtimestamp(1407458053),
            'market_cap_by_available_supply': 124991.3258020573,
            'market_cap_by_total_supply': 124991.3258020573,
            'price_usd': 0.00219195,
            'price_btc': .00000372172,
            'est_available_supply': 57022890.942794,
            'est_total_supply': 57022890.942794
        }
        self.assertEqual(data[-1], expectedLast)

        data, volData = parseMarketCap(
            jsonDump, 'navajo', includeVolume=True)
        self.assertEqual(len(volData), 7)
        expectedVolFirst = {
            'currency': 'navajo',
            'time': datetime.utcfromtimestamp(1406855058),
            'volume': 2447.37
        }
        self.assertEqual(volData[0], expectedVolFirst)
        expectedVolLast = {
            'currency': 'navajo',
            'time': datetime.utcfromtimestamp(1407375855),
            'volume': 477.609
        }

if __name__ == "__main__":
    unittest.main()
