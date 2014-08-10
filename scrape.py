""" Core scraper for coinmarketcap.com. """
import codecs
import coinmarketcap
from datetime import datetime
import logging
import os
import pg
import sys
import traceback

# Configuration
lookbacks = [365, 180, 90, 30, 7]
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s:%(message)s',
    datefmt='%m/%d/%Y %I:%M:%S %p')


def _saveToFile(content, prefix, extension):
    """Save given entity to a file."""
    f = codecs.open("{0}/data/{1}_{2}.{3}".format(
        os.path.dirname(os.path.abspath(__file__)),
        prefix,
        int((datetime.utcnow() - datetime(1970, 1, 1)).total_seconds()),
        extension),
        'w', 'utf-8')
    f.write(content)
    f.close()


def scrapeCurrencyList():
    """Scrape currency list."""
    html = coinmarketcap.requestCurrencyList('all')
    _saveToFile(html, 'currencylist', 'html')
    data = coinmarketcap.parseCurrencyListAll(html)
    pg.insertCurrencyList(data, withHistory=True)
    return data


def scrapeMarketCap(slug, numDays, includeVolume=False):
    """Scrape market cap for the specified currency slug."""
    jsonDump = coinmarketcap.requestMarketCap(slug, numDays)
    _saveToFile(
        jsonDump,
        'marketcap_{0}_{1}d'.format(slug, numDays),
        'json')
    result = coinmarketcap.parseMarketCap(
        jsonDump,
        pg.selectCurrencyId(slug),
        includeVolume=includeVolume)
    if includeVolume:
        data, volData = result
        pg.insertMarketCapVolume(volData)
    else:
        data = result
    pg.insertMarketCap(data, numDays)


logging.info("Attempting to scrape currency list...")
currencies = scrapeCurrencyList()
logging.info("Finished scraping currency list. Starting on currencies...")
for currency in currencies:
    logging.info(">Starting scrape of currency {0}...".format(
        currency['slug']))
    for lookback in lookbacks:
        includeVolume = True if lookback == 365 else False
        logging.info(">>Starting scrape of lookback {0}...".format(
            lookback))
        try:
            scrapeMarketCap(
                currency['slug'], lookback, includeVolume=includeVolume)
        except Exception as e:
            print '-'*60
            print "Could not scrape currency {0}, lookback {1}.".format(
                currency['slug'], lookback)
            print traceback.format_exc()
            print '-'*60
            logging.info(
                ">>Could not scrape lookback {0}. Skipping.".format(
                    lookback))
            continue
        logging.info(">>Done with scrape of lookback {0}.".format(
            lookback))
    logging.info(">Done with scrape of currency {0}.".format(
        currency['slug']))
logging.info("Finished scraping currencies. All done.")
logging.info("Made {0} requests in total.".format(
    coinmarketcap.countRequested))
