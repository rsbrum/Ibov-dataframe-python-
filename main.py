from bs4 import BeautifulSoup
from selenium import webdriver
from matplotlib import style
import pandas, urllib, requests, time, csv, logging, os
import matplotlib.pyplot as plt

style.use('ggplot')

logger = logging.getLogger('root')
formatter = logging.Formatter('%(asctime)s : %(levelname)s : %(message)s')
sh = logging.StreamHandler()
sh.setLevel(logging.INFO)
sh.setFormatter(formatter)
logger.addHandler(sh) 
logger.setLevel(logging.INFO)
"""
    Create dataframe of the bovespa indice 
"""
def import_ibov_tickers():
    """
        Gets current integrants of the indice 
    """
    logger.info('Downloading tickers...')
    url = "http://bvmf.bmfbovespa.com.br/indices/ResumoCarteiraTeorica.aspx?Indice=IBOV&idioma=en-us"
    with urllib.request.urlopen(url) as page:
        soup = BeautifulSoup(page.read(), 'html.parser').find_all(
            'td', attrs={'class': 'rgSorted'})

    tickers = []
    for tick in soup:
        span = tick.find('span')
        tickers.append(span.text)

    return tickers


def download_historic_data():
    """ 
        Scrapes the data for each ticker
    """
    logger.info("Downloading historic data...")
    tickers = import_ibov_tickers()
    dir_path = os.path.dirname(os.path.realpath(__file__)) + "\data"

    profile = webdriver.FirefoxProfile()
    profile.set_preference('browser.download.folderList', 2)  # custom location
    profile.set_preference('browser.download.manager.showWhenStarting', True)
    profile.set_preference('browser.download.dir', dir_path)
    profile.set_preference(
        'browser.helperApps.neverAsk.saveToDisk', 'text/csv')

    for tick in tickers:
        logger.info('Downloading historic data of: {}'.format(tick))
        try:
            #if os.path.isfile('./data/{}'.format(tick + '.SA.csv')):
            #    raise Exception('File already exists')

            driver = webdriver.Firefox(
                profile, executable_path='./geckodriver.exe')
                #period1=946864800&period2=1553828400&interval=1d&filter=history&frequency=1d
            url = "https://finance.yahoo.com/quote/{}/history?period1=1522528454&period2=1554064454&interval=1d&filter=history&frequency=1d".format(
                tick + ".SA")
            driver.get(url)
            driver.find_element_by_xpath(
                "//a[@class='Fl(end) Mt(3px) Cur(p)']").click()
            driver.close()

        except:
            logger.warning('File already exists!')


def import_dates(combination):
    """
        Imports the range of dates into one collum 
    """
    logger.info('Importing dates...')
    companies = import_ibov_tickers()
    file = './data/{}.SA.csv'.format(companies[0])
    csv_file = open(file)
    csv_reader = csv.reader(csv_file, delimiter=',')
    line = 0

    for row in csv_reader:
        if line != 0:
            combination['dates'].append(row[0])
        line += 1            


def import_values(combination, companies):
    """
        Creates one column for each company,
        using the close price of the day
    """
    logger.info('Importing close prices...')
    for company in companies:
        logger.info('Importing close prices of: {}'.format(company))
        file = './data/{}.SA.csv'.format(company)
        line = 0
        values = []

        try:
            csv_file = open(file)
            csv_reader = csv.reader(csv_file, delimiter=',')

            for row in csv_reader:
                if line != 0:
                    values.append(row[5])
                line += 1

            combination['values'].append(values)
            combination['indexes'].append(company)
        except:
            logger.warning("Couldn't append {} info".format(company))

def create_ibov_ds():
    """
        Creates the ibov data frame 
    """
    logger.info('Creating the dataframe...')
    combination = {
        'indexes': [],
        'values': [],
        'dates': []
    }

    companies = import_ibov_tickers()
    import_dates(combination)
    import_values(combination, companies)

    first_row = []
    first_row.append('Dates')

    for index in combination['indexes']:
        first_row.append(index)

    with open('./ibov.csv', mode='w', newline='') as ibov:
        writer = csv.writer(ibov, delimiter=',',
                            quotechar='"', quoting=csv.QUOTE_MINIMAL)
        writer.writerow(first_row)
        
        #range in number of rows of the first column 
        for x in range(0, len(combination['values'][0])):
            row = []
            row.append(combination['dates'][x])
            for value in combination['values']:
                try:
                    row.append(value[x])
                except:
                    row.append('NaN')

            writer.writerow(row)


def process_percent_change(ticker, days):
    df = pandas.read_csv('./ibov.csv')
    cols = [c for c in df[ticker]]
    count = 0
    result = []
    differences = []

    for x in range(0, days):
        for price in cols:
            try:
                if count == 4:
                    break
                difference = ((price - cols[x]) / cols[x] ) * 100
                differences.append(difference)     
                count += 1                                                                                                  
                result.append(differences)
            except:
                logger.warning()
                differences = []
                count = 0

    return result 

def missing_downloaded_files():
    logger.info("Checking for missing files...")
    tickers = import_ibov_tickers()
    dir_tickers = os.listdir('./data')
    missing_tickers = []
    ext = '.SA.csv'
    is_equal = False 

    for ticker in tickers:
        is_equal = False 
        ticker += ext

        for dir_ticker in dir_tickers:
            if dir_ticker == ticker:
                is_equal = True
                break
        
        if is_equal == False:
            missing_tickers.append(ticker)

    return missing_tickers

download_historic_data()

missing_files = missing_downloaded_files()

if len(missing_downloaded_files) > 0:
    for file in missing_files:
        logger.warning("{} historic data is missing!".format(file))

create_ibov_ds()

