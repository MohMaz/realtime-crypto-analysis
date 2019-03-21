import requests
from lxml import etree
import pandas as pd
import os
import datetime
import urllib.parse
import time
import urllib
from PIL import Image
import io
import matplotlib.pyplot as plt


class CoinMarketcap():

    def __init__(self):
        self.root_url = "http://coinmarketcap.com"
        self.dir = os.path.dirname(__file__) + "/data/"
        self.tree = self.read()

    def get(self):
        """
        Requests coinmarketcap.com and saves results to disk.
        """
        #print("Downloading data for {}".format(self.name))
        time.sleep( 5 )
        cmc = requests.get(self.root_url)
        time.sleep( 5 )
        text = cmc.text
        with open(os.path.join(self.dir,'coinmarketcap.html'), 'w+',encoding="utf8") as f:
            f.write(text)
        return text

    def read(self):
        """
        Returns an lxml etree of coinmarketcap.html.
        Uses cached version if exists, otherwise calls get().
        """
        try:
            with open(os.path.join(self.dir,'coinmarketcap.html'),encoding="utf8") as f:
                text = f.read()
        except FileNotFoundError:
            text = self.get()
        tree = etree.HTML(text)
        return tree

    def coin_names(self):
        """ 
        Returns a list the 100 coins from the front page of coinmarketcap.com
        """
        coins = self.tree.findall(".//a[@class='currency-name-container link-secondary']") 
        coin_names = [e.text for e in coins]
        return coin_names

    def coin_urls(self):
        """ 
        Returns a list of 100 URLs. The ith URL is a the relative path for the 
        coinmarketcap page corresponding to the ith coin from coinmarketcap. 
        """
                
        
        coins = self.tree.findall(".//a[@class= 'currency-name-container link-secondary']") 
        coin_urls = [e.attrib['href'] for e in coins]
        return coin_urls

    def coins(self):
        "Returns a list of Coin objects."
        coins = zip(self.coin_names(), self.coin_urls())
        coins = [Coin(name, url) for (name, url) in coins]
        return coins

    def all_coin_data(self, ret_df=True, ret_json=False, github=False):
        """
        Generate a list of coin objects, containing all fields as specified in
        the Coin class.
        """
        all_coin_data = []
        coins = self.coins()
        for coin in coins:
            coin = self.coin(coin)
            coin_data = coin.all_fields()
            all_coin_data.append(coin_data)
        if ret_json:
            return all_coin_data
        if ret_df:
            return pd.DataFrame(all_coin_data)

    def get_price_history(self, coin_name='bitcoin', start='20130428', end=None):
        if end == None:
            end = time.strftime("%Y%m%d")
        bitcoin_market_info = pd.read_html("https://coinmarketcap.com/currencies/{0}/historical-data/?start={1}&end={2}".format(coin_name, start,end))[0]
        # convert the date string to the correct date format
        bitcoin_market_info = bitcoin_market_info.assign(Date=pd.to_datetime(bitcoin_market_info['Date']))
        # when Volume is equal to '-' convert it to 0
        try:
            bitcoin_market_info.loc[bitcoin_market_info['Volume']=="-",'Volume']=0
        except TypeError:
            pass
        # convert to int
        bitcoin_market_info['Volume'] = bitcoin_market_info['Volume'].astype('int64')

        # Generate Volatility Column
        bitcoin_market_info['Volatility'] = (bitcoin_market_info['High']- bitcoin_market_info['Low']) / bitcoin_market_info['Open']

        # Generate day of year and month Column
        bitcoin_market_info['doy'] = pd.to_datetime(bitcoin_market_info['Date']).apply(lambda x: x.dayofyear)

        # Generate Volatility Column
        bitcoin_market_info['month'] = pd.to_datetime(bitcoin_market_info['Date']).apply(lambda x: x.month)

        # Generate Daily Price Change Column
        bitcoin_market_info['day_diff'] = bitcoin_market_info['Close']  / bitcoin_market_info['Open']

        return bitcoin_market_info

    def plot_price(self, market_info, split_date=None, image_url=None, plot_volatility=False):
        image = None
        if image_url:
            image = urllib.request.urlopen(image_url)
            image_file = io.BytesIO(image.read())
            image = Image.open(image_file)
            width_image , height_image  = image.size
            image = image.resize((int(image.size[0]*0.8), int(image.size[1]*0.8)), Image.ANTIALIAS)

        fig, (ax1, ax2) = plt.subplots(2,1, gridspec_kw = {'height_ratios':[3, 1]}, figsize=(12,6))
        ax1.set_ylabel('Closing Price (USD)',fontsize=12)
        ax2.set_ylabel('Volume (Billion USD)',fontsize=12)
        ax2.set_yticklabels(range(10))
        ax1.set_xticks([datetime.date(i,j,1) for i in range(2013,2019) for j in [1,7]])
        ax1.set_xticklabels('')
        ax2.set_xticks([datetime.date(i,j,1) for i in range(2013,2019) for j in [1,7]])
        ax2.set_xticklabels([datetime.date(i,j,1).strftime('%b %Y')  for i in range(2013,2019) for j in [1,7]])
        ax1.plot(market_info[market_info['Date'] < split_date]['Date'].astype(datetime.datetime),market_info[market_info['Date'] < split_date]['Open'], color='#8FBAC8')
        ax1.plot(market_info[market_info['Date'] >= split_date]['Date'].astype(datetime.datetime),market_info[market_info['Date'] >= split_date]['Open'], color='#B08FC7')
        ax2.bar(market_info['Date'].astype(datetime.datetime).values, market_info['Volume'].values)

        #Plot Volatility
        if plot_volatility:
            ax3 = ax1.twinx()
            ax3.plot(market_info['Date'].astype(datetime.datetime),market_info['Volatility'], color='black', linewidth=0.5, alpha=0.5)

        fig.tight_layout()
        if image:
            fig.figimage(image, 100, 30, zorder=3,alpha=.5)
        plt.show()


class Scraper():
    def __init__(self, name, url):
        self.name = name
        self.url = url
        self.dir = os.path.dirname(__file__) + "/data/"
        self.tree = self.read()

    def __str__(self):
        return self.name

    def __repr__(self):
        return str(self.json())

    def get(self):
        """
        Get's the page http://github.com/relative_url
        """
        if self.url == None:
            return None
        print("Downloading data for {}".format(self.name))
        time.sleep( 5 )
        page = requests.get(self.url)
        text = page.text
        with open(os.path.join(self.dir,'{}.html'.format(self.name)), 'w+',encoding="utf8") as f:
            f.write(text)
        return text

    def read(self):
        try:
            with open(os.path.join(self.dir,'{}.html'.format(self.name)),encoding="utf8") as f:
                text = f.read()
        except FileNotFoundError:
            text = self.get()
        if text == None:
            return None
        tree = etree.HTML(text)
        self.tree = tree
        return tree

    def json(self):
        return """Overwride this function!"""


class GitHub(Scraper):
    def __init__(self, name, url, coin):
        super().__init__(name, url)
        self.coin = coin

    def json(self):
        github_data = {
                'name': self.name,
                'url': self.url,
                'coin': self.coin,
                }
        return github_data

    def pinned_repos(self):
        if self.tree == None:
            return None
        pinned_repos = self.tree.findall(".//div[@class='pinned-repo-item-content']") 
        repos = []
        for e in pinned_repos:
                name = e.find(".//span[@class='repo js-repo']").text
                url = urllib.parse.urljoin(self.url, e          \
                        .find(".//span[@class='repo js-repo']") \
                        .getparent()    \
                        .attrib['href'])
                description = e.find(".//p").text
                repo = Repo(name+"-repo", url, description, self.coin)
                repos.append(repo)
        return repos

    def is_repo(self):
        if self.tree == None:
            return None
        t = self.tree.find(".//a[@class='header-search-scope no-underline']")
        if t == None:
            return False
        if 'repository' in t.text:
            return True
        else:
            return False

    def repo(self):
        if self.is_repo():
            return Repo(self.name, self.url, "", self.coin)
        pinned_repos = self.pinned_repos()
        if pinned_repos == None:
            return None
        try:
            return pinned_repos[0]
        except IndexError:
            return None

class Repo(Scraper):
    def __init__(self, name, url, description, coin):
        super().__init__(name, url)
        self.description = description
        self.coin = coin

    def json(self):
        repo_data = {
                'name': self.name,
                'coin': self.coin,
                'url': self.url,
                'description': self.description,
                'language': self.language(),
                'stars': self.stars(),
                'forks': self.forks(),
                'watch': self.watch(),
                }
        return repo_data

    def language(self):
        langs = self.tree.findall(".//span[@class='lang']")
        langs = [l.text for l in langs]
        try:
            return langs[0]
        except:
            return None

    def stars(self):
        stars = self.tree.find(".//a[@class='social-count js-social-count']") 
        stars = stars.text.strip()
        return int(stars.replace(',',''))

    def forks(self):
        links = self.tree.findall(".//a") 
        try:
            forks = [a for a in links if a.attrib['href'].endswith('network')][0]
        except:
            return None
        forks = forks.text.strip()
        return int(forks.replace(',',''))

    def watch(self):
        links = self.tree.findall(".//a") 
        try:
            watch = [a for a in links if a.attrib['href'].endswith('watchers')][0]
        except:
            return None
        watch = watch.text.strip()
        return int(watch.replace(',',''))

class Coin(Scraper):
    def __init__(self, name, relative_url):
        url = "http://coinmarketcap.com" + relative_url
        super().__init__(name, url)

    def __str__(self):
        return self.name

    def __repr__(self):
        return str(self.json())

    def json(self):
        coin_data = {
                'name': self.name,
                'url': self.url,
                'symbol': self.symbol(),
                'price': self.price(),
                'volume': self.volume(),
                'marketcap': self.marketcap(),
                'timestamp': None,
                'github_url': self.github_url(),
                }
        return coin_data

    def symbol(self):
        try:
            symbol = self.tree.find(".//small[@class='bold hidden-xs']") 
            # Strip Parenthesis
            symbol = symbol.text[1:-1]
        except:
            return None
        return symbol

    def website(self):
        links = self.tree.findall(".//a")
        website = [l.attrib['href'] for l in links if l.text == "Website"][0]
        return website

    def github_url(self):
        links = self.tree.findall(".//a")
        try:
            github_url = [l.attrib['href'] for l in links if l.text == "Source Code"][0]
        except IndexError:
            github_url = None
        return github_url

    def forum_url(self):
        links = self.tree.findall(".//a")
        github_url = [l.attrib['href'] for l in links if l.text == "Source Code"][0]
        return github_url

    def github(self):
        return GitHub(name=self.name + "-github", url=self.github_url(), coin=self.name)

    def repo(self):
        github = self.github()
        return github.repo()

    def today(self):
        history = self.read_history()
        today = history.head(1)
        return today

    def price(self):
        today = self.today()
        return today['Open*'][0]

    def volume(self):
        today = self.today()
        return today['Volume'][0]

    def marketcap(self):
        today = self.today()
        return int(today['Market Cap'][0])

    def read_history(self, start=20130428,end=None, cache_days=7):
        if end == None:
            end = datetime.datetime.now().strftime("%Y%m%d")
        filename = os.path.join(self.dir,'{}-history.csv'.format(self.name))
        try:
            # Check for freshness. If more than a day old, re-download
            modification_time = datetime.datetime.fromtimestamp(
                    int(os.path.getmtime(filename))
                    )
            now = datetime.datetime.now()
            delta = now - modification_time

            if delta.days > cache_days:
                df = self.get_history(start, end)
            else:
                df = pd.read_csv(filename)
        except FileNotFoundError:
            df = self.get_history(start, end)
        return df

    def get_history(self, start=None,end=None):
        url = self.url + "/historical-data"
        if start and end:
            url = url + "/?start={0}&end={1}".format(start, end)
        print("Downloading history data for {}".format(self.name))
        df = pd.read_html(url)[0]
        df.to_csv(os.path.join(self.dir,'{}-history.csv'.format(self.name)))
        return df

