''' Functions for Google News data fetching. '''

import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))


import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
from functools import lru_cache


class GoogleNewsRSS:
    ''' Scraper for Google News RSS. '''
    def __init__(self, rss_url):
        self.response = requests.get(rss_url, headers={'User-Agent': 'Mozilla/5.0'})
        try:
            self.soup = BeautifulSoup(self.response.text, 'xml')
        except Exception as e:
            print('Could not parse XML:', e)
            self.soup = None

        if self.soup:
            self.articles = self.soup.find_all('item')
            self.articles_dicts = [{
                'title': a.title.text,
                'link': a.link.text,
                'description': a.description.text if a.description else '',
                'pubdate': a.pubDate.text if a.pubDate else ''
            } for a in self.articles]
        else:
            self.articles_dicts = []


@lru_cache
def convert_time(time: str):
    ''' Convert Google News date string to datetime. '''
    try:
        return datetime.strptime(time, '%a, %d %b %Y %H:%M:%S %Z')
    except Exception:
        return None


def get_data(coin: str = 'BTC'):
    ''' Fetch latest Google News RSS data for BTC or ETH. '''

    if coin == 'BTC':
        query = 'bitcoin+CoinDesk+OR+Cointelegraph'
    elif coin == 'ETH':
        query = 'ethereum+CoinDesk+OR+Cointelegraph'
    else:
        raise ValueError(f'Coin not supported: {coin}')

    url = f'https://news.google.com/rss/search?q={query}&hl=en-US&gl=US&ceid=US:en'
    request = GoogleNewsRSS(url)

    if not request.articles_dicts:
        print("⚠️ No articles found — check query or network.")
        return pd.DataFrame()

    df = pd.DataFrame(request.articles_dicts)
    df['datetime'] = df['pubdate'].apply(convert_time)
    df['timestamp'] = df['datetime'].apply(lambda x: x.timestamp() if x else None)
    df = df.dropna(subset=['timestamp'])
    df = df.set_index('timestamp').sort_index()

    return df[['title', 'link', 'description', 'datetime']]