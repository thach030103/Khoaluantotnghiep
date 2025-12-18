''' Script to fetch all historic reddit posts from several subreddits through the Pushshift API. '''

import pytz
from datetime import datetime
import os
from functions import get_historic_data


if __name__ == '__main__':
    save_dir = r'C:\Users\Admin\Downloads\crypto-price-forecasting-public-main\crypto-price-forecasting-public-main\1_data_acquisition\reddit'
    # before = int(datetime.now().replace(tzinfo=pytz.UTC).timestamp())
    before = int(datetime(2025, 11, 20, tzinfo=pytz.UTC).timestamp())

    #fetch r/Bitcoin data since 2012
    after = int(datetime(2012, 1, 1, tzinfo=pytz.UTC).timestamp())
    reddit_r_bitcoin = get_historic_data('Bitcoin', after, before)

    reddit_r_bitcoin_path = os.path.join(save_dir, 'reddit_r_bitcoin.parquet.gzip')
    reddit_r_bitcoin.to_parquet(reddit_r_bitcoin_path, compression='gzip')
    
    # fetch r/ethereum data since 2014
    after = int(datetime(2014, 2, 1, tzinfo=pytz.UTC).timestamp())
    reddit_r_ethereum = get_historic_data('ethereum', after, before)
    reddit_r_ethereum_path = os.path.join(save_dir, 'reddit_r_ethereum.parquet.gzip')
    reddit_r_ethereum.to_parquet(reddit_r_ethereum_path, compression='gzip')
    
   