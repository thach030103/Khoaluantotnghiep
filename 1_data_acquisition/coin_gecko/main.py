''' Script to fetch historic cryptocurrency data from CoinGecko. '''

from functions import get_data
import os
from datetime import datetime

if __name__ == '__main__':
    save_dir = r'C:\Users\Admin\Downloads\crypto-price-forecasting-public-main\crypto-price-forecasting-public-main\1_data_acquisition\coin_gecko'

    # Option 1: Use free API (limited to last 365 days)
    # btc_data = get_data('bitcoin')
    
    # Option 2: Use paid API key for full historical data from 2013
    # Get your API key from: https://www.coingecko.com/en/api/pricing
    # You can set it as an environment variable: COINGECKO_API_KEY
    api_key = os.getenv('COINGECKO_API_KEY', None)  # Set this or pass directly
    
    # If you have an API key, you can fetch from 2013
    if api_key:
        print('Fetching historical data from 2013 using API key...')
        btc_data = get_data('bitcoin', api_key=api_key, start_date=datetime(2013, 1, 1))
    else:
        print('Using free API (last 365 days only). Set COINGECKO_API_KEY environment variable for full historical data.')
        btc_data = get_data('bitcoin')
    
    btc_path = os.path.join(save_dir, 'gecko_btc_data.parquet.gzip')
    btc_data.to_parquet(btc_path, compression='gzip')
    
    # Fetch Ethereum data
    if api_key:
        eth_data = get_data('ethereum', api_key=api_key, start_date=datetime(2015, 8, 6))  # ETH launched in 2015
    else:
        eth_data = get_data('ethereum')
    
    eth_path = os.path.join(save_dir, 'gecko_eth_data.parquet.gzip')
    eth_data.to_parquet(eth_path, compression='gzip')