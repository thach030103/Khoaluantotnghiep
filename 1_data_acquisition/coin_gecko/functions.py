import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

import requests
import pandas as pd
from datetime import datetime, timedelta
from utils.wrappers import timeit, telegram_notify
from tqdm import tqdm
import time

# ---------- Cấu hình hiển thị toàn bộ số thập phân ----------
# Hiển thị tất cả chữ số thập phân, không dùng e-notation
pd.set_option('display.float_format', '{:.6f}'.format)



@timeit
@telegram_notify
def get_data(coin: str = 'bitcoin', prefix: bool = True):
    """
    Fetch full history market data (price, market_cap, total_volume) from CoinGecko.
    Market data: prices, market_cap, total_volume
    """
    if coin == 'bitcoin':
        start_time = datetime(2025, 10, 19)
        pref = 'btc_'
    elif coin == 'ethereum':
        start_time = datetime(2025, 9, 30)
        pref = 'eth_'
    else:
        raise ValueError("Coin not supported")
    
    end_time = datetime(2025, 11, 21)  # Lấy 1 năm mẫu
    df = pd.DataFrame()
    
    total_days = (end_time - start_time).days
    pbar = tqdm(total=total_days, desc=f"{coin} data")

    # ------------------------
    # Fetch Market Chart Data
    # ------------------------
    market_dfs = []
    batch_days = 90  # lấy 90 ngày/lượt
    current_start = start_time
    while current_start < end_time:
        current_end = min(current_start + timedelta(days=batch_days), end_time)

        from_ts = int(current_start.timestamp())
        to_ts = int(current_end.timestamp())
        url = f"https://api.coingecko.com/api/v3/coins/{coin}/market_chart/range"
        params = {"vs_currency": "usd", "from": from_ts, "to": to_ts}

        try:
            resp = requests.get(url, params=params).json()
            tmp_df = pd.DataFrame({
                'timestamp': [p[0]/1000 for p in resp.get('prices', [])],
                'prices': [p[1] for p in resp.get('prices', [])],
                'market_caps': [p[1] for p in resp.get('market_caps', [])],
                'total_volumes': [p[1] for p in resp.get('total_volumes', [])],
            })
            market_dfs.append(tmp_df)
        except Exception as e:
            print(f"Error fetching market data for {current_start}: {e}")

        current_start = current_end
        pbar.update(batch_days)
        time.sleep(0.1)

    # Concat tất cả batch lại
    df = pd.concat(market_dfs, ignore_index=True)

    # Thêm cột datetime
    df['datetime'] = pd.to_datetime(df['timestamp'], unit='s')
    df['datetime'] = df['datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')  # định dạng Y-m-d H:M:S


    # Thêm prefix
    if prefix:
        df = df.add_prefix(pref)
        # Thêm datetime dựa trên timestamp đã được prefix
        df[f'{pref}datetime'] = pd.to_datetime(df[f'{pref}timestamp'], unit='s').dt.strftime('%Y-%m-%d %H:%M:%S')

    # Đặt index
    if f'{pref}timestamp' in df.columns:
        df = df.set_index(f'{pref}timestamp')

    pbar.close()
    print(f"✓ Done: full history fetched for {coin}")
    return df.sort_index()

