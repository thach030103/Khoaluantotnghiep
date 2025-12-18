import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

import time
import random
import requests
import pandas as pd
from datetime import datetime, timedelta
from utils.wrappers import log_execution, timeit, telegram_notify

url = 'https://api.pullpush.io/reddit/search/submission'

# danh sách User-Agent để xoay vòng
user_agents = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15)',
    'Mozilla/5.0 (X11; Linux x86_64)'
]

@log_execution
def fetch_data(subreddit, after, before, size=50, max_retries=5):
    """
    Fetch posts from Pushshift in a given interval.
    """
    params = {
        'subreddit': subreddit,
        'after': after,
        'before': before,
        'size': size,
    }

    for attempt in range(max_retries):
        headers = {'User-Agent': random.choice(user_agents)}
        try:
            print(f"🔁 Attempt {attempt+1}/{max_retries} | Fetching {after} → {before}")
            response = requests.get(url, headers=headers, params=params, timeout=15)

            if response.status_code == 200:
                data = response.json().get('data', [])
                print(f"✅ Got {len(data)} posts")
                return data

            elif response.status_code in [403, 429, 430]:
                wait = random.randint(5, 15)
                print(f"⚠️ Status {response.status_code}, sleeping {wait}s...")
                time.sleep(wait)

            else:
                print(f"⚠️ Status {response.status_code}, retrying in 5s")
                time.sleep(5)

        except requests.exceptions.Timeout:
            wait = random.randint(3, 7)
            print(f"⏰ Timeout, retry in {wait}s")
            time.sleep(wait)
        except Exception as e:
            wait = random.randint(3, 7)
            print(f"❌ Error: {e}, retry in {wait}s")
            time.sleep(wait)

    print("🚫 Max retries reached. Returning empty list.")
    return []

@timeit
@telegram_notify
def get_historic_data(subreddit: str, after: int, before: int, interval_sec: int = 21600):
    """
    Fetch historic Reddit posts by interval (default 6h = 21600s)
    """
    all_data = pd.DataFrame()
    total_posts = 0

    while after < before:
        temp_before = min(after + interval_sec, before)
        posts = fetch_data(subreddit, after, temp_before, size=50)

        if len(posts) == 0:
            print("✅ No posts in this interval.")
            after = temp_before
            continue

        # transform to pandas df
        columns = ('url','created_utc', 'author', 'num_comments', 'score', 'title', 'selftext')
        df = pd.DataFrame(posts)

        # chỉ lấy các cột tồn tại
        df = df.loc[:, [c for c in columns if c in df.columns]]

        # xử lý thời gian
        if 'created_utc' in df.columns:
            # 1. Ép numeric trước (bắt buộc)
            df['created_utc'] = pd.to_numeric(df['created_utc'], errors='coerce')

            # 2. Loại bỏ giá trị không hợp lệ
            df = df.dropna(subset=['created_utc'])

            # 3. Ép về int64
            df['created_utc'] = df['created_utc'].astype('int64')

            # 4. Chuyển sang datetime
            df['time'] = pd.to_datetime(df['created_utc'], unit='s', errors='coerce')

        elif 'utc_datetime_str' in df.columns:
            df['time'] = pd.to_datetime(df['utc_datetime_str'], format='%Y-%m-%d %H:%M:%S', errors='coerce')
        else:
            print("⚠️ No datetime column found. Here is a sample post:")
            print(posts[:3])
            return pd.DataFrame()  # tạm dừng, tránh raise ValueError

        df = df.dropna(subset=['time'])
        df['timestamp'] = df['time'].astype('int64') // 10**9
        df = df.set_index('timestamp').sort_index()

        all_data = pd.concat([all_data, df])
        total_posts += len(df)
        print(f"📦 Retrieved {len(df)} posts, total = {total_posts}")

        # Next interval
        after = int(df.index[-1]) + 1 if not df.empty else temp_before
        time.sleep(random.uniform(5, 15))  # tránh rate limit

    print(f"\n🎯 Done! Total collected posts: {total_posts}")
    return all_data.sort_index()