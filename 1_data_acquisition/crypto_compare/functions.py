''' Functions for fetching CryptoCompare data. '''

import requests
import json
import pandas as pd
import time

url = 'https://min-api.cryptocompare.com/data/'
api_key = 'b74c35af8d711e416096ee9a5524bdc9f15dfe975f6e7f61c8e398d011f6b55e'
auth_key = None

def get_data(feature: str,
             params: dict,
             coin: str = 'btc',
             prefix: str = None,
             itype: int = 2):

    # xác định thời gian bắt đầu dựa trên loại coin
    if coin == 'btc':
        start_time = 1314316800
    elif coin == 'eth':
        start_time = 1438819200
    else:
        raise ValueError(f'Coin not supported: {coin}')

    headers = {'authorization': f'Apikey {api_key}'}
    params.update({'limit': 2000})

    # --- lần fetch đầu tiên ---
    response = requests.get(url + feature, params=params, headers=headers)
    if response.status_code != 200:
        print(f"⚠️ Error fetching {feature}: HTTP {response.status_code}")
        return pd.DataFrame()

    data_dict = response.json()

    # --- kiểm tra dữ liệu hợp lệ ---
    if 'Response' in data_dict and data_dict['Response'] == 'Error':
        print(f"❌ API Error for {feature}: {data_dict.get('Message')}")
        return pd.DataFrame()

    if 'Data' not in data_dict:
        print(f"⚠️ Invalid response for {feature}: missing 'Data' field.")
        return pd.DataFrame()

    # --- chuyển đổi sang DataFrame ---
    data_pandas = pd.DataFrame()

    if itype == 1:
        data_list = data_dict.get('Data', {}).get('Data', [])
    else:
        data_list = data_dict.get('Data', [])

    if not isinstance(data_list, list) or len(data_list) == 0:
        print(f"⚠️ No valid data found for {feature}")
        return pd.DataFrame()

    for item in data_list:
        data_pandas = pd.concat([data_pandas, pd.DataFrame([item])], ignore_index=True)

    # --- xử lý cột time ---
    if 'time' not in data_pandas.columns:
        print(f"⚠️ Warning: 'time' column missing in first batch for {feature}.")
        return pd.DataFrame()

    data_pandas = data_pandas.set_index('time')

    # --- xoá cột notes nếu có ---
    if 'notes' in data_pandas.columns:
        data_pandas = data_pandas.drop(columns='notes')

    # --- bắt đầu fetch các batch tiếp theo ---
    while (
        data_pandas.duplicated().sum() < 1000
        and data_pandas.index.min() > start_time
    ):
        oldest_time = int(data_pandas.index.min())
        params.update({'toTs': oldest_time})

        try:
            response = requests.get(url + feature, params=params, headers=headers, timeout=10)
            data_dict = response.json()
        except Exception as e:
            print(f"⚠️ Error fetching next batch for {feature}: {e}")
            break

        if 'Response' in data_dict and data_dict['Response'] == 'Error':
            print(f"⚠️ API Error in next batch for {feature}: {data_dict.get('Message')}")
            break

        # --- convert batch mới ---
        data_pandas_new = pd.DataFrame()
        if itype == 1:
            data_list = data_dict.get('Data', {}).get('Data', [])
        else:
            data_list = data_dict.get('Data', [])

        if not isinstance(data_list, list) or len(data_list) == 0:
            print(f"⚠️ Empty batch for {feature}, stopping.")
            break

        for item in data_list:
            data_pandas_new = pd.concat([data_pandas_new, pd.DataFrame([item])], ignore_index=True)

        if 'time' not in data_pandas_new.columns:
            print(f"⚠️ Warning: 'time' column missing in response. Skipping batch.")
            continue

        data_pandas_new = data_pandas_new.set_index('time')
        data_pandas = pd.concat([data_pandas_new[:-1], data_pandas])

        if 'notes' in data_pandas.columns:
            data_pandas = data_pandas.drop(columns='notes')

        time.sleep(0.2)  # tránh bị rate limit

    # --- thêm prefix cho các cột ---
    if prefix:
        data_pandas = data_pandas.add_prefix(prefix)

    # --- lọc dữ liệu hợp lệ ---
    data_pandas = data_pandas[data_pandas.index > start_time]
    data_pandas = data_pandas.loc[~(data_pandas == 0).all(axis=1)]

    return data_pandas


# --------------------------
# convert_balance_data + get_balance_data giữ nguyên, chỉ bổ sung kiểm tra an toàn
# --------------------------

def convert_balance_data(data_dict: dict):
    if not data_dict or 'Data' not in data_dict or 'Data' not in data_dict['Data']:
        print("⚠️ Invalid balance data format.")
        return pd.DataFrame()

    data_pandas = pd.DataFrame()
    for i in range(len(data_dict['Data']['Data'])):
        record = data_dict['Data']['Data'][i]
        balances = record.get('balance_distribution', [])
        if not balances:
            continue

        x = pd.DataFrame(balances)
        if 'to' in x.columns:
            x = x.drop(columns='to')

        x = pd.melt(x, id_vars=x.columns[1:3])
        x['idx'] = x.variable + '_' + x.value.round(3).astype(str)
        x = x.drop(columns=['variable', 'value']).transpose()
        x.columns = x.iloc[-1].values
        x = x.iloc[:-1]
        x['time'] = record.get('time')
        x = x.set_index('time').rename_axis(None, axis=1)
        data_pandas = pd.concat([data_pandas, x])

    return data_pandas


def get_balance_data(feature: str, params: dict, prefix: str = None):
    headers = {'authorization': f'Apikey {api_key}'}
    params.update({'limit': 2000})

    response = requests.get(url + feature, params=params, headers=headers)
    data_dict = response.json()

    if 'Response' in data_dict and data_dict['Response'] == 'Error':
        print(f"⚠️ API Error in balance data: {data_dict.get('Message')}")
        return pd.DataFrame()

    data_pandas = convert_balance_data(data_dict)

    while (
        data_pandas.duplicated().sum() < 1000
        and data_pandas.index.min() > 1314316800
    ):
        oldest_time = int(data_pandas.index.min())
        params.update({'toTs': oldest_time})

        response = requests.get(url + feature, params=params, headers=headers)
        data_dict = response.json()

        new_data = convert_balance_data(data_dict)
        if new_data.empty:
            break

        data_pandas = pd.concat([new_data[:-1], data_pandas])
        time.sleep(0.2)

    if prefix:
        data_pandas = data_pandas.add_prefix(prefix)

    data_pandas = data_pandas[data_pandas.index > 1314316800]
    data_pandas = data_pandas.loc[~(data_pandas == 0).all(axis=1)]

    return data_pandas

