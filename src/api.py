import requests
import sqlite3
from pprint import pprint
import json
from pathlib import Path
from collections import namedtuple
from time import perf_counter, sleep

import settings


def get_exchange_info():

    url = settings.ENDPOINT_BASE + 'exchangeInfo'
    response = requests.get(url)

    exchange_info = response.json()

    return exchange_info


AggTradeResult = namedtuple('AggTradeResult', 'status used_weight data')


def get_aggregate_trades(symbol: str, from_id: int):

    if from_id < 0:
        raise ValueError('from_id must be >= 0')

    url = settings.ENDPOINT_BASE + 'aggTrades?symbol={}&fromId={}&limit=1000'
    url = url.format(symbol, from_id)

    response = requests.get(url)
    status = response.status_code

    if status == 200:
        used_weight = int(response.headers['x-mbx-used-weight'])
        used_weight_1m = int(response.headers['x-mbx-used-weight-1m'])
        data = tuple((r['a'], r['T'], float(r['p']), float(r['q'])) for r in response.json())
        return AggTradeResult(status, max(used_weight, used_weight_1m), data)
    elif status == 429:
        print(f'EXCEEDED RATE LIMIT')
        exit()
    else:
        print(f'Request failed: {status} -> {response.reason}')
        return AggTradeResult(status, None, None)


# if __name__ == '__main__':
    #    exchange_info = get_exchange_info()

#    weight = 0.

#    idx = 0

#    last_update = perf_counter()
#    for i in range(1000):

#        new_time = perf_counter()
#        elapsed_time = new_time - last_update
#        last_update = new_time

#        if weight < 500:
#            result = get_aggregate_trades('BTCTUSD', idx)
#            weight = float(result.used_weight)
#            idx_next = result.data[-1][0] + 1
#            print(f'{idx} -> {idx_next}')
#            idx = idx_next
#        else:
#            weight = weight - 18. * elapsed_time
#            sleep(1)

#        weight = max(weight, 0)

#        print(weight)

#    data = get_aggregate_trades('BTCTUSD', 0)
#    symbols = tuple(r['symbol'] for r in exchange_info['symbols'])
#    pprint(sorted(symbols))
#    print(exchange_info['symbols'][2])
