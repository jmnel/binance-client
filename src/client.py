import sqlite3
from pprint import pprint
import json
from pathlib import Path
from time import perf_counter, sleep
import multiprocessing as mp
#from multiprocessing import Pipe, Process
#from multiprocessing.connection import Connection
from multiprocessing.connection import Connection
from enum import Enum
from collections import namedtuple

import matplotlib
matplotlib.use('module://matplotlib-backend-kitty')
import matplotlib.pyplot as plt

import settings
from api import get_aggregate_trades


N = settings.NUM_WORKERS
DB_PATH = settings.DATA_DIRECTORY / settings.DATABASE_NAME


class MessageType(Enum):
    STOP = 0
    WAIT_UPDATE = 1
    AGG_TRADES_REQUEST = 2
    AGG_TRADES_RESULT = 3


Message = namedtuple('Message', 'type args')


def prep_database():

    if DB_PATH.exists():
        DB_PATH.unlink()
    db = sqlite3.connect(DB_PATH)

    db.execute('''
CREATE TABLE trades_BTCUSDT(
    id INTEGER PRIMARY KEY,
    timestamp UNSIGNED INT,
    price FLOAT,
    quantity FLOAT);''')
    db.close()


def write_buffer(buff):

    buff_2 = list()

    if len(buff) > 0:
        for b in buff:
            if b is not None:
                if len(b) > 0:
                    if len(buff_2) > 0:
                        assert buff_2[-1][0] + 1 == b[0][0]
                    buff_2.extend(b)

    if len(buff_2) > 0:
        db = sqlite3.connect(DB_PATH)
        db.executemany('INSERT INTO trades_BTCUSDT(id, timestamp, price, quantity) VALUES(?, ?, ?, ?)',
                       buff_2)
        db.commit()
        db.close()

#    print(f'wrote {len(buff_2)} rows')


def proc(worker_idx: int, child_conn: Connection):

    weight = 0.

    prev_time = perf_counter()

    while True:
        while not child_conn.poll():
            sleep(1)

        msg = child_conn.recv()
        if msg.type == MessageType.STOP:
            #            print(f'recevied stop')
            break

        elif msg.type == MessageType.AGG_TRADES_REQUEST:

            while True:
                wave, symbol, from_id = msg.args
#                print(f'w={wave}, s={symbol}, idx={from_id}')

                try:
                    result = get_aggregate_trades(symbol, from_id)
                except:
                    print(f'failed')
                    sleep(5)
                    continue

                if result.status == 200:
                    true_weight = result.used_weight
                    data = result.data
                    if len(data) == 0:
                        last_id = -1
                    else:
                        last_id = data[-1][0]
                    child_conn.send(Message(MessageType.AGG_TRADES_RESULT, (wave, true_weight, data, from_id, last_id)))
                    break

                else:
                    print(f'failed: {result.status}')
                    sleep(10)

        elif msg.type == MessageType.WAIT_UPDATE:
            weight = msg.args[0]
#            print(f'{worker_idx} : new weight={weight}')

    new_time = perf_counter()
    elapsed = new_time - prev_time
    prev_time = new_time

    weight = weight - settings.WEIGHT_DECAY * elapsed

#    print('worker done')


def main():

    prep_database()

    parent_conn, child_conn = tuple(zip(*(mp.Pipe() for _ in range(settings.NUM_WORKERS))))
    workers = tuple(mp.Process(target=proc, args=(i, child_conn[i],)) for i in range(settings.NUM_WORKERS))
    for w in workers:
        w.start()

#    parent_conn[2].send(Message(MessageType.WAIT_UPDATE, (12,)))

    sleep(2)
    wave = 0
    next_idx = 0
#    next_idx = 381084197 + 1002 * 500
#    next_idx = int(382082680 - 1e9)
#    print(next_idx)
#    exit()
    weight = 0.0
    busy = [False, ] * N

#    q = 2000
    p = 0
    buff = list(None for _ in range(N))

    weights = list()

    should_stop = False
    is_stopping = False

    iterations = 0
    prev_wave = 0

    while True:

        if iterations % 100 == 0 and wave != prev_wave:
            print('Wave {}: Weight: {}, Progress: {:.1f}, Next ID: {}'.format(
                wave + 1, weight, 100. * (next_idx / 382116257), next_idx))
            prev_wave = wave

        iterations += 1

        weight_updated = False
        weight_i = [0, ] * N

        # Check children for messages.
        for i in range(N):
            if parent_conn[i].poll():
                msg = parent_conn[i].recv()

                if msg.type == MessageType.AGG_TRADES_RESULT:
                    wave_i, true_weight, data, from_id, last_id = msg.args
                    if wave_i == wave:
                        weight_i[i] = true_weight
                    print(f'{i} : {wave_i}, {from_id} -> {last_id} : {true_weight}')
                    busy[i] = False
                    assert buff[i] is None
#                    assert data[0][0] == from_id
#                    assert data[-1][0] == last_id

                    if len(data) < 1002:
                        should_stop = True
#                        print('reached end')

                    buff[i] = data
                    p += 1

        if all(map(lambda b: b is not None, buff)):
            #            print(f'writing buffer')

            write_buffer(buff)
#            for idx in range(N - 1):
#                assert buff[idx][-1][0] + 1 == buff[idx + 1][0][0]

            buff = list(None for _ in range(N))

#        if p >= q:
            #            print('done')
#            break

        if not any(busy):

            weight = max(weight_i)
#            weight_updated = True
#            wave += 1

            t_0 = perf_counter()

            while weight >= 1000:
                #                print(f'waiting {weight}')
                t_1 = perf_counter()
                t_d = t_1 - t_0
                t_0 = t_1
                weight = max(0, weight - settings.WEIGHT_DECAY * t_d)
#                print(f'new weight: {weight}')
                sleep(0.1)

            wave += 1
            for i in range(N):
                parent_conn[i].send(Message(MessageType.AGG_TRADES_REQUEST, (wave, 'BTCUSDT', next_idx)))
                next_idx += 1002
                busy[i] = True

        weights.append(weight)

#        if p % N == 0:
#            plt.plot(weights)
#            plt.show()

        if should_stop and not is_stopping:
            sleep(10)
            is_stopping = True
            print('should stop')
            continue

        if is_stopping:
            print('Done')

            c = len(tuple(filter(lambda e: e is not None, buff)))
#            print(f'{c} left in buffer')
            write_buffer(buff)

            break

            #    for i in range(NUM_WORKERS):
            #        parent_conn[i % 4].send(Message(MessageType.AGG_TRADES_REQUEST, ('BTCTUSD', next_idx)))
            #        result = parent_conn[i % 4].recv()

            #        args = result.args
            #        print(f'{next_idx} -> {args[-1]+1}')
            #        print(f'len: {args[-1] - next_idx}')

            #        print(f'true len: {len(result.args[1])}')
            #        t0 = args[1][0][0]
            #        t1 = args[1][-1][0]
            #        print(f'{t0} -> {t1}')
            #        print()
            #        next_idx = args[-1] + 1

    sleep(2)
    for idx in range(settings.NUM_WORKERS):
        parent_conn[idx].send(Message(MessageType.STOP, None))

    sleep(2)
#    while not parent_conn.poll():
#        print('waiting')
#        sleep(0.5)


#    x = parent_conn.recv()
#    p.kill()

#    print(x)


main()
