import sys
sys.path.append('..')
from trade.orgnization.analyst import initialize_trading_engine
from utils.configuration import load_configuration, GatewayTypes
from vnpy.trader.event import EVENT_TICK
from multiprocessing import Queue, Process
from queue import Full, Empty
import time
from dataclasses import asdict
from datetime import datetime


def tracking_routine(configuration, shared_queue):

    def customized_on_tick(event):
        data = event.data
        try:
            print(data)
            shared_queue.put(data, block=True, timeout=1)
        except Full:
            print("Queue is full")

    ctp_setting = configuration['ctp_setting']
    gateway = GatewayTypes[configuration['gateway']]
    symbols = configuration['vt_symbols']

    engine = initialize_trading_engine(gateway, ctp_setting)
    engine.subscribe(symbols)
    engine.event_engine.register(EVENT_TICK, customized_on_tick)

    while engine.get_tick(symbols[0]) is None:
        print('Confirming subscription.')
        engine.subscribe(symbols)
        time.sleep(1)


def save_to_file(file_name, tick_data):
    with open(file_name, 'a') as f:
        f.writelines(tick_data)
    tick_data.clear()


def save_tick_routine(configuration, shared_queue):
    file_name = configuration['save_path']
    tick_data = []

    while True:
        h = datetime.now().hour
        print(h)
        if h not in [0, 1, 2, 3, 9, 10, 11, 12, 13, 14, 15, 21, 22, 23]:
            save_to_file(file_name, tick_data)
            return
        elif len(tick_data) >= 1000:
            save_to_file(file_name, tick_data)
            tick_data.clear()
        else:
            try:
                data = shared_queue.get(block=True, timeout=1)
                tick_data.append(str(asdict(data)))
            except Empty:
                pass


def main():
    if len(sys.argv) != 2:
        raise Exception("please specify configuration file path")

    shared_queue = Queue()

    file_name = sys.argv[1]
    configuration = load_configuration(file_name)

    # initialize process to tracking data
    tracking_proc = Process(target=tracking_routine, args=(configuration, shared_queue))
    tracking_proc.daemon = True
    tracking_proc.start()

    saving_proc = Process(target=save_tick_routine, args=(configuration, shared_queue))
    saving_proc.daemon = True
    saving_proc.start()

    tracking_proc.join()
    saving_proc.join()


if __name__ == '__main__':
    main()
