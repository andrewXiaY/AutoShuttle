import sys
sys.path.append("..")
from trade.orgnization.analyst import initialize_trading_engine
import time
import json
from queue import Full
from dataclasses import dataclass
from multiprocessing import Queue, Process
from vnpy.trader.object import TickData
from vnpy.trader.constant import OrderType
from vnpy.app.script_trader import ScriptEngine
from vnpy.event import Event
from vnpy.trader.event import EVENT_TICK
from vnpy.gateway.ctp import CtpGateway
from vnpy.trader.object import Status


"""
程序分为两部分：
    1. 主程序订阅行情数据，并将tick data放入到一个进程安全的Queue中
    2. 子进程根据Queue中数据进行计算，进行交易，子进程需要维护一个TradeState的dictionary

所有的可变量都放入到一个配置文件中， 这样只需要改动配置文件即可改变交易对象或者是交易参数，
TODO：通过动配置文件达到在程序运行时改动变量的目的

如何运行？
    python price_diff.py "path/to/configuration/file"
"""


@dataclass
class TradeState:
    """
    这个类用来记录当前交易的状态
    """
    open_pos: int = 0
    open_price: int = 0
    i: int = 0
    order_id_buy: str = ''
    order_id_sell: str = ''
    order_id_profit_taking: str = ''

    def reset_all(self):
        self.open_pos = 0
        self.open_price = 0
        self.i = 0
        self.order_id_buy = ''
        self.order_id_sell = ''
        self.order_id_profit_taking = ''


STATE = TradeState()


def price_change_in_normal_range(change, upper_bound=2000, lower_bound=-2000):
    """判断价格变动是否在可交易区间"""
    return lower_bound <= change <= upper_bound


def consume_tick_data(shared_queue: Queue, configuration: dict):
    """
    用来消耗tick数据的方法，
    """
    ctp_setting = configuration['ctp_setting']
    vt_symbol = configuration['vt_symbol']

    # 连接交易所并订阅相应symbol
    engine = initialize_trading_engine(CtpGateway, ctp_setting)
    engine.subscribe([vt_symbol])

    # confirm subscription is success
    while engine.get_tick(vt_symbol) is None:
        print('Tick data consumer is confirming subscription.')
        engine.subscribe([vt_symbol])
        time.sleep(1)

    # 确保至少有一条数据在共享队列中，用来当作最开始的处理对象
    print("Trade process starts")
    while shared_queue.qsize() == 0:
        print("Tick data consumer is waiting tick data.")
        time.sleep(0.6)
        continue

    # 拿到第一条tick data，当作旧数据
    pre_tick: TickData = shared_queue.get(block=True, timeout=1)

    while True:
        if shared_queue.qsize() >= 1:
            # 从队列中取得第一条数据当作最新数据
            cur_tick = shared_queue.get(block=True, timeout=1)
            # 根据最新数据和旧数据进行交易，这里不需要使用到queue，所以不会影响订阅推送
            process_tick(pre_tick, cur_tick, engine, configuration)
            # 将最新tick数据赋值给旧数据变量
            pre_tick = cur_tick


def process_tick(prev_tick: TickData, cur_tick: TickData, engine: ScriptEngine, configuration: dict):
    """
    交易逻辑
    """
    tick_th = configuration['tick_th']
    vol_th = configuration['vol_th']
    vt_symbol = configuration['vt_symbol']
    num_slot = configuration['num_slot']
    hold_ticks = configuration['hold_ticks']
    hold_ticks_close = configuration['hold_ticks_close']  # max holding ticks
    tgt_ticks = configuration['tgt_ticks']
    tick_size = configuration['tick_size']
    tgt_profit = tgt_ticks * tick_size
    log_file_path = configuration['log_file']

    p_change = cur_tick.last_price - prev_tick.last_price  # 价格变动
    vol_change = cur_tick.volume - prev_tick.volume   # volume变动
    if price_change_in_normal_range(p_change):
        print("=================================================")
        print(f"p_change: {p_change}, vol_change: {vol_change}")
        if p_change < tick_th and vol_change < vol_th:  # 成交量很小 但是价格下跌很迅速
            if STATE.open_pos == 0:
                print("Open Stage")
                STATE.order_id_buy = engine.buy(vt_symbol, volume=num_slot, order_type=OrderType.LIMIT,
                                                price=cur_tick.bid_price_1 + tick_size)  # 超价一个tick去买
                STATE.open_price = cur_tick.bid_price_1
                print(f"open long： {STATE.open_price}")
                STATE.i += 1
                STATE.open_pos = 1

        elif STATE.open_pos == 1 and STATE.i < hold_ticks:
            print("Less than hold ticks stage")
            if engine.get_order(STATE.order_id_buy) is None:
                STATE.i += 1
            elif engine.get_order(STATE.order_id_buy).status == Status.ALLTRADED:
                STATE.i += 1
                if STATE.order_id_profit_taking != '':
                    print(f'Profit taking order exist: {STATE.order_id_profit_taking}')
                else:
                    # 已经买入，挂卖出限价单
                    STATE.order_id_profit_taking = engine.sell(vt_symbol, volume=num_slot, order_type=OrderType.LIMIT,
                                                               price=(STATE.open_price + tgt_profit), ost='CLOSETODAY')
                    print(f'Send profit taking order: {STATE.order_id_profit_taking}')
            else:
                engine.cancel_order(STATE.order_id_buy)  # 取消之前的买单，按照新的对价挂单
                print(f"Cancel buy order: {STATE.order_id_buy}")
                STATE.order_id_buy = engine.buy(vt_symbol, volume=num_slot, order_type=OrderType.LIMIT,
                                                price=cur_tick.bid_price_1)
                STATE.open_price = cur_tick.bid_price_1
                print(f"Open long: {STATE.open_price}")

        elif STATE.open_pos == 1 and STATE.i == hold_ticks:
            print("Equal to hold ticks stage")
            if STATE.order_id_profit_taking == '':
                STATE.i += 1
            elif engine.get_order(STATE.order_id_profit_taking).status == Status.ALLTRADED:
                print(f'Profit taking order filled: {STATE.order_id_profit_taking}')
                STATE.reset_all()
            else:
                engine.cancel_order(STATE.order_id_profit_taking)  # 撤销挂出的止盈单
                print(f'Cancel profit taking order {STATE.order_id_profit_taking}')
                STATE.order_id_profit_taking = ''
                # 按照即刻的对价成交
                STATE.order_id_sell = engine.sell(vt_symbol, volume=num_slot, order_type=OrderType.LIMIT,
                                                  price=cur_tick.ask_price_1, ost='CLOSETODAY')
                print(f"Close long: {cur_tick.ask_price_1}")
                # 马上判断是否成交
                if engine.get_order(STATE.order_id_sell) is None:
                    print(f"Close long rejected: {STATE.order_id_sell}")
                    STATE.reset_all()
                elif engine.get_order(STATE.order_id_sell).status == Status.ALLTRADED:
                    print(f"Close long filled: {STATE.order_id_sell}")
                    STATE.reset_all()
                else:
                    STATE.i += 1

        elif STATE.open_pos == 1 and hold_ticks < STATE.i < hold_ticks_close:  # 发出平仓指令之后，但是未抵达最长持仓限制
            print("Larger than hold ticks stage")
            # 判断是否成交 - 如果成交就跳出
            if engine.get_order(STATE.order_id_sell) is None:
                STATE.i += 1
            elif engine.get_order(STATE.order_id_sell).status == Status.ALLTRADED:
                print(f"Close long filled: {STATE.order_id_sell}")
                STATE.reset_all()
            else:
                STATE.i += 1

        elif STATE.open_pos == 1 and STATE.i >= hold_ticks_close:
            print("Larger than hold tick close stage")
            if engine.get_order(STATE.order_id_sell) is None:
                STATE.i += 1
            elif engine.get_order(STATE.order_id_sell).status == Status.ALLTRADED:
                print(f"Close long filled: {STATE.order_id_sell}")
                STATE.reset_all()
            else:
                engine.cancel_order(STATE.order_id_sell)  # 取消之前的卖单，按照目前市价单平仓
                # market price order to be filled
                STATE.order_id_sell = engine.sell(vt_symbol, volume=num_slot, order_type=OrderType.LIMIT,
                                                  price=cur_tick.ask_price_1 - tick_size, ost='CLOSETODAY')
                print('send sell order - excel max holding time.')
                STATE.i += 1
        print("==================================================")


def main():

    shared_queue = Queue()

    def customized_on_tick(event: Event, q=shared_queue):
        """
        自定义的on tick函数， 用来处理推送的数据，将数据TickData直接放入共享队列中
        """
        tick_data = event.data
        try:
            q.put(tick_data, block=True, timeout=1)
        except Full:
            print("Queue is full")
        return tick_data

    # ===============  loading configurations ===============
    with open(sys.argv[1], 'r', encoding='UTF-8') as f:
        configuration = json.load(f, encoding='UTF-8')

    ctp_setting = configuration['ctp_setting']
    vt_symbol = configuration['vt_symbol']
    # =======================================================

    # 先运行消耗tick data的程序，这样能够保证当数据第一时间到的时候能马上进行处理
    trading_proc = Process(target=consume_tick_data, args=(shared_queue, configuration,))
    trading_proc.daemon = True
    trading_proc.start()

    # 连接交易所并订阅相应symbol
    engine = initialize_trading_engine(CtpGateway, ctp_setting)
    engine.subscribe([vt_symbol])
    engine.event_engine.register(EVENT_TICK, customized_on_tick)

    # confirm subscription is success
    while engine.get_tick(vt_symbol) is None:
        print("\rTick data tracker is confirming subscription.")
        engine.subscribe([vt_symbol])
        time.sleep(1)

    while True:
        time.sleep(0.5)

    trading_proc.join()


if __name__ == '__main__':
    main()
