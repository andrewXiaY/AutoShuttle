"""
将交易逻辑封装在这个类中，在operation machine中initialize trader这个类
trader = Trader(account_number[ticker number?])
trader.trade(signal) 将进行交易

pos: Z3
"""
from vnpy.gateway.ctp import CtpGateway
from vnpy.trader.constant import OrderType
from orgnization.analyst import initialize_trading_engine
from datetime import datetime
from orgnization.analyst import \
    write_log, ticker_jq2vt, ctp_ticker_to_symbol, get_current_position, round_price_tick, get_roll_v2, sync_routine
from constants import PARENT_PATH, ST_NAME, ALL_TICKERS, CMD_NAV
import time
import pandas as pd
from sqlalchemy import create_engine
from configuration import Trades_DB_ADD, Intern_DB_ADD, Signal_DB_ADD

position_data_file_path = PARENT_PATH + '/position_data/position_data.json'
position_cost_data_file_path = PARENT_PATH + '/position_data/position_cost_data.json'


class Trader:
    RETRY_TIMES = 10

    def __init__(self, gateway=CtpGateway):
        self.trade_engine = initialize_trading_engine(gateway)
        self.gateway = gateway
        self.cur_pos = None
        self.vt_symbol = None
        self.num_slot = '1'
        self.orders = []

        self.db_engine = create_engine(Trades_DB_ADD)
        self.engine_intern = create_engine(Intern_DB_ADD)
        self.engine_signal = create_engine(Signal_DB_ADD)
        self._update_price_tick_dict()
        self._subscribe_quotes()

    def _update_price_tick_dict(self):
        self.price_tick_dict = self.trade_engine.get_all_contracts(True).set_index('vt_symbol')['pricetick'].to_dict()

    def _format_cur_pos(self, ticker, current_time):
        self.cur_pos['direction_str'] = [x.name for x in self.cur_pos['direction']]
        self.vt_symbol = ticker_jq2vt(ticker, current_time.date())
        # add simple ticker
        self.cur_pos['symbol_simple'] = self.cur_pos['vt_symbol'].apply(ctp_ticker_to_symbol)

        write_log(self.vt_symbol)

    def _pre_trade(self, ticker):
        current_time = datetime.now()
        print(ticker, " Trade")

        self.cur_pos = self.trade_engine.get_all_positions(True)
        while self.cur_pos is None:
            self.trade_engine = initialize_trading_engine(self.gateway)
            self.cur_pos = self.trade_engine.get_all_positions(True)

        self._format_cur_pos(ticker, current_time)
        return current_time

    def _subscribe_quotes(self):
        # 订阅行情
        tick = self.trade_engine.get_all_contracts(True)
        tick['pro'] = tick['product'].astype(str)
        tick = tick[tick['pro'] == 'Product.FUTURES']
        ctp_tickers = list(tick['vt_symbol'])
        self.trade_engine.subscribe(ctp_tickers)

    def _get_price(self, p_latest, hold_ctp_ticker, mode='cover'):
        p = 0
        if mode == 'cover':
            p = int(p_latest + self.price_tick_dict[hold_ctp_ticker])
        elif mode == 'sell':
            p = int(p_latest - self.price_tick_dict[hold_ctp_ticker])

        return round_price_tick(p, self.price_tick_dict[hold_ctp_ticker])

    def _action_cover(self, pos, hold_ctp_ticker):
        num_slot_cover = str(pos)
        if self.vt_symbol[-4:] == 'SHFE':
            write_log(hold_ctp_ticker + ' cover today')
            p_latest = self.trade_engine.get_tick(hold_ctp_ticker).last_price
            price = self._get_price(p_latest, hold_ctp_ticker)
            order_id_cover = self.trade_engine.cover(hold_ctp_ticker, volume=int(num_slot_cover),
                                                     order_type=OrderType.LIMIT, price=price, ost='CLOSETODAY')

            for t in [order_id_cover, str(int(p_latest * 1.001)), str(self.price_tick_dict[hold_ctp_ticker])]:
                write_log(t)

            time.sleep(0.05)
            od_info = self.trade_engine.get_order(order_id_cover, True)

            if od_info['status'][0].name == 'REJECTED':
                write_log(hold_ctp_ticker + ' cover yesterday')
                order_id_cover = self.trade_engine.cover(hold_ctp_ticker, volume=num_slot_cover,
                                                         order_type=OrderType.LIMIT, price=price, ost='CLOSEYESTERDAY')
                write_log(order_id_cover)
        else:
            _p_ = 0 if self.vt_symbol[-4:] == 'CZCE' else 1
            order_id_cover = self.trade_engine.cover(hold_ctp_ticker, volume=num_slot_cover,
                                                     order_type=OrderType.MARKET, price=_p_, ost='CLOSE')
        self.orders.append(order_id_cover)
        write_log(hold_ctp_ticker + " cover " + str(num_slot_cover) + " " + str(order_id_cover))

    def _action_sell(self, pos, hold_ctp_ticker):
        num_slot_sell = str(pos)
        if self.vt_symbol[-4:] == 'SHFE':
            write_log(hold_ctp_ticker + ' sell today')
            p_latest = self.trade_engine.get_tick(hold_ctp_ticker).last_price
            price = self._get_price(p_latest, hold_ctp_ticker, 'sell')
            order_id_sell = self.trade_engine.sell(hold_ctp_ticker, volume=num_slot_sell, order_type=OrderType.LIMIT,
                                                   price=price, ost='CLOSETODAY')
            write_log(order_id_sell)
            time.sleep(0.05)
            od_info = self.trade_engine.get_order(order_id_sell, True)

            if od_info['status'][0].name == 'REJECTED':
                txt_ = hold_ctp_ticker + ' sell yesterday'
                write_log(txt_)
                order_id_sell = self.trade_engine.sell(hold_ctp_ticker, volume=num_slot_sell,
                                                       order_type=OrderType.LIMIT, price=price, ost='CLOSEYESTERDAY')
                write_log(order_id_sell)
        else:
            _p_ = 0 if self.vt_symbol[-4:] == 'CZCE' else 1
            order_id_sell = self.trade_engine.sell(hold_ctp_ticker, volume=num_slot_sell, order_type=OrderType.MARKET,
                                                   price=_p_, ost='CLOSE')
        self.orders.append(order_id_sell)
        write_log(hold_ctp_ticker + " sell " + str(num_slot_sell) + " " + order_id_sell)

    def _action_buy(self, pos_=None, vt_symbol=None):
        if pos_ is None:
            _, pos_ = get_current_position(self.cur_pos, vt_symbol, 'LONG')

        if str(pos_) == '0':
            if self.vt_symbol[-4:] == 'SHFE':
                p_latest = self.trade_engine.get_tick(vt_symbol).last_price
                price = self._get_price(p_latest, vt_symbol)
                order_id_buy = self.trade_engine.buy(vt_symbol, volume=self.num_slot,
                                                     order_type=OrderType.LIMIT, price=price)
            else:
                _p_ = 0 if self.vt_symbol[-4:] == 'CZCE' else 1
                order_id_buy = self.trade_engine.buy(vt_symbol, volume=self.num_slot,
                                                     order_type=OrderType.MARKET, price=_p_)
            self.orders.append(order_id_buy)
            write_log(vt_symbol + " buy " + str(self.num_slot) + " " + str(order_id_buy))

    def _action_short(self, pos_=None, vt_symbol=None):
        if pos_ is None:
            _, pos_ = get_current_position(self.cur_pos, vt_symbol, 'SHORT')

        if str(pos_) == '0':
            if self.vt_symbol[-4:] == 'SHFE':
                p_latest = self.trade_engine.get_tick(vt_symbol).last_price
                price = self._get_price(p_latest, vt_symbol, mode='sell')
                order_id_short = self.trade_engine.short(vt_symbol, volume=self.num_slot, order_type=OrderType.LIMIT,
                                                         price=price)
            else:
                _p_ = 0 if self.vt_symbol[-4:] == 'CZCE' else 1
                order_id_short = self.trade_engine.short(vt_symbol, volume=self.num_slot,
                                                         order_type=OrderType.MARKET, price=_p_)

            self.orders.append(order_id_short)
            txt_ = vt_symbol + " short " + str(self.num_slot) + " " + order_id_short
            write_log(txt_)

    def _action_cover_or_sell(self, latest_signal):
        # cover
        hold_ctp_ticker1, short_pos_ = get_current_position(self.cur_pos, self.vt_symbol, 'SHORT')
        if str(short_pos_) != '0' and latest_signal == 0:
            self._action_cover(short_pos_, hold_ctp_ticker1)

        # sell
        hold_ctp_ticker2, long_pos_ = get_current_position(self.cur_pos, self.vt_symbol, 'LONG')
        if str(long_pos_) != '0' and latest_signal == 0:
            self._action_sell(long_pos_, hold_ctp_ticker2)

    def _finalize_single(self, ticker, current_time, st_name):
        try:
            order_dfs = [self.trade_engine.get_order(od, True) for od in self.orders]
            pd.concat(order_dfs).to_csv(PARENT_PATH + "/trade_orders/" + ticker[:-9] + "_" +
                                        str(current_time).replace(':', '_') + ".csv")
            order_df = pd.concat(order_dfs).set_index('datetime')
            order_df['st'] = st_name
            order_df.index = pd.to_datetime(order_df.index)
            order_df.astype(str).to_sql('orders', con=self.db_engine, if_exists='append')
            order_df.astype(str).to_sql('orders', con=self.engine_intern, if_exists='append')
            write_log('Saved order ids to db')

        except Exception as e:
            write_log(str(e))
            write_log("ticker save orders failed")

    def _confirm_subscribe(self, vt_symbol=None):
        # make sure trader can get tick data
        for t in range(Trader.RETRY_TIMES):
            if self.trade_engine.get_tick(self.vt_symbol if vt_symbol is None else vt_symbol):
                break
            self._subscribe_quotes()
            time.sleep(0.5)

            if t == Trader.RETRY_TIMES - 1:
                raise TimeoutError("Maximum Retry Time Reached")

    def _get_domain_tickers(self, ticker):
        domain_tickers = get_roll_v2(ticker, self.engine_intern)
        return [ticker_jq2vt(x) for x in domain_tickers]

    def _cal_num_slots(self, vt_symbol):
        last_price = self.trade_engine.get_tick(vt_symbol).last_price
        size = self.trade_engine.get_contract(vt_symbol).size
        return str(int(CMD_NAV / (last_price * size)))

    def trade(self, ticker, latest_signal, num_slot='1'):
        try:
            self._confirm_subscribe()
            current_time = self._pre_trade(ticker)  # 这一步会将ticker转换成vt_symbol，确保pre trade在任何操作前调用
            self.num_slot = num_slot if num_slot else self._cal_num_slots(self.vt_symbol)

            self._action_cover_or_sell(latest_signal)

            if latest_signal > 0:
                self._action_buy(vt_symbol=self.vt_symbol)

            elif latest_signal < 0:
                self._action_short(vt_symbol=self.vt_symbol)

            self._finalize_single(ticker, current_time, ST_NAME)
        except TimeoutError as e:
            print(e)

        self.orders.clear()

    def roll_contracts(self):

        for ticker in ALL_TICKERS:
            domain_tickers = self._get_domain_tickers(ticker)

            if len(domain_tickers) == 1:
                write_log(ticker + " No Need Roll")
            else:
                write_log(ticker + " Need Roll")

                vt_symbol_td, vt_symbol_yestd = domain_tickers[1], domain_tickers[0]
                write_log("Roll from " + vt_symbol_yestd + " to " + vt_symbol_td)

                try:
                    self._confirm_subscribe(vt_symbol_td)
                except TimeoutError as e:
                    print(e)
                    write_log(f'can not subscribe {ticker} data')
                    continue

                cur_pos = self.trade_engine.get_all_positions(True)
                cur_pos['direction_str'] = [x.name for x in cur_pos['direction']]
                cur_pos['symbol_simple'] = cur_pos['vt_symbol'].apply(ctp_ticker_to_symbol)

                hold_ctp_ticker_short, pos_hold_short = get_current_position(cur_pos, vt_symbol_td, 'SHORT')
                hold_ctp_ticker_long, pos_hold_long = get_current_position(cur_pos, vt_symbol_td, 'LONG')
                num_slot_new = self._cal_num_slots(vt_symbol_td)

                if str(pos_hold_long) != '0':
                    # sell current position and create new buy order
                    write_log("Roll long position")
                    num_slot_sell = str(int(pos_hold_long))
                    self._action_sell(num_slot_sell, vt_symbol_yestd)
                    self._action_buy(num_slot_new, vt_symbol_td)

                elif str(pos_hold_short) != '0':
                    write_log("Roll short position")
                    num_slot_cover = str(int(pos_hold_short))
                    self._action_cover(num_slot_cover, vt_symbol_yestd)
                    self._action_short(num_slot_new, vt_symbol_td)

            self._finalize_single(ticker, str(datetime.now()), ST_NAME + ' rolls')

        sync_routine(self.trade_engine, self.db_engine, self.engine_intern, self.engine_signal)
        self.orders.clear()


# how to run
if __name__ == '__main__':
    trader = Trader()
    trader.roll_contracts()
    trader.trade(ticker='ABCDE', latest_signal=1, num_slot='1')
