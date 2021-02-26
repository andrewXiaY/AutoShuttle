"""
定期计算信号

signal = ptf_manager.run()
{
    'type': [trade, roll contract],
    'signal': [(ticker, latest_signal, num_slot), None]
}

level: Z4
"""
from datetime import timedelta, datetime
from orgnization.analyst import initialize_trading_engine, send_dd_msg, sync_routine, write_log, sys_monitoring
from sqlalchemy import create_engine
from vnpy.gateway.ctp import CtpGateway
from configuration import Trades_DB_ADD, Intern_DB_ADD, Signal_DB_ADD
from constants import ALL_SYMBOLS, FWD_BARS, FWD_BARS_LASSO, NUM_ZS_LASSO, \
    NUM_ZS, ALL_TICKERS, NS_LASSO, NS, ALPHA_LASSO, INTRADAY_DATA_PATH, ST_NAME
from orgnization.associate import start_ipycluster, get_models, is_trading_period, is_lunch_break, \
    day_trade_finished, is_rolling_contracts, extract_dataframes, get_dataframes, signal_calculation_unit, \
    signal_calculation_analyst
import pandas as pd
from jqdatasdk import get_price
import os
import signal
import time

ROLL_CONTRACT = 'Roll Contracts'
TRADE = 'TRADE'


class PtfManager:
    def __init__(self, gateway=CtpGateway):
        self.trade_engine = initialize_trading_engine(gateway)
        self.gateway = gateway

        self.db_engine = create_engine(Trades_DB_ADD)
        self.engine_intern = create_engine(Intern_DB_ADD)
        self.engine_signal = create_engine(Signal_DB_ADD)

        self.view = start_ipycluster()

        self.models = []
        self.models_lasso = []

    def _load_models(self):
        self.models = [get_models(ALL_SYMBOLS[i], FWD_BARS[i], NUM_ZS[i], NS[i]) for i in range(len(ALL_TICKERS))]
        self.models_lasso = [get_models(ALL_SYMBOLS[i], FWD_BARS_LASSO[i], NUM_ZS_LASSO[i],
                                        NS_LASSO[i], ALPHA_LASSO[i]) for i in range(len(ALL_TICKERS))]

    def _save_signal_database_routine(self, sig_df, current_time, ticker, symbol):
        db_last = pd.read_sql(f"select max(tradeTime) from Live where symbol = '{symbol}'", self.db_engine).values[0][0]
        sig_df.index = pd.to_datetime(sig_df.index)
        sig_df = sig_df.sort_index()
        sig_df_save = sig_df[db_last:].iloc[1:, :]
        sig_df_save.index.names = ['tradeTime']
        if len(sig_df_save) > 0:
            sig_df_save.to_sql('Live', self.db_engine, if_exists='append')
            sig_df_save.to_sql('live_signals', self.engine_signal, if_exists='append')
            print(current_time, ' ', ticker, ' ', ' Save to DB.\n')
            if sig_df['signal'].diff().values[-1] != 0:
                send_dd_msg(ticker, str(sig_df['signal'].values[-1]))
        else:
            print('No save needed.\n')

    def _save_price_database_routine(self, dfs_all):
        for i in range(len(ALL_TICKERS)):
            db_last = pd.read_sql(f"select max(date) from cmdty_price_intraday where ticker = '{str(ALL_TICKERS[i])}'",
                                  self.engine_intern).values[0][0]
            df_p_save = dfs_all[i]
            df_p_save.index = pd.to_datetime(df_p_save.index)
            df_p_save = df_p_save[db_last:].iloc[1:, :]
            if len(df_p_save) > 0:
                df_p_save.index.names = ['date']
                df_p_save['symbol'] = ALL_TICKERS[i][:-9]
                df_p_save['ticker'] = ALL_TICKERS[i]
                df_p_save.to_sql('cmdty_price_intraday', con=self.engine_intern, if_exists='append')

    def _day_trade_ending_routine(self, current_time):
        print(current_time, 'Finished day-trade.')
        for i in range(len(ALL_TICKERS)):
            df_input = get_price(ALL_TICKERS[i],
                                 start_date='2005-01-01',
                                 end_date=current_time.date() + timedelta(days=1),
                                 frequency='15m').dropna()
            df_input[:current_time].to_csv(INTRADAY_DATA_PATH + str(ALL_SYMBOLS[i]) + '_pre.csv')

        sync_routine(self.trade_engine, self.db_engine, self.engine_intern, self.engine_signal)

        self.trade_engine.close()
        time.sleep(3)
        os.kill(os.getpid(), signal.SIGTERM)

    def _lunch_break_routine(self):
        print('Lunch Break')
        sys_monitoring(st_name=ST_NAME, engine_intern=self.engine_intern)
        time.sleep(10)

    def _run_model(self, dfs_all):
        result = self.view.map(signal_calculation_unit, dfs_all, self.models, NUM_ZS, NS)
        # add lasso model signals
        result_lasso = self.view.map(signal_calculation_unit, dfs_all, self.models_lasso, NUM_ZS_LASSO,
                                     NS_LASSO, ['lasso'] * len(NS_LASSO))
        return result.get(), result_lasso.get()

    def _calculate_signal_routine(self, current_time, c_h):
        sig = {}
        dff_gp = get_dataframes()
        dfs, dfs_all = extract_dataframes(dff_gp, current_time)

        # 计算信号 -> 交易 -> 存储
        if len(dfs[0][:current_time]) > 0 or c_h in ['9']:
            r_list, r_list_lasso = self._run_model(dfs_all)
            # loop over tickser - trade and save DB
            current_time = datetime.now()
            for idx, ticker in enumerate(ALL_TICKERS):
                signal_change, latest_signal, sig_df = \
                    signal_calculation_analyst(r_list[idx], r_list_lasso[idx], current_time, ticker)

                if signal_change != 0:
                    sig[ticker] = {'type': TRADE, 'signal': (latest_signal, '1')}

                self._save_signal_database_routine(sig_df, current_time, ticker, ALL_SYMBOLS[idx])
        self._save_price_database_routine(dfs_all)
        return sig

    def run(self, current_time):
        sig = {}

        c_h, c_m, c_s = str(current_time.hour), str(current_time.minute), str(current_time.second)
        if is_rolling_contracts(c_h, c_m):
            sig['type'] = ROLL_CONTRACT
            sig['signal'] = None

        if is_trading_period(c_h, 'hour'):
            if is_trading_period(c_m, 'minute'):
                if is_lunch_break(c_h, c_m):
                    self._lunch_break_routine()
                else:
                    sig = self._calculate_signal_routine(current_time, c_h)
                    sync_routine(self.trade_engine, self.db_engine, self.engine_intern, self.engine_signal)
                    time.sleep(60)
            else:
                time.sleep(10)
                sync_routine(self.trade_engine, self.db_engine, self.engine_intern, self.engine_signal)

        elif day_trade_finished(c_h, c_m):
            self._day_trade_ending_routine(current_time)
        # elif c_m[-2:] in ['59', '14', '29', '44']:
        #     if c_s in ['58', '59']:
        #         time.sleep(0.1)
        #     else:
        #         time.sleep(0.5)
        #     sync_routine(self.trade_engine, self.db_engine, self.engine_intern, self.engine_signal)
        else:
            if c_s in ['58', '59']:
                time.sleep(0.1)
            else:
                time.sleep(0.5)
            sync_routine(self.trade_engine, self.db_engine, self.engine_intern, self.engine_signal)

        return sig
