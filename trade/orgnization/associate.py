"""
manager的utility, level: Z2
"""
import os
import sys
import pickle
from ..constants import PARENT_PATH
import pandas as pd
from datetime import datetime, timedelta
from IPython.parallel import Client
from jqdatasdk import get_price
from ..constants import INTRADAY_DATA_PATH, ALL_SYMBOLS, ALL_TICKERS
from ..orgnization.analyst import write_log
cwd = os.getcwd()


# 根据模型计算信号
def signal_calculation_unit(df_input, model, num_z, n, mode=None, cwd=cwd):
    """
    根据模型计算信号
    TODO: 明确参数变量含义
    :param df_input:
    :param model:
    :param num_z:
    :param n:
    :param mode:
    :param cwd:
    :return: signal
    """
    sys.path.append(cwd)
    import quant as q
    if mode == 'lasso':
        return q.live_cal_lasso(df_input, model, num_z, n)
    else:
        return q.live_cal(df_input, model, num_z, n)


def start_ipycluster():
    """
    初始化ipython clusters
    :return:
    """
    return Client().load_balanced_view()


# 模型名字可以规定一个模式，这样方便以后扩展和精简代码
def get_single_model(symbol, fwd_bar, num_z, n, alpha=None):
    if alpha:
        model_name = PARENT_PATH + '/model_lasso/' + symbol + '_' + str(fwd_bar) + '_' + str(num_z) + \
                     '_' + str(alpha) + '_' + str(n) + '.pkl'
    else:
        model_name = PARENT_PATH + '/model/' + symbol + '_' + str(num_z) + '_' + str(n) + '_' + str(fwd_bar) + \
                     '_20200420.model'
    # load model
    f = open(model_name, 'rb')
    s = f.read()
    model = pickle.loads(s)

    return model


def get_models(symbols, fwd_bars, num_zs, ns, alphas=None):
    if alphas:
        return [get_single_model(symbols[i], fwd_bars[i], num_zs[i], ns[i], alphas[i]) for i in range(len(symbols))]
    else:
        return [get_single_model(symbols[i], fwd_bars[i], num_zs[i], ns[i]) for i in range(len(symbols))]


def get_latest_data_of_ticker(current_time, symbol, dff):
    """
    获取最新数据
    TODO: 明确参数变量含义
    :param current_time:
    :param symbol:
    :param dff:
    :return:
    """
    df = pd.read_csv(INTRADAY_DATA_PATH + symbol + '_pre.csv', index_col=0).append(dff[:current_time])
    df.to_csv(INTRADAY_DATA_PATH + symbol + '_latest.csv')
    print(symbol, ' Save at ', current_time)
    return df.tail(2000)


def get_previous_data_of_ticker(current_time, symbol):
    """
    获取上一次数据
    TODO: 明确参数变量含义
    :param current_time:
    :param symbol:
    :return:
    """
    df = pd.read_csv(INTRADAY_DATA_PATH + symbol + '_pre.csv', index_col=0)
    df.to_csv(INTRADAY_DATA_PATH + symbol + '_latest.csv')
    print(symbol, ' Use Pre data at ', current_time)
    return df.tail(2000)


def get_dataframes():
    current_time = datetime.now()
    print('\n' + str(current_time.time()) + '\n')
    dff = get_price(ALL_TICKERS, start_date=current_time.date(), end_date=current_time.date() + timedelta(days=1),
                    frequency='15m')
    dff_gp = dff.groupby('code')
    return dff_gp


def extract_dataframes(dff_gp, current_time):
    """
    生成计算信号所需要的dataframe
    TODO: 明确参数变量含义
    :param dff_gp:
    :param current_time:
    :return:
    """
    dfs, dfs_all = [], []

    for idx, ticker in enumerate(ALL_TICKERS):
        dff = dff_gp.get_group(ticker).set_index('time')
        del dff['code']
        dfs.append(dff.copy())

        if len(dff[:current_time]) > 0:
            latest_data = get_latest_data_of_ticker(current_time, ALL_SYMBOLS[idx])
            dfs_all.append(latest_data)
        else:
            previous_data = get_previous_data_of_ticker(current_time, ALL_SYMBOLS[idx])
            dfs_all.append(previous_data)
    return dfs, dfs_all


def signal_calculation_analyst(sig_df_n, sig_df_lasso, current_time, ticker):
    sig_df_combine = pd.concat([sig_df_n.tail(20), sig_df_lasso.tail(20)], axis=1)
    sig_df = sig_df_combine.mean(axis=1)

    sig_df_print = pd.concat([sig_df_combine, sig_df.to_frame('AGG')], axis=1)
    sig_df_print.columns = ['NL', 'L', 'AGG']
    txt_ = "#########" + ticker + str(sig_df_print.tail(5))
    write_log(txt_)

    sig_df = sig_df.to_frame('signal')
    sig_df['symbol'] = ticker
    sig_df['sim_id'] = 2
    sig_df['insert_time'] = current_time
    sig_df.index = pd.to_datetime(sig_df.index)

    signal_change = sig_df['signal'].diff().tail(1).values[0]
    latest_signal = sig_df['signal'].tail(1).values[0]
    return signal_change, latest_signal, sig_df


def is_trading_period(t, granularity='hour'):
    """
    判断是否在允许交易的时间段
    :param t: 时间
    :param granularity:
    :return:
    """
    if granularity == 'hour':
        return t in ['9', '10', '11', '12', '13', '14', '15']
    elif granularity == 'minute':
        return t[-2:] in ['0', '15', '30', '45']
    else:
        raise ValueError("Granularity should be among 'hour', 'minute' or 'second'")


def is_rolling_contracts(c_h, c_m):
    return c_h in ['9'] and c_m[-2:] in ['35']


def day_trade_finished(h, m):
    """
    判断每天交易是否结束
    :param h: hour
    :param m: minute
    :return: boolean
    """
    return h in ['16'] and m[-2:] in ['0', '15']


def is_lunch_break(h, m):
    """
    判断是否午休
    :param h: hour
    :param m: minute
    :return: boolean
    """
    return (h in ['11'] and m in ['45']) or (h in ['12'] and m in ['0', '15', '30', '45'])