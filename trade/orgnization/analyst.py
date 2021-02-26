"""
trader的utility, level: Z1
最基本的utility都在这里
"""
import sys
sys.path.append('..')
import json
from vnpy.app.script_trader import init_cli_trading
from jqdatasdk import get_price, get_dominant_future, auth
import pandas as pd
from trade.constants import LOG_FILE_NAME, ST_NAME
from trade.configuration import Auth_ID, Auth_Secret, ctp_setting
from datetime import datetime
import requests
import time

if Auth_ID:
    auth(Auth_ID, Auth_Secret)


def initialize_trading_engine(gateway, setting=ctp_setting):
    """
    初始化行情交易接口
    TODO: 明确参数变量含义
    :param gateway:
    :param setting:
    :return: engine
    """
    trading_engine = init_cli_trading([gateway])
    trading_engine.connect_gateway(setting, "CTP")
    time.sleep(5)
    return trading_engine


def write_log(txt, log_file=LOG_FILE_NAME):
    print(txt)
    with open(log_file, 'a') as f:
        print(txt, file=f)


def get_roll_v2(ticker, engine_intern):
    symbol = ticker[:-9]
    trading_dates = list(get_price(ticker, start_date='2020-01-01', end_date=datetime.now().date()).index)
    df_active_ctr = pd.read_sql("select * from active_ctr where symbol = '" + symbol + "'",
                                engine_intern).set_index('date')
    df_active_ctr.index = pd.to_datetime(df_active_ctr.index)
    df_active_ctr = df_active_ctr[df_active_ctr.index.isin(trading_dates)]
    today_ctr = df_active_ctr['active_ctr'].values[-1]
    yes_ctr = df_active_ctr['active_ctr'].values[-2]

    if today_ctr != yes_ctr:
        return [yes_ctr, today_ctr]
    else:
        return [today_ctr]


def ticker_jq2vt(jq_: str, dt=None):
    """
    jq ticker code to vt ticker code
    :param jq_: jq ticker code
    :param dt: date
    :return: vt ticker code
    """

    td_sym = get_dominant_future(jq_[:-9], dt)[:-9] if dt else jq_[:-9]

    if jq_.endswith('XDCE'):
        return td_sym.lower() + td_sym[-9:-5] + '.DCE'
    elif jq_.endswith('XSGE'):
        return td_sym.lower() + td_sym[-9:-5] + '.SHFE'
    elif jq_.endswith('XZCE'):
        return td_sym + td_sym[-8:-5] + '.CZCE'
    elif jq_.endswith('CCFX'):
        return td_sym + td_sym[-9:-5] + '.CFFEX'
    else:
        raise KeyError("Unknown jq ticker code")


def round_price_tick(price, price_tick):
    """
    取整价格到合约最小价格变动
    :param price:
    :param price_tick:
    :return:
    """
    return round(price / price_tick, 0) * price_tick


def ctp_ticker_to_symbol(ticker: str):
    """
    ctp ticker to symbol

    :param ticker: ticker
    :return: symbol
    """
    if ticker.endswith('SHFE'):
        return ticker[:-9]
    elif ticker.endswith('CFFEX'):
        return ticker[:-10]
    else:
        return ticker[:-8]


def get_current_position(cur_pos, ctp_ticker, direction):
    """
    get current ctp ticker and position
    """
    if ctp_ticker[-4:] == 'SHFE' or ctp_ticker[-5:] == 'CFFEX':
        pos = cur_pos[(cur_pos['symbol_simple'] == ctp_ticker[:-9]) & (cur_pos['direction_str'] == direction)]
        if len(pos) == 0:
            return '_', 0
        else:
            return pos['vt_symbol'].values[0], pos['volume'].sum()
    else:
        pos = cur_pos[(cur_pos['symbol_simple'] == ctp_ticker[:-8]) & (cur_pos['direction_str'] == direction)]
        if len(pos) == 0:
            return '_', 0
        else:
            return pos['vt_symbol'].values[0], pos['volume'].sum()


def dingmessage(tex, hk):
    webhook = hk
    header = {
        "Content-Type": "application/json",
        "Charset": "UTF-8"
    }
    message ={
        "msgtype": "text",
        "text": {
            "content": tex
        },
        "at": {
            "isAtAll": True
        }
    }
    message_json = json.dumps(message)
    info = requests.post(url=webhook,data=message_json,headers=header)


def send_dd_msg(ticker, signal):
    text = ''
    text += "ML Model Signal: \n"
    text += "Symbol : "+str(ticker)+"\n"
    text += "Action : "+str(signal)+"\n"
    text += "Time : "+str(datetime.now())+"\n"
    hk = "https://oapi.dingtalk.com/robot/send?access_token=" \
         "76fabadb5eddb5067df6ecadc5d7e61353a8cac32d04bd67fc0b4beefce90d1b"
    dingmessage(text, hk)
    print("Sent DD message.")


def sys_monitoring(st_name, engine_intern):
    df_signal_check = pd.DataFrame(index=[datetime.now()])
    df_signal_check['st'] = st_name
    df_signal_check['check'] = 1
    df_signal_check.index.names = ['date']
    df_signal_check.index = pd.to_datetime(df_signal_check.index)
    df_signal_check.to_sql('monitor', con=engine_intern, if_exists='append')


def insert_balance(engine, nav_engine):
    try:
        x = engine.get_all_accounts()
        x = pd.DataFrame(index=[pd.to_datetime(datetime.now())], data = {'balance': [x[0].balance], 'sim_id': [2]})
        x.index.names = ['date']
        x.to_sql('nav', con = nav_engine, if_exists='append')
    except:
        pass


# Sync routine
def sync_routine(engine, db_engine, engine_intern, engine_signal):
    """
    暂时不知道是干什么的
    :param engine:
    :param db_engine:
    :param engine_intern:
    :param engine_signal:
    :return:
    """
    insert_balance(engine=engine, nav_engine=engine_signal)
    insert_balance(engine=engine, nav_engine=db_engine)
    sys_monitoring(st_name=ST_NAME, engine_intern=engine_intern)