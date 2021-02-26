# 交易所代码

LOG_FILE_NAME = 'C:/Users/Administrator/Desktop/股指/log/log.txt'

PARENT_PATH = 'C:/Users/Administrator/Desktop/股指'
INTRADAY_DATA_PATH = 'C:/Users/Administrator/Desktop/股指/intraday_data/'
GET_PRICE_START_DAY = '2020-01-01'

ALL_TICKERS = ['IC9999.CCFX', 'IF9999.CCFX', 'IH9999.CCFX']
ALL_SYMBOLS = ['IC', 'IF', 'IH']
ST_NAME = 'Live - 2'
CMD_NAV = (1000000 * 2 / len(ALL_TICKERS))

#  普通模型参数
FWD_BARS = [30, 35, 35]
NUM_ZS = [200, 45, 50]
NS = [80, 175, 10]

#  lasso参数
FWD_BARS_LASSO = [5, 5, 3]
NUM_ZS_LASSO = [100, 200, 100]
ALPHA_LASSO = [10, 1, 100]
NS_LASSO = [15, 5, 10]
