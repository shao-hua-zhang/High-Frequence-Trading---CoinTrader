# encoding: UTF-8

import sys

sys.path.append("../")
import trader.coinDataStruct
import trader.defines


# Okex常量结构体
class OkexConstants(object):
    def __init__(self):

        # 合约货币代码
        self.SYMBOL_BTC = 'btc'
        self.SYMBOL_LTC = 'ltc'
        self.SYMBOL_ETH = 'eth'
        self.SYMBOL_ETC = 'etc'
        self.SYMBOL_BCH = 'bch'

        # 期货合约到期类型
        self.FUTURE_EXPIRY_THIS_WEEK = 'this_week'
        self.FUTURE_EXPIRY_NEXT_WEEK = 'next_week'
        self.FUTURE_EXPIRY_QUARTER = 'quarter'

        # 行情深度
        self.DEPTH_5 = 5
        self.DEPTH_10 = 10
        self.DEPTH_20 = 20

        # K线时间区间
        self.INTERVAL_1M = '1min'
        self.INTERVAL_3M = '3min'
        self.INTERVAL_5M = '5min'
        self.INTERVAL_15M = '15min'
        self.INTERVAL_30M = '30min'
        self.INTERVAL_1H = '1hour'
        self.INTERVAL_2H = '2hour'
        self.INTERVAL_4H = '4hour'
        self.INTERVAL_6H = '6hour'
        self.INTERVAL_1D = 'day'
        self.INTERVAL_3D = '3day'
        self.INTERVAL_1W = 'week'

        # 期货委托类型
        self.FUTURE_TYPE_LONG = 1
        self.FUTURE_TYPE_SHORT = 2
        self.FUTURE_TYPE_SELL = 3
        self.FUTURE_TYPE_COVER = 4

        # 期货是否用对手价
        self.FUTURE_ORDER_MATCH = 1
        self.FUTURE_ORDER_NO_MATCH = 0

        # 期货杠杆
        self.FUTURE_LEVERAGE_10 = 10
        self.FUTURE_LEVERAGE_20 = 20

        # 委托状态
        self.ORDER_STATUS_NOTTRADED = 0
        self.ORDER_STATUS_PARTTRADED = 1
        self.ORDER_STATUS_ALLTRADED = 2
        self.ORDER_STATUS_CANCELLED = -1
        self.ORDER_STATUS_CANCELLING = 4

########################################################################
class OK_depth_Data(trader.coinDataStruct.coinDepthData):
    def __init__(self):
        super(OK_depth_Data, self).__init__()

    def initSpotDepthData(self, symbol, data, apiType):

        self.isSpot             =   True
        self.API                =   apiType
        self.remoteTimeStamp    =   int(data['timestamp'])
        self.symbol             =   symbol

        bidSize = len(data['bids'])
        askSize = len(data['asks'])
        self.AskPrice = float(data['asks'][askSize - 1][0])
        self.BidPrice = float(data['bids'][0][0])
        self.AskVolume = float(data['asks'][askSize - 1][1])
        self.BidVolume = float(data['bids'][0][1])
        for i in range(bidSize):
            self.Bids.append(float(data['bids'][i][0]))
            self.BidVolumes.append(float(data['bids'][i][1]))
        for i in reversed(range(askSize)):
            self.Asks.append(float(data['asks'][i][0]))
            self.AskVolumes.append(float(data['asks'][i][1]))

    def initFutureDepthData(self, symbol, expiry, data, apiType):

        self.isSpot             =   False
        self.API                =   apiType
        self.remoteTimeStamp    =   int(data['timestamp'])
        self.symbol             =   symbol
        self.expiry             =   expiry

        bidSize = len(data['bids'])
        askSize = len(data['asks'])
        self.AskPrice = float(data['asks'][askSize - 1][0])
        self.BidPrice = float(data['bids'][0][0])
        self.AskVolume = float(data['asks'][askSize - 1][1])
        self.BidVolume = float(data['bids'][0][1])
        for i in range(bidSize):
            self.Bids.append(float(data['bids'][i][0]))
            self.BidVolumes.append(float(data['bids'][i][1]))
        for i in reversed(range(askSize)):
            self.Asks.append(float(data['asks'][i][0]))
            self.AskVolumes.append(float(data['asks'][i][1]))

########################################################################


