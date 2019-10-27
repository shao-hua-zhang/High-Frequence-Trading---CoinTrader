# encoding: UTF-8

import sys

sys.path.append("../")
import trader.coinDataStruct
import trader.defines

########################################################################
class Huobi_depth_Data(trader.coinDataStruct.coinDepthData):
    def __init__(self):
        super(Huobi_depth_Data, self).__init__()

    def initSpotDepthData(self, symbol, data, apiType):
        self.isSpot             =   True
        self.API                =   apiType
        self.remoteTimeStamp    =   int(data['ts'])
        self.symbol             =   symbol

        bidSize = len(data["tick"]['bids'])
        askSize = len(data["tick"]['asks'])
        self.AskPrice = float(data["tick"]['asks'][0][0])
        self.BidPrice = float(data["tick"]['bids'][0][0])
        self.AskVolume = float(data["tick"]['asks'][0][1])
        self.BidVolume = float(data["tick"]['bids'][0][1])
        for i in range(bidSize):
            self.Bids.append(float(data["tick"]['bids'][i][0]))
            self.BidVolumes.append(float(data["tick"]['bids'][i][1]))
        for i in range(askSize):
            self.Asks.append(float(data["tick"]['asks'][i][0]))
            self.AskVolumes.append(float(data["tick"]['asks'][i][1]))

    def initFutureDepthData(self, symbol, expiry, data, apiType):
        self.isSpot = False
        self.API = apiType
        self.remoteTimeStamp = int(data['ts'])
        self.symbol = symbol
        self.expiry = expiry

        bidSize = len(data['bids'])
        askSize = len(data['asks'])
        self.AskPrice = float(data['asks'][0][0])
        self.BidPrice = float(data['bids'][0][0])
        self.AskVolume = int(data['asks'][0][1])
        self.BidVolume = int(data['bids'][0][1])
        for i in range(bidSize):
            self.Bids.append(float(data['bids'][i][0]))
            self.BidVolumes.append(float(data['bids'][i][1]))
        for i in (range(askSize)):
            self.Asks.append(float(data['asks'][i][0]))
            self.AskVolumes.append(float(data['asks'][i][1]))
########################################################################


