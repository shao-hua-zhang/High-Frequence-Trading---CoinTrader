# encoding: UTF-8

import sys

sys.path.append("../")
import trader.coinDataStruct
import trader.defines

########################################################################
class Dgf_depth_Data(trader.coinDataStruct.coinDepthData):
    def __init__(self):
        super(Dgf_depth_Data, self).__init__()

    def initSpotDepthData(self, symbol, data, apiType):
        self.isSpot             =   True
        self.API                =   apiType
        self.remoteTimeStamp    =   int(data['date'])
        self.symbol             =   symbol

        bidSize = len(data['bids'])
        askSize = len(data['asks'])

        data["asks"].reverse()

        self.AskPrice = float(data['asks'][0][0])
        self.BidPrice = float(data['bids'][0][0])
        self.AskVolume = float(data['asks'][0][1])
        self.BidVolume = float(data['bids'][0][1])

        for i in range(bidSize):
            self.Bids.append(float(data['bids'][i][0]))
            self.BidVolumes.append(float(data['bids'][i][1]))
        for i in range(askSize):
            self.Asks.append(float(data['asks'][i][0]))
            self.AskVolumes.append(float(data['asks'][i][1]))
########################################################################


