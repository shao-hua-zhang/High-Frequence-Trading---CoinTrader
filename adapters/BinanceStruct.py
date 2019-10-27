# encoding: UTF-8

import sys
sys.path.append("../")
import time
import trader.coinDataStruct
import trader.defines
import trader.coinUtilities
from .binance.depthcache import DepthCache

########################################################################
class Binance_depth_Data(trader.coinDataStruct.coinDepthData):
    def __init__(self):
        super(Binance_depth_Data, self).__init__()

    def initSpotDepthData(self, symbol, data, apiType):
        self.isSpot             =   True
        self.API                =   apiType
        self.remoteTimeStamp    =   int(time.time())
        self.symbol             =   symbol


        bidList = data.get_bids()
        askList = data.get_asks()

        # print(bidList)
        # print(askList)
        if len(bidList) > 0 and len(askList) > 0:
            self.BidPrice = float(bidList[0][0])
            self.BidVolume = float(bidList[0][1])
            self.AskPrice = float(askList[0][0])
            self.AskVolume = float(askList[0][1])
            self.Bids = [bid[0] for bid in bidList]
            self.BidVolumes = [bid[1] for bid in bidList]
            self.Asks = [ask[0] for ask in askList]
            self.AskVolumes = [ask[1] for ask in askList]


        '''
        bidSize = len(data["b"])
        askSize = len(data["a"])
        if bidSize > 0 and askSize > 0:
            self.BidPrice = float(data["b"][0][0])
            self.BidVolume = float(data["b"][0][1])
            self.AskPrice = float(data["a"][0][0])
            self.AskVolume = float(data["a"][0][1])

            for i in range(bidSize):
                self.Bids.append(float(data["b"][i][0]))
                self.BidVolumes.append(float(data["b"][i][1]))
            for i in range(askSize):
                self.Asks.append(float(data["a"][i][0]))
                self.AskVolumes.append(float(data["a"][i][1]))
        '''


########################################################################


