# encoding: UTF-8

import sys
import time
from copy import deepcopy

sys.path.append("../")
import trader.coinDataStruct
import trader.defines

########################################################################
class Bitmex_depth_Data(trader.coinDataStruct.coinDepthData):
    def __init__(self):
        super(Bitmex_depth_Data, self).__init__()

    def initFutureDepthData(self, data, expiry, apiType):

        # copy
        # print(data)
        idata = deepcopy(data)
        asks = [[ask["price"], ask["size"]] for ask in idata if ask["side"]== "Sell"]
        bids = [[buy["price"], buy["size"]] for buy in idata if buy["side"]== "Buy"]
        asks = sorted(asks, key=lambda x:x[0])
        bids = sorted(bids, key=lambda x:x[0], reverse=True)

        self.isSpot             =   False
        self.API                =   apiType
        self.expiry = expiry
        # orderbook has no timestamp
        self.remoteTimeStamp    =   int(time.time()*1000)
        self.symbol             =   idata[0]["symbol"]

        bidSize = len(bids)
        askSize = len(asks)
        if bidSize > 0 and askSize > 0:
            self.BidPrice = bids[0][0]
            self.BidVolume = bids[0][1]
            self.AskPrice = asks[0][0]
            self.AskVolume = asks[0][1]

            for i in range(bidSize):
                self.Bids.append(bids[i][0])
                self.BidVolumes.append(bids[i][1])
            for i in range(askSize):
                self.Asks.append(asks[i][0])
                self.AskVolumes.append(asks[i][1])
########################################################################


