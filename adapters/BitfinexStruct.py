# encoding: UTF-8

import sys

sys.path.append("../")
import trader.coinDataStruct
import trader.coinUtilities
import trader.defines
import time

########################################################################
class Bitfinex_depth_Data(trader.coinDataStruct.coinDepthData):
    def __init__(self):
        super(Bitfinex_depth_Data, self).__init__()
        self.bids = {}
        self.asks = {}

    def dataIsReady(self):
        return not self.LastPrice is None

    def initSpotDepthData(self, symbol, data, apiType):
        self.isSpot             =   True
        self.API                =   apiType
        self.symbol             =   symbol

    def updateLastPrices(self, lastPrice, volume, bid, bidVol, ask, askVol):
        self.remoteTimeStamp = int(time.time())
        self.LastPrice = lastPrice
        self.Volume = volume
        self.BidPrice = bid
        self.BidVolume = bidVol
        self.AskPrice = ask
        self.AskVolume = askVol

    def updateOrderBook(self, updateInfo):
        self.remoteTimeStamp = int(time.time())
        for iInfo in updateInfo:
            if isinstance(iInfo, (list, tuple)):
                # find status
                if iInfo[1] == 0:
                    # remove exsiting order
                    if iInfo[2] == 1:
                        # remove from bid
                        del self.bids[iInfo[0]]
                    elif iInfo[2] == -1:
                        # remove from ask
                        del self.asks[iInfo[0]]
                    else:
                        print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                              "get undefined info for order book", updateInfo)
                else:
                    # update new order in list
                    if iInfo[2] > 0:
                        # update bid
                        self.bids[iInfo[0]] = iInfo[2]
                    else:
                        # update ask
                        self.asks[iInfo[0]] = iInfo[2]
            else:
                # updateInfo itself only
                # find status
                if updateInfo[1] == 0:
                    # remove exsiting order
                    if updateInfo[2] == 1:
                        # remove from bid
                        del self.bids[updateInfo[0]]
                    elif updateInfo[2] == -1:
                        # remove from ask
                        del self.asks[updateInfo[0]]
                    else:
                        print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                              "get undefined info for order book", updateInfo)
                else:
                    # update new order in list
                    if updateInfo[2] > 0:
                        # update bid
                        self.bids[updateInfo[0]] = updateInfo[2]
                    else:
                        # update ask
                        self.asks[updateInfo[0]] = updateInfo[2]
                break

        # update depth data
        bidList = [(k, self.bids[k]) for k in sorted(self.bids.keys())]
        askList = [(k, self.asks[k]) for k in sorted(self.asks.keys())]
        bidSize = len(bidList)
        askSize = len(askList)
        self.Asks = []
        self.AskVolumes = []
        self.Bids = []
        self.BidVolumes = []
        if bidSize > 0 and askSize > 0:
            self.BidPrice = float(bidList[-1][0])
            self.BidVolume = float(bidList[-1][1]) / self.BidPrice
            self.AskPrice = float(askList[0][0])
            self.AskVolume = float(askList[0][1]) / self.AskPrice

            for i in reversed(range(bidSize)):
                self.Bids.append(float(bidList[i][0]))
                self.BidVolumes.append(float(bidList[i][1]) / float(bidList[i][0]))
            for i in range(askSize):
                self.Asks.append(float(askList[i][0]))
                self.AskVolumes.append(float(askList[i][1]) / float(askList[i][0]))

########################################################################


