# encoding: UTF-8

import time


# depth data
class coinDepthData(object):
    def __init__(self):
        self.isSpot = True
        self.API = None
        self.localTimeStamp = time.time()
        self.remoteTimeStamp = self.localTimeStamp
        self.symbol = ""
        self.expiry = "None"
        self.BidPrice = None
        self.BidVolume = None
        self.AskPrice = None
        self.AskVolume = None
        self.LastPrice = None
        self.Volume = None

        self.Asks = []
        self.AskVolumes = []
        self.Bids = []
        self.BidVolumes = []

    def printDepthData(self):
        print(self.symbol, self.expiry, self.AskPrice, self.BidPrice, self.LastPrice, self.Volume)
        print("asks", len(self.Asks))
        print("bids", len(self.Bids))


