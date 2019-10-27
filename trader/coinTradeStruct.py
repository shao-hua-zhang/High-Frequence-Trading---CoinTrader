# encoding: UTF-8
import time
import sys
import threading

sys.path.append("../")
import trader.defines

# order send out
class coinOrder(object):
    def __init__(self):
        self.isSpot = True
        self.isCancel = False
        self.API = None
        self.price = 0
        self.symbol = ""
        self.expiry = "None"
        self.amount = 0.
        self.orderType = trader.defines.coverLong_limitPrice
        self.orderID = -1
        self.orderCreateTime = time.time()
        self.orderUpdateTime = -1
        self.filledAmount = 0.0
        self.orderStatus = trader.defines.orderInit
        self.leverage = 1
        self.strategyTag = ""
        self.fillPrice = 0.0
        self.isOrderBackOnly = False

    # set order to cancel
    def setCancelOrder(self):
        self.isCancel = True
    # query order is cancelling or not
    def isCancelling(self):
        return self.isCancel
    # query order is finished
    def isFinishedOrder(self):
        return self.orderStatus >= trader.defines.orderFilled
    # set order status
    def updateOrderStatus(self, status):
        if status > self.orderStatus:
            self.orderStatus = status
    # set filled amount
    def updateFilledAmount(self, filledAmount):
        if filledAmount > self.filledAmount:
            self.filledAmount = filledAmount
            if self.filledAmount >= self.amount:
                self.orderStatus = trader.defines.orderFilled
    # query filled amount
    def getFilledAmount(self):
        return self.filledAmount
    # query filled price
    def getFilledPrice(self):
        return self.fillPrice
    # query current order status
    def getOrderStatus(self):
        return self.orderStatus
    # query strategyTag
    def getStrategyTag(self):
        return self.strategyTag
    # query API name
    def getAPI_name(self):
        return self.API
    # set orderID
    def setOrderID(self, orderID):
        self.orderID = orderID
    # init spot order, generate in strategy
    def iniSpotOrder(self, symbol, price, amount, orderType, apiType, strategyTag):
        self.API = apiType
        self.isSpot = True
        self.price = price
        self.symbol = symbol
        self.amount = amount
        self.orderType = orderType
        self.strategyTag = strategyTag
        # print(self.orderType)
    # set spot order feedback, generate in api
    def setSpotOrderFeedback(self, data, isOrderBackOnly = True):
        self.API = data["apiType"]
        self.isSpot = True
        self.symbol = data["symbol"]
        self.orderID = data["orderID"]
        self.orderStatus = data["status"]
        self.orderUpdateTime = data["orderUpdateTime"]
        self.strategyTag = data["strategyTag"]
        if isOrderBackOnly:
            self.filledAmount = data["deal_amount"]
            self.isOrderBackOnly = True
            self.fillPrice = data["fillPrice"]
    # init future order, generate in strategy
    def iniFutureOrder(self, symbol, expiry, price, amount, orderType, \
                       leverage, apiType, strategyTag):
        self.API = apiType
        self.isSpot = False
        self.price = price
        self.expiry = expiry
        self.symbol = symbol
        self.amount = amount
        self.orderType = orderType
        self.leverage = leverage
        self.strategyTag = strategyTag
    # set future order feedback, generate in api
    def setFutureOrderFeedback(self, data, isOrderBackOnly = True):
        self.API = data["apiType"]
        self.isSpot = False
        self.symbol = data["symbol"]
        self.expiry = data["expiry"]
        self.orderID = data["orderID"]
        self.orderStatus = data["status"]
        self.orderUpdateTime = data["orderUpdateTime"]
        self.strategyTag = data["strategyTag"]
        if isOrderBackOnly:
            self.filledAmount = data["deal_amount"]
            self.isOrderBackOnly = True
            self.fillPrice = data["fillPrice"]

    # prinf order info
    def printOrderInfo(self):
        return self.symbol + ", " + self.expiry + ", " + str(self.amount) + ", " + str(self.price) + ", " + \
    str(self.orderType) + ", " + str(self.leverage) + ", " + str(self.strategyTag) + ", " + str(self.orderID) + \
    ", " + str(self.isCancel) + "\n"
    def printOrderBackInfo(self):
        # print(self.orderType, self.API, self.price, self.strategyTag, self.amount)
        return str(self.orderCreateTime) + ", "+str(self.orderUpdateTime) + ", " + self.symbol + ", " + self.expiry + ", " + str(self.filledAmount) + ", " + str(self.price) + ", " + \
    str(self.orderType) + ", " + str(self.leverage) + ", " + str(self.strategyTag) + ", " + str(self.orderStatus) + \
    ", " + str(self.orderID) + ", " + str(self.fillPrice) +"\n"
##############################################################################################
# trade feedback
class coinTrade(object):
    def __init__(self):
        self.isSpot = True
        self.API = None
        self.tradeTime = None
        self.symbol = ""
        self.contract_id = ""
        self.expire = "None"
        self.tradePrice = None
        self.tradeAmount = None
        self.orderAmount = None # order total amount to be filled
        self.dealAmount = None # all the filled amount
        self.orderID = -1
        self.type = None
        self.status = None
        self.tradeFee = 0.
        self.strategyTag = None

    def initFutureTrade(self, apiType, tradeTime, contract_id, expire, tradePrice, tradeAmount, orderId, strategyTag, tradeFee = None, \
                        orderAmount = None, dealAmount = None, orderType = None, orderStatus = None):
        self.API = apiType
        self.isSpot = False
        self.tradeTime = tradeTime
        self.contract_id = contract_id
        self.tradePrice = tradePrice
        self.tradeAmount = tradeAmount
        self.orderAmount = orderAmount
        self.dealAmount = dealAmount
        self.orderID = orderId
        self.strategyTag = strategyTag
        self.tradeFee = tradeFee
        self.expire = expire
        self.type = orderType
        self.status = orderStatus

    def initSpotTrade(self, apiType, tradeTime, symbol, tradePrice, tradeAmount, orderId, strategyTag, tradeFee = None, \
                     orderAmount=None, dealAmount = None, orderType = None, orderStatus = None):
        self.API = apiType
        self.isSpot = True
        self.symbol = symbol
        self.tradeTime = tradeTime
        self.tradePrice = tradePrice
        self.tradeAmount = tradeAmount
        self.dealAmount = dealAmount
        self.orderAmount = orderAmount
        self.orderID = orderId
        self.strategyTag = strategyTag
        self.tradeFee = tradeFee
        self.type = orderType
        self.status = orderStatus

    def printTradeBackInfo(self):
        return self.symbol + ", " + self.expire + ", " + str(self.tradeAmount) + ", " + str(self.tradePrice) + ", " \
               + str(self.strategyTag) + ", " + str(self.orderID) + "," + self.API + "\n"
#####################################################################################
# for all market order info
class marketOrderInfo(object):
    def __init__(self):
        self.timestamp = -1
        self.symbol = ""
        self.expiry = "None"
        self.tradeId = None
        self.price = 0.
        self.amount = 0.
        self.tradeType = None
        self.API = "None"
        self.isSpot = True

    def initSpotMarketOrderInfo(self, dataDict):
        self.timestamp = dataDict["timestamp"]
        self.symbol = dataDict["symbol"]
        self.tradeId = dataDict["tradeId"]
        self.price = dataDict["price"]
        self.amount = dataDict["amount"]
        self.tradeType = dataDict["tradeType"]
        self.API = dataDict["API"]

    def initFutureMarketOrderInfo(self, dataDict):
        self.timestamp = dataDict["timestamp"]
        self.symbol = dataDict["symbol"]
        self.expiry = dataDict["expiry"]
        self.tradeId = dataDict["tradeId"]
        self.price = dataDict["price"]
        self.amount = dataDict["amount"]
        self.tradeType = dataDict["tradeType"]
        self.API = dataDict["API"]
        self.isSpot = False

    def printMarketOrderInfo(self):
        return self.symbol + ", " + self.expiry + ", " + str(self.tradeId) + ", " + str(self.price) + ", " + \
            str(self.amount) + ", " + str(self.tradeType) + ", " + str(self.API) + ", " + str(self.isSpot)
