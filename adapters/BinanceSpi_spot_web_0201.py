# -*- coding: utf-8 -*-
import sys
import json
from threading import Thread
sys.path.append("../")
import trader.coinTradeStruct
import trader.traderInterface
import trader.coinUtilities
import trader.positionManager
import trader.defines
import adapters.BinanceStruct
import trader.locker
import threading
import pandas as pd
import gzip
import time
from .binance.client import Client
from .binance.websockets import BinanceSocketManager
from .binance.depthcache import DepthCache, DepthCacheManager

class BinanceSpi_spot_web(trader.traderInterface.adapterTemplate):
    def __init__(self):
        super(BinanceSpi_spot_web, self).__init__()
        self._api_key = ''  # 用户名
        self._secret_key = ''  # 密码
        self._host = ''  # 服务器地址
        self._client = None
        self._ws = None  # websocket应用对象
        self._restURL = ''
        self.thread = None
        self.API_name = ""
        self.tickerTable = None

        self.strategies = {}
        self.workingTickers = []
        self.orderID_strategyMap = {}
        self.query_time = []
        self.query_numberLimit = 1
        self.query_timeLimit = 1 # seconds
        self.timeLimitLock = threading.Lock()
        self.dataFeedOn = True
        self.adapterOn = True
        self.tickerLastPriceVolume = {}
        self.trader = None
        self.queryPositionFailedTimes = 0
        self.orderSendLock = threading.Lock()
        self.positionQueryLock = trader.locker.RWLock()
        self.depthDataUpdateLock = trader.locker.RWLock()
        self.eachTickerFreePosition = {}
        self.eachTickerFreezedPosition = {}
        self.openFeeRate = 0.001
        self.closeFeeRate = 0.0
        self.path2PositionFolder = ""
        self.allSymbolPositionMap = {}
        self.tradeTickerPositionMap = {}
        self.cashBaseValue = {}
        self.secondaryAPI_symbol_orders = {}
        self.IDCount = 0
        self.sendFailedOrderIDList = []
        self.cancelOrderID2OriginalOrderID = {}

        # dict to convert the ticker names
        self.BinanceTickerToOkexTickerDict = {}
        self.OkexTickerToBinanceTickerDict = {}
        # depth data managers
        self.depthDataManagerDict = {}

        # order status
        self.orderType2BinanceOrderTypeDict = {
            trader.defines.openLong_limitPrice: "buy-limit", \
            trader.defines.openShort_limitPrice: "buy-limit", \
            trader.defines.coverLong_limitPrice: "sell-limit", \
            trader.defines.coverShort_limitPrice: "sell-limit", \
            trader.defines.openLong_marketPrice: "buy-market", \
            trader.defines.openShort_marketPrice: "buy-market", \
            trader.defines.coverLong_marketPrice: "sell-market", \
            trader.defines.coverShort_marketPrice: "sell-market", \
            trader.defines.openLong_buy_ioc: "buy-ioc", \
            trader.defines.coverLong_sell_ioc: "sell_ioc", \
            trader.defines.openLong_buy_limit_maker: "buy_limit_maker", \
            trader.defines.coverLong_sell_limit_maker: "sell_limit_maker"
        }

        self.OrderStatus2SystemOrderStatusDict = {
            "NEW" : trader.defines.orderFilling,
            "PARTIALLY_FILLED" : trader.defines.orderFilling,
            "PENDING_CANCEL" : trader.defines.orderCancelled,
            "FILLED" : trader.defines.orderFilled,
            "CANCELED" : trader.defines.orderCancelled,
            "REJECTED" : trader.defines.orderRejected,
            "EXPIRED" : trader.defines.orderCancelled
        }
        # rest api
        self.restAPI = None

    # ------------------------------------------------------------
    def initAdapter(self, loginInfoDict):
        self._host = loginInfoDict["webURL"]
        self._api_key = loginInfoDict["apiKey"]
        self._secret_key = loginInfoDict["secretKey"]
        self._restURL = loginInfoDict['restURL']
        self.API_name = loginInfoDict["apiName"]
        self.openFeeRate = loginInfoDict["openFeeRate"]
        self.closeFeeRate = loginInfoDict["closeFeeRate"]
        self.path2PositionFolder = loginInfoDict["path2PositionFolder"]
        # load positions in adapter
        self.loadLocalPositions()

    def loadLocalPositions(self):
        # read csv file
        positionFile = self.path2PositionFolder + "/" + self.getAPI_name() + ".csv"
        try:
            positions = pd.read_csv(positionFile)
            # create position
            # print(positions)
            for index, row in positions.iterrows():
                # only need to save the position that belong to this adapter
                if row["api"] == self.API_name:
                    iPos = trader.positionManager.openPosition(row["strategy"])
                    if row["expiry"] == "None":
                        # init spot position
                        iPos.initSpotPosition(row["symbol"], self.API_name, float(row["long"]), bool(row["subTractFee"]))
                    else:
                        print("[ERR]: obtained future position with spot API for adapter", self.getAPI_name(), file=sys.stderr)
                    # set pnl info
                    iPos.setFeeRate(self.openFeeRate, self.closeFeeRate)
                    iPos.setContractSize(int(row["contractSize"]))
                    iPos.setGrossAndFee(float(row["gross"]), float(row["fee"]), float(row["lastPrice"]))
                    self.allSymbolPositionMap[row["symbol"] + "-" + row["expiry"] + "-" + row["strategy"]] = iPos
        except:
            print("[WARN]: failed to open position file", positionFile, "for adapter", self.getAPI_name(), file=sys.stderr)
            print("Will init all positions as 0", file=sys.stderr)

    def setTrader(self, trader):
        '''
        link adapter to trade interface
        :param trader:
        :return:
        '''
        self.trader = trader

    def startAdapter(self):
        """连接服务器"""
        self.adapterOn = True
        self.dataFeedOn = True

        # for rest
        self._client = Client(self._api_key, self._secret_key)
        # self._client = Client("", "")

        # for ws
        self._ws = BinanceSocketManager(self._client)

        # start user socket
        self._ws.start_user_socket(self.processUserInfo)

        # start to get data
        self._ws.start_miniticker_socket(self.processDetailData, update_time=1000)

        # regiest tickers
        self.regiestTickers()

        # query positions
        self.queryPositions()

    def saveTickerPositions(self):
        # save the positions one by one
        positionFile = self.path2PositionFolder + "/" + self.getAPI_name() + ".csv"
        f = open(positionFile, "w+")
        # header
        f.write("symbol,expiry,api,long,short,strategy,gross,fee,lastPrice,contractSize,subTractFee\n")
        for i, iPos in self.allSymbolPositionMap.items():
            f.write(iPos.getPositonInfoString())
        f.write("\n")
        f.close()

    def printEachPosition(self):
        for i, pos in self.tradeTickerPositionMap.items():
            pos.printPositionContent()

    def printAdapterPosition(self):
        # get positions for each adapter
        adapterPositions = []
        for i, pos in self.tradeTickerPositionMap.items():
            findMerge = False
            for po in adapterPositions:
                findMerge = (po.mergePosition(pos) or findMerge)
            if not findMerge:
                # new ticker
                position = trader.positionManager.openPosition("all")
                position.copyPositionInfo(pos)
                adapterPositions.append(position)
        for pos in adapterPositions:
            pos.printPositionContent()

    def stopDataFeed(self):
        self.dataFeedOn = False

    def stopAdapter(self):
        # before close connect, save positions
        self.saveTickerPositions()
        print("saved ticker positions info for adapter", self.getAPI_name())

        self.adapterOn = False
        # close depthData managers
        for iManager in self.depthDataManagerDict.values():
            try:
                iManager.close()
            except Exception as e:
                print("exit depthDataManager failed", str(e))

        self.close_connect()

    def timeLimitCoolDown(self):
        # here we need to check send frequency first
        # if the firequency is larger than threashold value, need to pause
        self.timeLimitLock.acquire()
        currentTime = time.time()
        if len(self.query_time) >= self.query_numberLimit:
            firstTime = self.query_time.pop(0)
            while (currentTime - firstTime) < self.query_timeLimit + 0.02:
                time.sleep(0.01)
                currentTime = time.time()
        self.query_time.append(currentTime)
        self.timeLimitLock.release()

    def sendOrder(self, order, isSecondaryAPI=False):
        if not self.adapterOn:
            print("trade adapter is not inited yet", self.getAPI_name())
            return
        # check send frequency
        self.timeLimitCoolDown()
        # order will be sent through rest API first
        self.orderSendLock.acquire()
        # here we use time stamp as orderID
        orderID = int(time.time()) * 100 + self.IDCount
        self.IDCount = (self.IDCount + 1) % 100
        orderDict = {
            "symbol" :  self.OkexTickerToBinanceTickerDict[order.symbol],
            "quantity" : format(order.amount, 'f'),
            "newClientOrderId" : str(orderID)
        }

        ret = None
        sendSuccess = False

        # print("order", order.printOrderInfo())
        # print("order", orderDict)
        if order.orderType == trader.defines.openLong_limitPrice:
            orderDict.update({"price" : str(order.price)})
            try:
                ret = self._client.order_limit_buy(**orderDict)
                sendSuccess = True
            except Exception as e:
                print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                      " order send failed for strategy ", order.getStrategyTag(), self.getAPI_name(), file=sys.stderr)
                print("[ERR]: ret info", str(e), file=sys.stderr)
        elif order.orderType == trader.defines.coverLong_limitPrice:
            orderDict.update({"price" : str(order.price)})
            try:
                ret = self._client.order_limit_sell(**orderDict)
                sendSuccess = True
            except Exception as e:
                print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                      " order send failed for strategy ", order.getStrategyTag(), self.getAPI_name(), file=sys.stderr)
                print("[ERR]: ret info", str(e), file=sys.stderr)
        elif order.orderType == trader.defines.openLong_marketPrice:
            try:
                ret = self._client.order_market_buy(**orderDict)
                sendSuccess = True
            except Exception as e:
                print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                      " order send failed for strategy ", order.getStrategyTag(), self.getAPI_name(), file=sys.stderr)
                print("[ERR]: ret info", str(e), file=sys.stderr)
        elif order.orderType == trader.defines.coverLong_marketPrice:
            try:
                ret = self._client.order_market_sell(**orderDict)
                sendSuccess = True
            except Exception as e:
                print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                      " order send failed for strategy ", order.getStrategyTag(), self.getAPI_name(), file=sys.stderr)
                print("[ERR]: ret info", str(e), file=sys.stderr)

        else:
            print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                  "orderType not defined for", self.getAPI_name(), " orderType:", order.orderType, file=sys.stderr)

        if sendSuccess:
            # save order id Info
            self.orderID_strategyMap[int(orderID)] = order.getStrategyTag()
            order.setOrderID(int(orderID))
            # find position manager
            iPositionManager = self.tradeTickerPositionMap[
                order.symbol + "-" + order.expiry + "-" + order.getStrategyTag()]
            iPositionManager.addOpenOrder(order)
            self.orderSendLock.release()
            return 0
        else:
            self.sendFailedOrderIDList.append(int(orderID))
            self.orderSendLock.release()
            return -1

    def cancelOrder(self, order):
        if not self.adapterOn:
            return
        # check order is in cancelling status, do not cancel pending order with ID -19860922
        if order.isCancel or (order.orderID == -19860922):
            return
        # check send frequency
        self.timeLimitCoolDown()
        # order will be sent through rest API first
        self.orderSendLock.acquire()
        # here we use time stamp as orderID
        orderID = int(time.time()) * 100 + self.IDCount
        self.IDCount = (self.IDCount + 1) % 100
        # get cancel order dict
        cancelDict = {
            "symbol" : self.OkexTickerToBinanceTickerDict[order.symbol],
            "origClientOrderId" : str(order.orderID),
            "newClientOrderId" : str(orderID)
        }

        # print("cancel order", order.printOrderInfo())
        # send cancel order out
        try:
            ret = self._client.cancel_order(**cancelDict)
            self.cancelOrderID2OriginalOrderID[orderID] = order.orderID
            order.isCancel = True
            # print("cancel ret", ret)
        except Exception as e:
            print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                  "failed to cancel order with ID", order.orderID, self.getAPI_name())
            print("ret", str(e))
            # return the cancel failed info back to strategy
            # order.orderStatus = trader.defines.orderCancelFailed
            # self.strategies[order.strategyTag].onOrderFeedback(order)
            self.orderSendLock.release()
            return
        self.orderSendLock.release()
        return 0

    def sendOrderBySecondaryAPI(self, order):
        pass

    def onOrderBack(self, orderBack):
        # print("onOrderBack", orderBack.printOrderBackInfo())
        # update position manager first
        self.tradeTickerPositionMap[orderBack.symbol + "-" + orderBack.expiry + "-" + orderBack.strategyTag].updateOrderStatusOnOrderBack(orderBack)
        # process order back in strategy
        self.strategies[orderBack.strategyTag].onOrderFeedback(orderBack)

        if not self.trader is None:
            self.trader.writeOrderBackInfo2File(orderBack)
        else:
            print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                  " order back received before trader is linked to adapters", file=sys.stderr)
            print("order info ", orderBack.printOrderBackInfo(), file=sys.stderr)

    def onTradeBack(self, tradeBack):
        pass

    def onErrorBack(self, errorMsg):
        pass

    def onMarketOrderBack(self, marketDeal):
        '''
        for last market deal
        :param errorMsg:
        :return:addStrategy
        '''
        if self.dataFeedOn:
            for strategy in self.strategies.values():
                strategy.onRtnTrade(marketDeal)

    def onDepthData(self, depthData):
        if self.dataFeedOn:
            tag0 = depthData.symbol + "-" + depthData.expiry
            for strategy in self.strategies.values():
                strategy.onRtnDepth(depthData)
                # update PNL calculation
                tag = tag0 + "-" + strategy.getStrategyTag()
                if tag in self.tradeTickerPositionMap:
                    self.tradeTickerPositionMap[tag].updatePNLOnTick(depthData.LastPrice)

    def addStrategy(self, strategy):
        '''
        subscribe strategy to this adapter
        so that the data and trade will be sent to the strategy from the adapter
        :param strategy:
        :return:
        '''
        if strategy.getStrategyTag() not in self.strategies:
            self.strategies[strategy.getStrategyTag()] = strategy
            # get working tickers for this api
            iWorkingTickers, iWorkingTickerIsSpot, iWorkingTickerExpires, iAPIs = strategy.getWorkingTickers()
            for i, iTicker in enumerate(iWorkingTickers):
                if iWorkingTickerIsSpot[i] and iAPIs[i] == self.getAPI_name() and len(iTicker) > 2:
                    tickerTag = iTicker + "-" + iWorkingTickerExpires[i] + "-" + strategy.getStrategyTag()
                    self.workingTickers.append(iTicker)
                    # check if the ticker position is created
                    if tickerTag in self.allSymbolPositionMap:
                        self.tradeTickerPositionMap[tickerTag] = self.allSymbolPositionMap[tickerTag]
                        self.tradeTickerPositionMap[tickerTag].setBaseCashValue(self.cashBaseValue)
                    else:
                        # create new position
                        iPos = trader.positionManager.openPosition(strategy.getStrategyTag())
                        iPos.initSpotPosition(iTicker, iAPIs[i], 0, False)
                        iPos.setBaseCashValue(self.cashBaseValue)
                        iPos.setFeeRate(self.openFeeRate, self.closeFeeRate)
                        self.tradeTickerPositionMap[tickerTag] = iPos
                        self.allSymbolPositionMap[tickerTag] = iPos

    def getWorkingTickers(self):
        '''
        :return: list of tickers in this api, format is symbol-expiry
        '''
        return set(self.workingTickers)

    def getAPI_name(self):
        return self.API_name

    def getPositionManager(self, symbol, expiry, strategyTag):
        try:
            return self.tradeTickerPositionMap[symbol + "-" + expiry + "-" + strategyTag]
        except:
            print("[ERR]: ", trader.coinUtilities.getCurrentTimeString(), \
                  "failed to get position manager for", symbol, expiry, "in adapter", self.getAPI_name(),
                  file=sys.stderr)
            return None

    def getTickerLongPosition(self, symbol, expiry, strategyTag):
        try:
            return self.tradeTickerPositionMap[symbol + "-" + expiry + "-" + strategyTag].getLongShare()
        except:
            print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                  "failed to get", symbol, expiry, strategyTag, "in tradeTickerPositionMap for ", self.getAPI_name(), file=sys.stderr)
            return 0

    def getTickerShortPosition(self, symbol, expiry, strategyTag):
        try:
            return self.tradeTickerPositionMap[symbol + "-" + expiry + "-" + strategyTag].getShortShare()
        except:
            print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                  "failed to get", symbol, expiry, strategyTag, "in tradeTickerPositionMap for ", self.getAPI_name(), file=sys.stderr)
            return 0
    def getTickerOpenOrders(self, symbol, expiry, strategyTag):
        try:
            return self.tradeTickerPositionMap[symbol + "-" + expiry + "-" + strategyTag].getOpenOrders()
        except:
            print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                  "failed to get", symbol, expiry, strategyTag, "in tradeTickerPositionMap for ", self.getAPI_name(), file=sys.stderr)
            return []

    def setBaseCashValue(self, cashBaseValue):
        self.cashBaseValue = cashBaseValue

    # -------------------------- 连接 --------------------------

    def reconnect(self):
        # api handle it
        pass

    # ---------------------------------------------------- #
    def regiestTickers(self):
        self.BinanceTickerToOkexTickerDict = {}
        self.OkexTickerToBinanceTickerDict = {}
        subscribedSymbolList = []
        for ticker in self.workingTickers:
            # here we need to convert system ticker name to huobi ticker name
            splitStr = ticker.split("_")
            binanceTicker = (splitStr[0] + splitStr[1]).upper()
            # save into map
            self.BinanceTickerToOkexTickerDict[binanceTicker] = ticker
            self.OkexTickerToBinanceTickerDict[ticker] = binanceTicker
            print(self.getAPI_name(), "subscribe", binanceTicker, ticker)
            # init lastprice and volume
            self.tickerLastPriceVolume[binanceTicker] = [None, None]


            # self._ws.start_depth_socket(binanceTicker, self.processDepthData)
            if binanceTicker not in subscribedSymbolList:
                # regiest for depth data
                iManager = DepthCacheManager(client=self._client, symbol=binanceTicker, callback=self.processDepthData)
                self.depthDataManagerDict[ticker] = iManager

                # regiest market trades
                self._ws.start_trade_socket(binanceTicker, self.processTradeData)

                subscribedSymbolList.append(binanceTicker)

        # start ws
        self._ws.start()


    def close_connect(self):
        print('---->>> onClose for adapter ', self.API_name, file=sys.stdout)
        """关闭接口"""
        self.dataFeedOn = False
        self.adapterOn = False
        time.sleep(1)
        try:
            self._ws.close()
        except:
            pass

    # -----------------------------------------------------------
    def processDepthData(self, data):
        if data:
            symbol = data.symbol
            systemSymbol = self.BinanceTickerToOkexTickerDict[symbol]
            depthData = adapters.BinanceStruct.Binance_depth_Data()
            depthData.initSpotDepthData(systemSymbol, data, self.API_name)
            # get last price and volume
            self.depthDataUpdateLock.read_acquire()
            if not self.tickerLastPriceVolume[symbol][0] is None:
                depthData.LastPrice = float(self.tickerLastPriceVolume[symbol][0])
                depthData.Volume = float(self.tickerLastPriceVolume[symbol][1])
                self.depthDataUpdateLock.read_release()
                self.onDepthData(depthData)
            else:
                self.depthDataUpdateLock.read_release()
        else:
            print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                  "get error in depth data collector", self.getAPI_name())


    # -----------------------------------------------------------
    def processDetailData(self, data):
        # get last price and volume
        for iData in data:
            symbol = iData['s']
            if symbol in self.tickerLastPriceVolume.keys():
                # print(iData)
                self.depthDataUpdateLock.write_acquire()
                self.tickerLastPriceVolume[symbol] = [float(iData["c"]), float(iData["v"])]
                self.depthDataUpdateLock.write_release()
    # ------------------------------------------------------------
    def processTradeData(self, data):
        if not self.dataFeedOn:
            return
        # print(data)
        # trade
        symbol = data['s']
        dataDict = {"symbol": self.BinanceTickerToOkexTickerDict[symbol], \
                    "API": self.API_name, \
                    "tradeId": data["t"], \
                    "price": float(data["p"]), \
                    "amount": float(data["q"]), \
                    "timestamp": int(data["T"]), \
                    "tradeType": trader.defines.tradeBid if data["m"]  else trader.defines.tradeAsk}
        tradeData = trader.coinTradeStruct.marketOrderInfo()
        tradeData.initSpotMarketOrderInfo(dataDict)

        for strategy in self.strategies.values():
            strategy.onRtnTrade(tradeData)

    # -----------------------------------------------------------
    def processUserInfo(self, data):
        #print("user info")
        #print(data)
        if data['e'] == "outboundAccountInfo":
            self.updateAccountInfo(data)
        elif data['e'] == "executionReport":
            self.processOrderExcution(data)

    # -----------------------------------------------------------
    def updateAccountInfo(self, data):
        # loop over each asset to update the info
        self.positionQueryLock.write_acquire()
        for iAsset in data["B"]:
            symbol = iAsset["a"].lower()
            self.eachTickerFreePosition[symbol] = float(iAsset["f"])
            self.eachTickerFreezedPosition[symbol] = float(iAsset["l"])
        self.positionQueryLock.write_release()

    def queryPositions(self):
        # query account balance
        self.timeLimitCoolDown()
        ret = self._client.get_account()
        # print(ret)
        # process the query info
        self.positionQueryLock.write_acquire()
        for iAsset in ret["balances"]:
            symbol = iAsset["asset"].lower()
            self.eachTickerFreePosition[symbol] = float(iAsset["free"])
            self.eachTickerFreezedPosition[symbol] = float(iAsset["locked"])
        self.positionQueryLock.write_release()

    def getFreePosition(self, ticker, expiry, diretion = "long"):
        # get ticker name
        tickerName = ticker.split("_")[0]
        self.positionQueryLock.read_acquire()
        try:
            value = self.eachTickerFreePosition[tickerName]
            self.positionQueryLock.read_release()
            return value
        except:
            self.positionQueryLock.read_release()
            return None

    def getFreezedPosition(self, ticker, expiry, diretion = "long"):
        tickerName = ticker.split("_")[0]
        self.positionQueryLock.read_acquire()
        try:
            value = self.eachTickerFreezedPosition[tickerName]
            self.positionQueryLock.read_release()
            return value
        except:
            self.positionQueryLock.read_release()
            return None

    def getCoinBalance(self, coin):
        freeValue = 0.0
        freedvalue = 0.0
        tickerName = coin.split("_")[0]
        self.positionQueryLock.read_acquire()
        try:
            freeValue = self.eachTickerFreePosition[tickerName]
            freedvalue = self.eachTickerFreezedPosition[tickerName]
            self.positionQueryLock.read_release()
            return freeValue + freedvalue
        except:
            self.positionQueryLock.read_release()
            return None

    ################################################################
    def processOrderExcution(self, json_data):
        self.orderSendLock.acquire()
        self.orderSendLock.release()
        # find strategyTag from ID
        # print(json_data)
        if not str(json_data["c"]).isdigit():
            # print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
            #       "obtained undefined orderID", json_data, self.getAPI_name())
            return
        ID = int(json_data["c"])
        if ID in self.orderID_strategyMap:
            # process trade data
            orderback = trader.coinTradeStruct.coinOrder()
            data = {"apiType": self.API_name, \
                    "symbol": self.BinanceTickerToOkexTickerDict[json_data["s"]], \
                    "orderID": ID, \
                    "status": self.OrderStatus2SystemOrderStatusDict[json_data["X"]], \
                    "deal_amount": float(json_data["z"]), \
                    "orderUpdateTime": int(json_data["E"]), \
                    "strategyTag": self.orderID_strategyMap[ID], \
                    "fillPrice": float(json_data["L"])
                    }
            orderback.setSpotOrderFeedback(data=data, isOrderBackOnly=True)
            if orderback.orderStatus == trader.defines.orderCancelled:
                print("--->>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>C OrderExe {}|{}".format(round(time.time(), 4), orderback.orderID))
            if orderback.orderStatus == trader.defines.orderFilled:
                print("--->>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>F OrderExe {}|{}".format(round(time.time(), 4), orderback.orderID))
            self.onOrderBack(orderback)
        else:
            if ID in self.cancelOrderID2OriginalOrderID:
                # process canceled order
                ID = self.cancelOrderID2OriginalOrderID[ID]
                # process trade data
                orderback = trader.coinTradeStruct.coinOrder()
                data = {"apiType": self.API_name, \
                        "symbol": self.BinanceTickerToOkexTickerDict[json_data["s"]], \
                        "orderID": ID, \
                        "status": self.OrderStatus2SystemOrderStatusDict[json_data["X"]], \
                        "deal_amount": float(json_data["z"]), \
                        "orderUpdateTime": int(json_data["E"]), \
                        "strategyTag": self.orderID_strategyMap[ID], \
                        "fillPrice": float(json_data["L"])
                        }
                orderback.setSpotOrderFeedback(data=data, isOrderBackOnly=True)
                if orderback.orderStatus == trader.defines.orderCancelled:
                    print("--->>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>C OrderExe {}|{}".format(round(time.time(), 4), orderback.orderID))
                if orderback.orderStatus == trader.defines.orderFilled:
                    print("--->>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>F OrderExe {}|{}".format(round(time.time(), 4), orderback.orderID))

                self.onOrderBack(orderback)
                return

            if ID not in self.sendFailedOrderIDList:
                # print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                #       "failed to find orderID", ID, "for adapter", self.getAPI_name())
                pass
            else:
                self.orderSendLock.acquire()
                self.sendFailedOrderIDList.remove(ID)
                self.orderSendLock.release()
