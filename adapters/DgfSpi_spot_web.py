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
import adapters.rest.OkcoinSpotAPI
import adapters.DgfStruct
import adapters.digifinex.client
import trader.locker
import threading
import pandas as pd
from websocket import create_connection
import gzip
import time
from queue import Queue, Empty
from copy import deepcopy

class DgfSpi_spot_web(trader.traderInterface.adapterTemplate):
    def __init__(self):
        super(DgfSpi_spot_web, self).__init__()
        self._api_key = ''  # 用户名
        self._secret_key = ''  # 密码
        self._host = ''  # 服务器地址
        self._ws = None  # websocket应用对象
        self._restURL = ''
        self.thread = None
        self.orderInfoThread = None
        self.linkTime = 0
        self.lastLinkTime = 0
        self.checkConnectionFailedTimes = 0
        self.API_name = ""

        self.strategies = {}
        self.workingTickers = []
        self.orderID_strategyMap = {}
        self.query_time = []
        self.query_numberLimit = 100
        self.query_timeLimit = 10 # seconds
        self.timeLimitLock = threading.Lock()
        self.dataFeedOn = True
        self.adapterOn = True
        self.tickerLastPriceVolume = {}
        self.trader = None
        self.queryPositionFailedTimes = 0
        self.orderSendLock = threading.Lock()
        self.positionQueryLock = trader.locker.RWLock()
        self.eachTickerFreePosition = {}
        self.eachTickerFreezedPosition = {}
        self.openFeeRate = 0.002
        self.closeFeeRate = 0.0
        self.path2PositionFolder = ""
        self.allSymbolPositionMap = {}
        self.tradeTickerPositionMap = {}
        self.cashBaseValue = {}
        self.secondaryAPI_symbol_orders = {}

        self.openOrderIDListLock = trader.locker.RWLock()
        self.openOrderIDOrderSymbolDict = {}

        # dict to convert the ticker names
        self.DgfTickerToOkexTickerDict = {}
        self.OkexTickerToDgfTickerDict = {}

        self.orderType2DgfOrderTypeDict = {
            trader.defines.openLong_limitPrice: "buy-limit", \
            trader.defines.openShort_limitPrice: "buy-limit", \
            trader.defines.coverLong_limitPrice: "sell-limit", \
            trader.defines.coverShort_limitPrice: "sell-limit", \
            trader.defines.openLong_buy_limit_maker: "buy-limit-maker", \
            trader.defines.coverLong_sell_limit_maker: "sell-limit-maker"
        }

        self.OrderStatus2SystemOrderStatusDict = {
            0 : trader.defines.orderFilling,
            1 : trader.defines.orderFilling,
            2 : trader.defines.orderFilled,
            3: trader.defines.orderCancelled,
            4 : trader.defines.orderCancelled
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
                        iPos.initSpotPosition(row["symbol"], self.API_name, float(row["long"]))
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

        # rest api
        self.restAPI = adapters.digifinex.client.dgf_client(self._restURL, self._api_key, self._secret_key)

        # regiest tickers
        self.regiestTickers()

        # query positions
        self.queryPositions()

    def saveTickerPositions(self):
        # save the positions one by one
        positionFile = self.path2PositionFolder + "/" + self.getAPI_name() + ".csv"
        f = open(positionFile, "w+")
        # header
        f.write("symbol,expiry,api,long,short,strategy,gross,fee,lastPrice,contractSize\n")
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
        time.sleep(4)
        '''
        if self.orderInfoThread and self.orderInfoThread.isAlive():
            try:
                self.orderInfoThread.join()
            except:
                pass

        if self.dataInfoThread and self.dataInfoThread.isAlive():
            try:
                self.dataInfoThread.join()
            except:
                pass
        '''

    def stopAdapter(self):
        self.adapterOn = False
        # before close connect, save positions
        self.saveTickerPositions()
        print("saved ticker positions info for adapter", self.getAPI_name())

    def timeLimitCoolDown(self):
        # here we need to check send frequency first
        # if the firequency is larger than threashold value, need to pause
        self.timeLimitLock.acquire()
        currentTime = time.time()
        if len(self.query_time) >= self.query_numberLimit:
            firstTime = self.query_time.pop(0)
            while (currentTime - firstTime) < self.query_timeLimit + 0.3:
                time.sleep(0.5)
                currentTime = time.time()
        self.query_time.append(currentTime)
        self.timeLimitLock.release()

    def sendOrder(self, order, isSecondaryAPI=False):
        # check send frequency
        self.timeLimitCoolDown()
        # order will be sent through rest API first
        self.orderSendLock.acquire()
        ret = None
        try:
            param = {}
            param["symbol"] = self.OkexTickerToDgfTickerDict[order.symbol]
            param["price"] = order.price
            param["amount"] = order.amount

            ordertype = self.orderType2DgfOrderTypeDict[order.orderType]
            splits = ordertype.split("-")
            # omk
            if len(splits) == 3:
                param["type"] = splits[0]
                param["post_only"] = 1
            else:
                param["type"] = splits[0]
                param["post_only"] = 0

            ret = self.restAPI.input_order(param)

        except:
            self.orderSendLock.release()
            print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                  " order send failed for strategy ", order.getStrategyTag(), file=sys.stderr)
            return -1

        if not ret is None:
            orderID = ret["order_id"]
            self.orderID_strategyMap[orderID] = order.getStrategyTag()
            order.setOrderID(orderID)
            # find position manager
            iPositionManager = self.tradeTickerPositionMap[order.symbol + "-" + order.expiry + "-" + order.getStrategyTag()]
            iPositionManager.addOpenOrder(order)
            self.orderSendLock.release()

            self.openOrderIDListLock.write_acquire()
            self.openOrderIDOrderSymbolDict[orderID] = order.symbol
            self.openOrderIDListLock.write_release()
            # self.openOrderIDQueue.put(orderID)
            return 0
        else:
            self.orderSendLock.release()
            print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                  " order send failed for strategy ", order.getStrategyTag(), file=sys.stderr)
            print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                  " errorInfo: ", ret, file=sys.stderr)
            return -1

    def sendOrderBySecondaryAPI(self, order):
        pass

    def cancelOrder(self, order):
        # check order is in cancelling status, do not cancel pending order with ID -19860922
        if order.isCancel or (order.orderID == "secondaryOrder"):
            return 0
        # check send frequency
        self.timeLimitCoolDown()

        param = {}
        param["order_id"] = order.orderID
        ret = None
        try:
            ret = self.restAPI.cancel_order(param)
        except:
            print("[WARN]:", trader.coinUtilities.getCurrentTimeString(), \
                  "restAPI exception, failed to cancel order", order.orderID, "for", self.getAPI_name(), ret)
            return -1
        if len(ret["success"]) == 0:
            print("[WARN]:", trader.coinUtilities.getCurrentTimeString(), \
                  "restAPI ret status not ok, failed to cancel order", order.orderID, "for", self.getAPI_name(), ret)
            return -1
        order.isCancel = True
        return 0

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
                        # spot contractSize is defaulted to 1
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

    # ---------------------------------------------------- #
    def regiestTickers(self):
        self.DgfTickerToOkexTickerDict = {}
        self.OkexTickerToDgfTickerDict = {}
        for ticker in self.workingTickers:
            # here we need to convert system ticker name to Dgf ticker name
            splitStr = ticker.split("_")
            DgfTicker = splitStr[1] + "_" + splitStr[0]
            # save into map
            self.DgfTickerToOkexTickerDict[DgfTicker] = ticker
            self.OkexTickerToDgfTickerDict[ticker] = DgfTicker
            print(self.getAPI_name(), "subscribe", DgfTicker, ticker)

            # init lastprice and volume
            self.tickerLastPriceVolume[DgfTicker] = [None, None]
            # subscribe data

        # start a thread to query order info
        self.orderInfoThread = Thread(target=self.restQueryInfos)
        self.orderInfoThread.start()

        # then start a separate thread to collect data
        self.dataInfoThread = Thread(target=self.dataCollector)
        self.dataInfoThread.start()

    def dataCollector(self):
        while (1):
            if self.adapterOn:
                try:
                    for isymbol in set(self.DgfTickerToOkexTickerDict.keys()):

                        deals = self.restAPI.deals(isymbol)

                        if not deals is None:
                            self.processDetailData(isymbol, deals)

                        depths = self.restAPI.depth(isymbol)

                        if not depths is None:
                            self.processDepthData(isymbol, depths)

                        time.sleep(0.15)
                    # 查询频率1s
                    time.sleep(1)
                except Exception:
                    pass
            else:
                break

    # -----------------------------------------------------------
    def processDepthData(self, isymbol, data):
        # print(data)
        systemSymbol = self.DgfTickerToOkexTickerDict[isymbol]
        depthData = adapters.DgfStruct.Dgf_depth_Data()
        depthData.initSpotDepthData(systemSymbol, data, self.API_name)
        # get last price and volume
        if self.tickerLastPriceVolume[isymbol][0]:
            depthData.LastPrice = float(self.tickerLastPriceVolume[isymbol][0])
            depthData.Volume = float(self.tickerLastPriceVolume[isymbol][1])
            self.onDepthData(depthData)

    # -----------------------------------------------------------
    def processDetailData(self, isymbol, data):
        # get last price and volume
        self.tickerLastPriceVolume[isymbol] = [data["data"][0]["price"], data["data"][0]["amount"]] # "vol" is amount * price
    # ------------------------------------------------------------
    def processTradeData(self, data):
        # print(data)
        # trade
        pass
    # -----------------------------------------------------------
    def queryPositions(self):
        # query account balance
        self.timeLimitCoolDown()
        ret = None
        try:
            ret = self.restAPI.balance()
        except:
            print("[WARN]:", trader.coinUtilities.getCurrentTimeString(), \
                  "failed to obtain account info for", self.getAPI_name())
            return

        if not ret is None:
            positionList = set(ret["free"]) | set(ret["frozen"])
            self.positionQueryLock.write_acquire()
            for iCoinData in positionList:
                self.eachTickerFreePosition[iCoinData] = float(ret["free"].get(iCoinData, 0))
                self.eachTickerFreezedPosition[iCoinData] = float(ret["frozen"].get(iCoinData, 0))
            self.positionQueryLock.write_release()
            self.queryPositionFailedTimes = 0
        else:
            # count query failed times
            self.queryPositionFailedTimes += 1
            if self.queryPositionFailedTimes > 5:
                print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                      "failed to query positions for adapter", self.getAPI_name())

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

    def restQueryInfos(self):

        while self.adapterOn:
            # query order status
            # print("query")
            # self.openOrderIDListLock.read_acquire()
            orderids = deepcopy(list(self.openOrderIDOrderSymbolDict.keys()))
            if len(orderids) > 0:
                ret = self.restAPI.batch_orders(orderids)
                # print(ret)
                if not ret is None:
                    for idata in ret["data"]:

                        orderback = trader.coinTradeStruct.coinOrder()

                        orderid = idata["order_id"]
                        filledAmount = float(idata["executed_amount"])
                        fillPrice = idata["avg_price"]
                        orderStatus = idata["status"]

                        # self.orderSendLock.acquire()
                        if orderid in self.orderID_strategyMap:
                            strategyTag = self.orderID_strategyMap[orderid]
                            # self.orderSendLock.release()

                        else:
                            # self.orderSendLock.release()
                            print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                                  "failed to find orderID", id, "in orderID_strategyMap for adapter", self.getAPI_name(),
                                  file=sys.stderr)
                            continue

                        data = {"apiType": self.API_name, \
                                "symbol": self.openOrderIDOrderSymbolDict[orderid], \
                                "orderID": orderid, \
                                "status": self.OrderStatus2SystemOrderStatusDict[orderStatus], \
                                "deal_amount": filledAmount, \
                                "orderUpdateTime": time.time(), \
                                "strategyTag": strategyTag, \
                                "fillPrice": fillPrice
                                }
                        orderback.setSpotOrderFeedback(data=data, isOrderBackOnly=True)
                        self.onOrderBack(orderback)

                        # remove complete orders
                        if self.OrderStatus2SystemOrderStatusDict[orderStatus] >= trader.defines.orderFilled:
                            self.openOrderIDOrderSymbolDict.pop(orderid)

                else:
                    # self.openOrderIDListLock.read_release()
                    continue
                # self.openOrderIDListLock.read_release()
            time.sleep(2)