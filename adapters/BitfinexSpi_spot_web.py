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
import adapters.BitfinexStruct
import trader.locker
import threading
import pandas as pd
import time
from queue import Empty
from multiprocessing import Queue

from .btfxwss.client import BtfxWss


class BitfinexSpi_spot_web(trader.traderInterface.adapterTemplate):
    def __init__(self):
        super(BitfinexSpi_spot_web, self).__init__()
        self._api_key = ''  # 用户名
        self._secret_key = ''  # 密码
        self._host = ''  # 服务器地址
        self._client = None
        self._restURL = ''
        self.messageThread = None
        self.API_name = ""
        self.tickerTable = None

        self.strategies = {}
        self.workingTickers = []
        self.orderID_strategyMap = {}
        self.orderCID_strategyMap = {}
        self.orderCID_orderIDMap = {}
        self.orderID_OrderCIDMap = {}
        self.query_time = []
        self.query_numberLimit = 10
        self.query_timeLimit = 1 # seconds
        self.timeLimitLock = threading.Lock()
        self.dataFeedOn = False
        self.adapterOn = True
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
        self.IDCount = 0
        self.isFirstTimeTradesList = True

        # dict to convert the ticker names
        self.BitfinexTickerToOkexTickerDict = {}
        self.OkexTickerToBitfinexTickerDict = {}

        # dict to link channel to the handle
        self._data_handlers = {
            'subscribed': self._handle_subscribed,
            'conf': self._handle_conf,
            'auth': self._handle_auth,
            'unauth': self._handle_auth,
            'ticker': self._handle_ticker,
            'book': self._handle_book,
            'raw_book': self._handle_raw_book,
            'candles': self._handle_candles,
            'trades': self._handle_trades
        }

        self.channel_handlers = {}
        self.channel_pair = {}
        self.depthDatas = {}
        self.subscribeInfo_channelID_map = {}


        self.orderType2BitfinexOrderTypeDict = {
            trader.defines.openLong_limitPrice: ("EXCHANGE LIMIT", 1), \
            trader.defines.openShort_limitPrice: ("EXCHANGE LIMIT", 1), \
            trader.defines.coverLong_limitPrice: ("EXCHANGE LIMIT", -1), \
            trader.defines.coverShort_limitPrice: ("EXCHANGE LIMIT", -1), \
            trader.defines.openLong_marketPrice: ("EXCHANGE MARKET", 1), \
            trader.defines.openShort_marketPrice: ("EXCHANGE MARKET", 1), \
            trader.defines.coverLong_marketPrice: ("EXCHANGE MARKET", -1), \
            trader.defines.coverShort_marketPrice: ("EXCHANGE MARKET", -1), \

        }

        self.OrderStatus2SystemOrderStatusDict = {
            "ACTIVE" : trader.defines.orderFilling,
            "PARTIALLY FILLED" : trader.defines.orderFilling,
            "CANCELED" : trader.defines.orderCancelled,
            "EXECUTED" : trader.defines.orderFilled,
            "INSUFFICIENT MARGIN" : trader.defines.orderCancelled
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
        # self.dataFeedOn = True

        # init client
        self._client = BtfxWss(key=self._api_key, secret=self._secret_key)

        # open message process thread
        self.messageThread = Thread(target=self.processDataBack)
        self.messageThread.start()

        # start ws, need to start before regiest tickers
        self._client.start()
        while not self._client.conn.connected.is_set():
            time.sleep(1)
            print("connecting ", self.getAPI_name(), "...")

        # auth
        self._client.authenticate()

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

    def stopAdapter(self):
        # before close connect, save positions
        self.saveTickerPositions()
        print("saved ticker positions info for adapter", self.getAPI_name())
        try:
            self.close_connect()
        except:
            pass

    def timeLimitCoolDown(self):
        # here we need to check send frequency first
        # if the firequency is larger than threashold value, need to pause
        self.timeLimitLock.acquire()
        currentTime = time.time()
        if len(self.query_time) >= self.query_numberLimit:
            firstTime = self.query_time.pop(0)
            while (currentTime - firstTime) < self.query_timeLimit + 0.3:
                time.sleep(0.05)
                currentTime = time.time()
        self.query_time.append(currentTime)
        self.timeLimitLock.release()

    def sendOrder(self, order, isSecondaryAPI=False):
        # check send frequency
        self.timeLimitCoolDown()
        # order will be sent through rest API first
        self.orderSendLock.acquire()
        # here we use time stamp as orderID
        orderID = int(time.time()) * 100 + self.IDCount
        self.IDCount = (self.IDCount + 1) % 100
        # generate order param
        orderType = self.orderType2BitfinexOrderTypeDict[order.orderType]
        orderDict = {
            "gid": 1,
            "cid": orderID,
            "type": orderType[0],
            "symbol": "t"+self.OkexTickerToBitfinexTickerDict[order.symbol],
            "amount": format(order.amount * orderType[1], 'f'), # need to adjust the direction here
            "price": str(order.price)
        }


        # print("send order", orderDict)
        try:
            self._client.new_order(**orderDict)
            # save order id Info
            self.orderCID_strategyMap[int(orderID)] = order.getStrategyTag()
            order.setOrderID(int(orderID))
            order.updateOrderStatus(trader.defines.orderPending)
            # find position manager
            iPositionManager = self.tradeTickerPositionMap[order.symbol + "-" + order.expiry + "-" + order.getStrategyTag()]
            iPositionManager.addOpenOrder(order)
        except Exception as e:
            print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                  "failed to send order out for", self.getAPI_name(), \
                  "error info", str(e))
        self.orderSendLock.release()
        return 0


    def sendOrderBySecondaryAPI(self, order):
        pass

    def cancelOrder(self, order):
        # check order is in cancelling status, do not cancel pending order with ID -19860922
        if order.isCancel or (order.orderID == -19860922):
            return 0
        # check send frequency
        self.timeLimitCoolDown()

        self.orderSendLock.acquire()
        # find the orderID from cid saved in system
        if not order.orderID in self.orderCID_orderIDMap:
            self.orderSendLock.release()
            print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                  "failed to get the real orderID for cid", order.orderID, self.getAPI_name())
            return -1
        realOrderID = self.orderCID_orderIDMap[order.orderID]
        self.orderSendLock.release()

        # construct cancel info
        cancelInfo = {"id":realOrderID}
        try:
            self._client.cancel_order(**cancelInfo)
        except Exception as e:
            print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                  "failed to cancel order", realOrderID, str(e))
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
        #print("onTradeBack", tradeBack.printTradeBackInfo())
        # update position manager
        order = self.tradeTickerPositionMap[tradeBack.symbol + "-" + tradeBack.expire + "-" + tradeBack.strategyTag].updatePositionOnTradeBack(tradeBack)
        # process trade back in strategy
        self.strategies[tradeBack.strategyTag].onTradeFeedback(tradeBack)

        if not self.trader is None:
            self.trader.writeTradeBackInfo2File(tradeBack)
            if order:
                self.trader.writeOrderBackInfo2File(order)
        else:
            print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                  " trade back received before trader is linked to adapters", file=sys.stderr)
            print("trade info ", tradeBack.printTradeBackInfo(), file=sys.stderr)

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
        self.BitfinexTickerToOkexTickerDict = {}
        self.OkexTickerToBitfinexTickerDict = {}
        for ticker in self.workingTickers:
            # here we need to convert system ticker name to huobi ticker name
            splitStr = ticker.split("_")
            bitfinexTicker = (splitStr[0][:3] + splitStr[1][:3]).upper()
            # save into map
            self.BitfinexTickerToOkexTickerDict[bitfinexTicker] = ticker
            self.OkexTickerToBitfinexTickerDict[ticker] = bitfinexTicker
            print(self.getAPI_name(), "subscribe", bitfinexTicker, ticker)
            # init depth data
            initDepth = adapters.BitfinexStruct.Bitfinex_depth_Data()
            initDepth.initSpotDepthData(ticker, None, self.getAPI_name())
            self.depthDatas[bitfinexTicker] = initDepth

            # regiest for ticker data
            self._client.subscribe_to_ticker(bitfinexTicker)
            # regiest for order book data
            self._client.subscribe_to_order_book(bitfinexTicker)
            # regiest for market trade data
            self._client.subscribe_to_trades(bitfinexTicker)
        self.dataFeedOn = True


    def close_connect(self):
        print('---->>> onClose for adapter ', self.API_name, file=sys.stdout)
        try:
            # unsubscribe data
            for ticker in self.BitfinexTickerToOkexTickerDict.keys():
                print("unsubscribe", ticker)
                # unregiest for ticker data
                #self._client.unsubscribe_from_ticker(ticker)
                self._unsubcribe("ticker", ticker)
                # unregiest for order book data
                #self._client.unsubscribe_from_order_book(ticker)
                self._unsubcribe("book", ticker)
                # unregiest for market trade data
                #self._client.unsubscribe_from_trades(ticker)
                self._unsubcribe("trades", ticker)
        except Exception as e:
            print("Exception", str(e))

        """关闭接口"""
        self.dataFeedOn = False
        self.adapterOn = False
        time.sleep(3)
        self._client.stop()


    # -----------------------------------------------------------
    def queryPositions(self):
        # query account balance
        self.timeLimitCoolDown()
        # need to write later
        pass

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

    def processDataBack(self):
        while self.adapterOn:
            try:
                message = self._client.conn.q.get(timeout=0.1)
            except Empty:
                continue

            dtype, data, ts = message
            # print(dtype, data)
            if dtype in ('subscribed', 'unsubscribed', 'conf', 'auth', 'unauth'):
                # print(dtype, data)
                if dtype == "subscribed":
                    self._handle_subscribed(data)
                if dtype == "auth":
                    self._handle_auth_sub(data)
            elif dtype == 'data':
                # print(dtype, data)
                if data[0] in self.channel_handlers:
                    self.channel_handlers[data[0]](data)
                else:
                    print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                          "Channel ID does not have a data handler!", message)
            else:
                print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                      "dtype not defined", dtype, data)
                continue


    def _handle_subscribed(self, data):
        # print("_handle_subscribed", data)
        if data['channel'] in self._data_handlers.keys():
            self.channel_handlers[data["chanId"]] = self._data_handlers[data['channel']]
            if "pair" in data:
                self.channel_pair[data["chanId"]] = data["pair"]
                self.subscribeInfo_channelID_map[data['channel'] + "-" + data["pair"]] = data["chanId"]
                print("sub", data['channel'] + "-" + data["pair"],  data["chanId"])
        else:
            print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                  "failed to find channel", data['channel'], "in _data_handlers for adapter", self.getAPI_name())


    def _handle_ticker(self, data):
        # print("_handle_ticker", data)
        pair = self.channel_pair[data[0]]
        # update the bid and ask info for depth data
        if pair in self.depthDatas:
            self.depthDatas[pair].updateLastPrices(float(data[1][-4]), float(data[1][-3]), float(data[1][0]), float(data[1][1]), \
                                               float(data[1][2]), float(data[1][3]))
        else:
            print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                  "failed to find ticker in depth data for data", pair, data)

    def _handle_book(self, data):
        # print("_handle_book", data)
        pair = self.channel_pair[data[0]]
        if pair in self.depthDatas:
            self.depthDatas[pair].updateOrderBook(data[1])
            # send depth data to strategies
            self.onDepthData(self.depthDatas[pair])
        else:
            print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                  "failed to find ticker in depth data for data", pair, data)

    def _handle_trades(self, data):
        # print("_handle_trades", data)
        if data[0] in self.channel_pair:
            pair = self.channel_pair[data[0]]
            if self.isFirstTimeTradesList:
                self.isFirstTimeTradesList = False
                return

            if isinstance(data[1], (list, tuple)):
                for iData in data[1]:
                    if isinstance(iData, (list, tuple)):
                        dataDict = {"symbol": self.BitfinexTickerToOkexTickerDict[pair], \
                                    "API": self.API_name, \
                                    "tradeId": iData[0], \
                                    "price": float(iData[3]), \
                                    "amount": float(iData[2] / float(iData[3])), \
                                    "timestamp": int(iData[1]), \
                                    "tradeType": trader.defines.tradeBid if iData[2] > 0 else trader.defines.tradeAsk}
                        tradeData = trader.coinTradeStruct.marketOrderInfo()
                        tradeData.initSpotMarketOrderInfo(dataDict)
                        self.onMarketOrderBack(tradeData)
                    else:
                        pass
            else:
                for iData in data[2]:
                    if isinstance(iData, (list, tuple)):
                        dataDict = {"symbol": self.BitfinexTickerToOkexTickerDict[pair], \
                                    "API": self.API_name, \
                                    "tradeId": iData[0], \
                                    "price": float(iData[3]), \
                                    "amount": float(iData[2] / float(iData[3])), \
                                    "timestamp": int(iData[1]), \
                                    "tradeType": trader.defines.tradeBid if iData[2] > 0 else trader.defines.tradeAsk}
                        tradeData = trader.coinTradeStruct.marketOrderInfo()
                        tradeData.initSpotMarketOrderInfo(dataDict)
                        self.onMarketOrderBack(tradeData)
                    else:
                        iData = data[2]
                        dataDict = {"symbol": self.BitfinexTickerToOkexTickerDict[pair], \
                                    "API": self.API_name, \
                                    "tradeId": iData[0], \
                                    "price": float(iData[3]), \
                                    "amount": float(iData[2] / float(iData[3])), \
                                    "timestamp": int(iData[1]), \
                                    "tradeType": trader.defines.tradeBid if iData[2] > 0 else trader.defines.tradeAsk}
                        tradeData = trader.coinTradeStruct.marketOrderInfo()
                        tradeData.initSpotMarketOrderInfo(dataDict)
                        self.onMarketOrderBack(tradeData)
                        break
        else:
            print("[ERR]:", trader.coinUtilities.getCurrentTimeString(),\
                  "failed to find id in channel_pair", data)

    def _handle_auth_sub(self, data):
        if data['status'] == 'OK':
            self.channel_handlers[data["chanId"]] = self._handle_auth
        else:
            print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                  "user auth failed for", self.getAPI_name())

    def _handle_auth(self, data):
        # handle orders
        if data[1] == "n" and data[2][1] == "on-req":
            # update orderID_orderCID map
            self.orderSendLock.acquire()
            if data[2][4][2] in self.orderCID_strategyMap and (not data[2][4][2] in self.orderCID_orderIDMap):
                self.orderID_OrderCIDMap[data[2][4][0]] = data[2][4][2]
                self.orderCID_orderIDMap[data[2][4][2]] = data[2][4][0]
                self.orderID_strategyMap[data[2][4][0]] = self.orderCID_strategyMap[data[2][4][2]]
            self.orderSendLock.release()
        elif data[1] == "oc" or data[1] == "on" or data[1] == "ou":
            # print("order", data)
            # if orderID is not updated, update orderID here
            if not (data[2][2] in self.orderCID_strategyMap):
                print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                      "failed to find cid", data[2][2], "in orderCID_strategyMap", self.getAPI_name())
                return
            if not data[2][0] in self.orderID_OrderCIDMap:
                self.orderID_OrderCIDMap[data[2][0]] = data[2][2]
                self.orderCID_orderIDMap[data[2][2]] = data[2][0]
                self.orderSendLock.acquire()
                self.orderID_strategyMap[data[2][0]] = self.orderCID_strategyMap[data[2][2]]
                self.orderSendLock.release()
            # update status of the open order
            orderStatus = trader.defines.orderFilling
            if "PARTIALLY FILLED" in data[2][13]:
                orderStatus = trader.defines.orderFilling
            elif "CANCELED" in data[2][13]:
                orderStatus = trader.defines.orderCancelled
            elif "INSUFFICIENT" in data[2][13]:
                orderStatus = trader.defines.orderRejected
            elif "ACTIVE" in data[2][13]:
                orderStatus = trader.defines.orderFilling
            else:
                pass
            # send order status back to strategy
            # Note: need to use cid instead of orderID in the system
            orderback = trader.coinTradeStruct.coinOrder()
            data = {"apiType": self.API_name, \
                    "symbol": self.BitfinexTickerToOkexTickerDict[data[2][3][1:]], \
                    "orderID": data[2][2], \
                    "status": orderStatus, \
                    "orderUpdateTime": time.time(), \
                    "strategyTag": self.orderID_strategyMap[data[2][0]]
                    }
            orderback.setSpotOrderFeedback(data=data, isOrderBackOnly=False)
            self.onOrderBack(orderback)
        elif data[1] == "tu":
            # process trade and generate tradeback
            # print("trade", data)
            tradeBack = trader.coinTradeStruct.coinTrade()
            tradeFee = 0.
            if len(data[2]) > 9:
                tradeFee = data[2][9]
            tradeBack.initSpotTrade(apiType = self.getAPI_name(), \
                                    tradeTime = data[2][2], \
                                    symbol = self.BitfinexTickerToOkexTickerDict[data[2][1][1:]],
                                    tradePrice = data[2][5],
                                    tradeAmount = abs(data[2][4]),
                                    orderId = self.orderID_OrderCIDMap[data[2][3]],
                                    strategyTag = self.orderID_strategyMap[data[2][3]],
                                    tradeFee = tradeFee)
            self.onTradeBack(tradeBack)
        else:
            print("else", data)


    def _handle_conf(self, data):
        pass

    def _handle_raw_book(self, data):
        pass

    def _handle_candles(self, data):
        pass

    def _unsubcribe(self, field, symbol):
        # get channelID
        channelID = self.subscribeInfo_channelID_map[field+"-"+symbol]
        # print("_unsubcribe", field+"-"+symbol, channelID)
        q = {'event': 'unsubscribe', 'chanId': channelID}
        self._client.conn.send(**q)