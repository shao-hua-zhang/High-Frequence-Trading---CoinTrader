# -*- coding: utf-8 -*-
import sys
import json
import bitmex
import urllib
import time, hmac, hashlib
import threading
from threading import Thread
import pandas as pd

sys.path.append("../")
import adapters.websocket
import adapters.BitmexStruct
import trader.traderInterface
import trader.coinTradeStruct
import trader.coinUtilities
import trader.defines
import trader.locker
import trader.positionManager

class BitmexSpi_future_web(trader.traderInterface.adapterTemplate):

    def __init__(self):
        super(BitmexSpi_future_web, self).__init__()
        self._api_key = ''  # 用户名
        self._secret_key = ''  # 密码
        self._host = ''  # 服务器地址
        self._ws = None  # websocket应用对象
        self._ws_url = ''
        self._restURL = ''
        self.thread = None
        self.isTestSystem = True
        self.strategies = {}

        self.linkTime = 0
        self.lastLinkTime = 0
        self.checkConnectionFailedTimes = 0
        self.API_name = ""

        # store orderbook
        self.data = {}
        self.keys = {}

        # self.strategies = {}
        self.workingTickers = []
        self.orderID_strategyMap = {}
        self.query_time = []
        self.query_numberLimit = 300
        self.query_timeLimit = 300 # seconds
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
        self.contractSize = 20
        self.path2PositionFolder = ""
        self.allSymbolPositionMap = {}
        self.tradeTickerPositionMap = {}
        self.cashBaseValue = {}

        self.contractFreezed = {}
        self.contractAvailable = {}
        self.coinBalance = {}

        self.orderType2BitmexOrderTypeDict = {
            trader.defines.openLong_limitPrice: "Buy-Limit", \
            trader.defines.openShort_limitPrice: "Sell-Limit", \
            trader.defines.coverLong_limitPrice: "Sell-Close-Limit", \
            trader.defines.coverShort_limitPrice: "Buy-Close-Limit", \
            trader.defines.openLong_marketPrice: "Buy-Market", \
            trader.defines.openShort_marketPrice: "Sell-Market", \
            trader.defines.coverLong_marketPrice: "Sell-Close-Market", \
            trader.defines.coverShort_marketPrice: "Buy-Close-Market"
        }


        self.OrderStatus2SystemOrderStatusDict = {
            "submitting" : trader.defines.orderPending,
            "PendingNew": trader.defines.orderPending,
            "New" : trader.defines.orderFilling,
            "PartiallyFilled" : trader.defines.orderFilling,
            "DoneForDay" : trader.defines.orderFilling,
            "Stopped": trader.defines.orderFilling,
            "PendingCancel": trader.defines.orderFilling,
            "Filled": trader.defines.orderFilled,
            "Canceled": trader.defines.orderCancelled,
            "Rejected": trader.defines.orderRejected,
            "Expired": trader.defines.orderRejected
        }

        self.symbolExpiryMap = {"XBTUSD":trader.defines.bitmexForever, "XBTJPY":trader.defines.bitmexForever, "ETHXBT":trader.defines.bitmexForever, "ETHUSD":trader.defines.bitmexForever, "XBTKRW":trader.defines.bitmexForever}

        # rest api
        self.restAPI = None

    def initAdapter(self, loginInfoDict):
        self._host = loginInfoDict["webURL"]
        self._api_key = loginInfoDict["apiKey"]
        self._secret_key = loginInfoDict["secretKey"]
        self._restURL = loginInfoDict['restURL']
        self.API_name = loginInfoDict["apiName"]
        self.openFeeRate = loginInfoDict["openFeeRate"]
        self.closeFeeRate = loginInfoDict["closeFeeRate"]
        self.path2PositionFolder = loginInfoDict["path2PositionFolder"]

        if self._restURL == "test":
            self.isTestSystem = True
        else:
            self.isTestSystem = False

        # load positions in adapter
        self.loadLocalPositions()

    def loadLocalPositions(self):
        # read csv file
        positionFile = self.path2PositionFolder + "/" + self.getAPI_name() + ".csv"
        try:
            positions = pd.read_csv(positionFile)
            # create position
            for index, row in positions.iterrows():
                # only need to save the position that belong to this adapter
                if row["api"] == self.API_name:
                    iPos = trader.positionManager.openPosition(row["strategy"])
                    if row["expiry"] != "None":
                        # init spot position
                        iPos.initFuturePosition(
                            row["symbol"], row["expiry"], self.API_name,
                            float(row["long"]), float(row["short"]))
                    else:
                        print("[ERR]: obtained spot position with future API for adapter", self.getAPI_name(),
                              file=sys.stderr)
                    # set pnl info
                    iPos.setFeeRate(self.openFeeRate, self.closeFeeRate)
                    iPos.setContractSize(int(row["contractSize"]))
                    iPos.setGrossAndFee(float(row["gross"]), float(row["fee"]), float(row["lastPrice"]))
                    self.allSymbolPositionMap[row["symbol"] + "-" + row["expiry"] + "-" + row["strategy"]] = iPos
        except:
            print("[WARN]: failed to open position file", positionFile, "for adapter", self.getAPI_name(),
                  file=sys.stderr)
            print("Will init all positions as 0", file=sys.stderr)

    def setTrader(self, trader):
        '''
        link adapter to trade interface
        :param trader:
        :return:
        '''
        self.trader = trader

    def getSubscribeUrls(self, symbol):
        # for all the tickers need to add them one by one
        symbolSubs = ["instrument", "order", "orderBookL2_25", "trade", "position"]
        subscribeTickers = []
        subscriptions = []
        for ticker in set(self.workingTickers):
            # separate string to get symbol and expiry info
            splitFields = ticker.split("-")

            # do not repeatedly subscribe tickers, because workingTickers string contain strategyTag
            if (splitFields[0] + splitFields[1]) in subscribeTickers:
                continue

            # generate sub info
            subscriptions.extend([sub + ':' + splitFields[0] for sub in symbolSubs])
            # regist subed ticker
            subscribeTickers.append(splitFields[0] + splitFields[1])
            # init self.tickerLastPriceVolume
            self.tickerLastPriceVolume[splitFields[0]] = [None, None]
            # init self.data
            self.data[splitFields[0]] = {}
            self.keys[splitFields[0]] = {}
            for isymbol in symbolSubs:
                self.data[splitFields[0]][isymbol] = []
                self.keys[splitFields[0]][isymbol] = []

        print("subs", subscriptions)
        return self._host + "/realtime?subscribe={}".format(','.join(subscriptions))

    def getAuth(self):
        nonce = int(round((time.time() + 5) * 1000))
        message = ('GET' + '/realtime' + str(nonce) + '').encode('utf-8')
        signature = hmac.new(self._secret_key.encode('utf-8'), message, digestmod=hashlib.sha256).hexdigest()

        return [
            # "api-nonce: " + str(nonce),
            "api-expires: " + str(nonce),
            "api-signature: " + signature,
            "api-key:" + self._api_key
        ]

    # ZZDAI 后面把getCurrentTimeString的时间戳切到
    def startAdapter(self):
        # set info for position managers
        for i, pos in self.tradeTickerPositionMap.items():
            pos.setBaseCashValue(self.cashBaseValue)

        # rest api
        self.restAPI = bitmex.bitmex(test=self.isTestSystem, api_key=self._api_key, api_secret=self._secret_key)

        time.sleep(1)

        """连接服务器"""
        # adapters.websocket.enableTrace(False)
        # zzdai? 针对一个ticker,后期优化
        self._ws_url = self.getSubscribeUrls(self.workingTickers[0])
        self._ws = adapters.websocket.WebSocketApp(self._ws_url,
                                          on_message=self.on_message,
                                          on_error=self.on_error,
                                          on_close=self.on_close,
                                          on_open=self.on_open,
                                          header=self.getAuth())

        self.thread = Thread(target=self._ws.run_forever)
        self.thread.start()

        conn_timeout = 5
        while not self._ws.sock or not self._ws.sock.connected and conn_timeout:
            time.sleep(1)
            conn_timeout -= 1
        if not conn_timeout:
            print("[ERR]: ", adapters.coinUtilities.getCurrentTimeString(), \
                  "link to", self.getAPI_name(), "failed", file=sys.stderr)

        # query account info
        self.query_future_userinfo()
        # query positions
        # self.queryPositions()

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
        self.dataFeedOn = False
        self.adapterOn = False
        # before close connect, save positions
        self.saveTickerPositions()
        print("saved ticker positions info for adapter", self.getAPI_name(), file=sys.stdout)
        self.close_connect()

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

    def amendOrder(self, order, price, qty, isSecondaryAPI=False):
        """
        修改订单
        访问频率: 300次/5min(all apis)
        symbol:   XRPU18
        side:     Buy/Sell
        orderQty:
        price:
        ordType:  Market/Limit
        """
        # check send frequency
        self.timeLimitCoolDown()
        # order will be sent through rest API first
        self.orderSendLock.acquire()
        ret = None
        try:
            # is close
            ret = self.restAPI.Order.Order_amend(orderID=order.orderID, orderQty=qty, price=price).result()[0]
            # print("send order ret", ret)
        except Exception as e:
            self.orderSendLock.release()
            print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                  " order amend exception for strategy ", order.getStrategyTag(), ", exception=", str(e),
                  self.getAPI_name(), file=sys.stderr)

            return -1

        if not ret is None:
            if ret["ordRejReason"] != "":
                self.orderSendLock.release()
                print("[WARN]:", trader.coinUtilities.getCurrentTimeString(), \
                      "failed to send order", order.orderID, "for", self.getAPI_name(), ret)
                return -1
            else:
                # 这里要在position里修改订单的价格和量
                newPrice = ret["price"]
                newQty = ret["orderQty"]
                # self.orderID_strategyMap[orderID] = order.getStrategyTag()
                order.price = newPrice
                order.amount = newQty
                # order.setOrderID(orderID)
                # # find position manager
                # iPositionManager = self.tradeTickerPositionMap[order.symbol + "-" + order.expiry + "-" + order.getStrategyTag()]
                # iPositionManager.addOpenOrder(order)
                self.orderSendLock.release()
                return 0
        else:
            self.orderSendLock.release()
            print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                  " order send failed for strategy ", order.getStrategyTag(), file=sys.stderr)
            print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                  " errorInfo: ", ret, file=sys.stderr)
            return -1

    def amendBulkOrder(self, orders, prices, qtys, isSecondaryAPI=False):
        """
        修改订单 batch version
        访问频率: 300 *10次/5min(all apis)
        symbol:   XRPU18
        side:     Buy/Sell
        orderQty:
        price:
        ordType:  Market/Limit
        """

        # check send frequency
        self.timeLimitCoolDown()
        # order will be sent through rest API first
        self.orderSendLock.acquire()
        ret = None
        try:
            orderRequests = []
            curRequest = {}

            for id, order in enumerate(orders):
                curRequest = {
                    'orderID': order.orderID,
                    'orderQty': qtys[id] if qtys[id] is not None else order.amount,
                    'price': prices[id] if prices[id] is not None else order.price
                }
                orderRequests.append(curRequest)

            ret = self.restAPI.Order.Order_amendBulk(orders=json.dumps(orderRequests)).result()[0]

            # print("send order ret", ret)
        except Exception as e:
            self.orderSendLock.release()
            print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                  " order amend bulk exception for strategy ", orders[0].getStrategyTag(), ", exception=", str(e),
                  self.getAPI_name(), file=sys.stderr)

            return -1
        if not ret is None:
            # 如何考虑部分拒绝的情况
            oneRejected = False
            for id, subRet in enumerate(ret):
                if subRet["ordRejReason"] != "":
                    print("[WARN]:", trader.coinUtilities.getCurrentTimeString(), \
                          "failed to send order", orders[id], "for", self.getAPI_name(), subRet)
                    oneRejected = True
                else:
                    order = orders[id]
                    newPrice = subRet["price"]
                    newQty = subRet["orderQty"]
                    order.price = newPrice
                    order.amount = newQty

            self.orderSendLock.release()

            return -1 * int(oneRejected)
        else:
            self.orderSendLock.release()
            print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                  " order send failed for strategy ", orders[0].getStrategyTag(), file=sys.stderr)
            print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                  " errorInfo: ", ret, file=sys.stderr)
            return -1

    def sendBulkOrder(self, orders, isSecondaryAPI=False):
        """
        期货委托 batch version
        访问频率: 300*10次/5min(all apis)
        symbol:   XRPU18
        side:     Buy/Sell
        orderQty:
        price:
        ordType:  Market/Limit
        """
        # check send frequency
        self.timeLimitCoolDown()
        # order will be sent through rest API first
        self.orderSendLock.acquire()
        ret = None
        try:
            orderRequests = []
            curRequest = {}

            for order in orders:
                if order.orderType in [trader.defines.coverLong_limitPrice, trader.defines.coverShort_limitPrice,
                                       trader.defines.coverLong_marketPrice, trader.defines.coverShort_marketPrice]:
                    side = self.orderType2BitmexOrderTypeDict[order.orderType].split("-")[0]
                    execinst = self.orderType2BitmexOrderTypeDict[order.orderType].split("-")[1]
                    ordtype = self.orderType2BitmexOrderTypeDict[order.orderType].split("-")[2]
                    curRequest = {
                        'symbol': order.symbol,
                        'side': side,
                        'orderQty': order.amount,
                        'price': order.price,
                        'ordType': ordtype,
                        'execInst': execinst
                    }
                # print("send order ret", ret)
                else:
                    side = self.orderType2BitmexOrderTypeDict[order.orderType].split("-")[0]
                    ordtype = self.orderType2BitmexOrderTypeDict[order.orderType].split("-")[1]
                    curRequest = {
                        'symbol': order.symbol,
                        'side': side,
                        'orderQty': order.amount,
                        'price': order.price,
                        'ordType': ordtype
                    }
                orderRequests.append(curRequest)

            ret = self.restAPI.Order.Order_newBulk(orders=json.dumps(orderRequests)).result()[0]
            # print("send order ret", ret)
        except Exception as e:
            self.orderSendLock.release()
            print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                  " order send bulk exception for strategy ", orders[0].getStrategyTag(), ", exception=", str(e),
                  self.getAPI_name(), file=sys.stderr)

            return -1
        if not ret is None:
            # 如何考虑部分拒绝的情况
            oneRejected = False
            for id, subRet in enumerate(ret):
                if subRet["ordRejReason"] != "":
                    print("[WARN]:", trader.coinUtilities.getCurrentTimeString(), \
                          "failed to send order", orders[id], "for", self.getAPI_name(), subRet)
                    oneRejected = True
                else:

                    order = orders[id]
                    orderID = subRet["orderID"]
                    # print('id: ', orderID)
                    self.orderID_strategyMap[orderID] = order.getStrategyTag()
                    order.setOrderID(orderID)
                    # find position manager
                    iPositionManager = self.tradeTickerPositionMap[
                        order.symbol + "-" + order.expiry + "-" + order.getStrategyTag()]
                    iPositionManager.addOpenOrder(order)
            self.orderSendLock.release()

            # print('order dict:')
            # for order in iPositionManager.getOpenOrders():
            #     print(order.__dict__)

            return -1 * int(oneRejected)
        else:
            self.orderSendLock.release()
            print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                  " order send failed for strategy ", orders[0].getStrategyTag(), file=sys.stderr)
            print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                  " errorInfo: ", ret, file=sys.stderr)
            return -1

    def sendOrder(self, order, isSecondaryAPI=False):
        """
        期货委托
        访问频率: 300次/5min(all apis)
        symbol:   XRPU18
        side:     Buy/Sell
        orderQty:
        price:
        ordType:  Market/Limit
        """
        # check send frequency
        self.timeLimitCoolDown()
        # order will be sent through rest API first
        self.orderSendLock.acquire()
        ret = None
        try:
            # is close
            if order.orderType in [trader.defines.coverLong_limitPrice, trader.defines.coverShort_limitPrice,
                                   trader.defines.coverLong_marketPrice, trader.defines.coverShort_marketPrice]:
                side = self.orderType2BitmexOrderTypeDict[order.orderType].split("-")[0]
                execinst = self.orderType2BitmexOrderTypeDict[order.orderType].split("-")[1]
                ordtype = self.orderType2BitmexOrderTypeDict[order.orderType].split("-")[2]
                ret = \
                    self.restAPI.Order.Order_new(symbol=order.symbol, side=side, orderQty=order.amount,
                                                 price=order.price,
                                                 ordType=ordtype, execInst=execinst).result()[0]
                # print("send order ret", ret)
            else:
                side = self.orderType2BitmexOrderTypeDict[order.orderType].split("-")[0]
                ordtype = self.orderType2BitmexOrderTypeDict[order.orderType].split("-")[1]
                ret = \
                    self.restAPI.Order.Order_new(symbol=order.symbol, side=side, orderQty=order.amount,
                                                 price=order.price,
                                                 ordType=ordtype).result()[0]
                # print("send order ret", ret)
        except Exception as e:
            self.orderSendLock.release()
            print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                  " order send exception for strategy ", order.getStrategyTag(), ", exception=", str(e), self.getAPI_name(),file=sys.stderr)

            return -1
        if not ret is None:
            if ret["ordRejReason"] != "":
                self.orderSendLock.release()
                print("[WARN]:", trader.coinUtilities.getCurrentTimeString(), \
                      "failed to send order", order.orderID, "for", self.getAPI_name(), ret)
                return -1
            else:
                orderID = ret["orderID"]
                self.orderID_strategyMap[orderID] = order.getStrategyTag()
                order.setOrderID(orderID)
                # find position manager
                iPositionManager = self.tradeTickerPositionMap[order.symbol + "-" + order.expiry + "-" + order.getStrategyTag()]
                iPositionManager.addOpenOrder(order)
                self.orderSendLock.release()
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
        if order.isCancel:
            return 0
        # check send frequency
        self.timeLimitCoolDown()
        try:
            ret = self.restAPI.Order.Order_cancel(orderID=order.orderID).result()[0][0]
            # print("cancel order ret", ret)
        except:
            print("[WARN]:", trader.coinUtilities.getCurrentTimeString(), \
                  "failed to cancel order", order.orderID, "for ", self.getAPI_name(), ret)
            return -1
        if ret.__contains__("error"):
            print("[WARN]:", trader.coinUtilities.getCurrentTimeString(), \
                  "failed to cancel order", order.orderID, "for ", self.getAPI_name(), ", errmsg=", ret["error"])
            return -1
        else:
            order.isCancel = True
        return 0

    def onOrderBack(self, orderBack):
        # update position manager first
        self.tradeTickerPositionMap[orderBack.symbol + "-" + orderBack.expiry + "-" + \
                                    orderBack.strategyTag].updateOrderStatusOnOrderBack(orderBack)
        # process order back in strategy
        self.strategies[orderBack.strategyTag].onOrderFeedback(orderBack)

        if not self.trader is None:
            self.trader.writeOrderBackInfo2File(orderBack)
        else:
            print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                  " [huobiAPI] order back received before trader is linked to adapters", file=sys.stderr)
            print("order info ", orderBack.printOrderBackInfo(), file=sys.stderr)

    def onTradeBack(self, tradeBack):
        '''
        not used in this adapter
        :param tradeBack:
        :return:
        '''
        if not self.trader is None:
            self.trader.writeTradeBackInfo2File(tradeBack)
        else:
            print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                  " trade back received before trader is linked to adapters", file=sys.stderr)
            print("order info ", tradeBack.printTradeBackInfo(), file=sys.stderr)

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
                if (not iWorkingTickerIsSpot[i]) and (iAPIs[i] == self.getAPI_name()):
                    tickerTag = iTicker + "-" + iWorkingTickerExpires[i] + "-" + strategy.getStrategyTag()
                    self.workingTickers.append(tickerTag)
                    # check if the ticker position is created
                    if tickerTag in self.allSymbolPositionMap:
                        self.tradeTickerPositionMap[tickerTag] = self.allSymbolPositionMap[tickerTag]
                    else:
                        # create new position
                        iPos = trader.positionManager.openPosition(strategy.getStrategyTag())
                        iPos.initFuturePosition(iTicker, iWorkingTickerExpires[i], iAPIs[i], 0, 0)
                        iPos.setFeeRate(self.openFeeRate, self.closeFeeRate)
                        iPos.setContractSize(self.contractSize)
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
                  "failed to get", symbol, expiry, strategyTag, "in tradeTickerPositionMap for ", self.getAPI_name(),
                  file=sys.stderr)
            return 0

    def getTickerShortPosition(self, symbol, expiry, strategyTag):
        try:
            return self.tradeTickerPositionMap[symbol + "-" + expiry + "-" + strategyTag].getShortShare()
        except:
            print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                  "failed to get", symbol, expiry, strategyTag, "in tradeTickerPositionMap for ", self.getAPI_name(),
                  file=sys.stderr)
            return 0

    def getTickerOpenOrders(self, symbol, expiry, strategyTag):
        try:
            return self.tradeTickerPositionMap[symbol + "-" + expiry + "-" + strategyTag].getOpenOrders()
        except:
            print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                  "failed to get", symbol, expiry, strategyTag, "in tradeTickerPositionMap for ", self.getAPI_name(),
                  file=sys.stderr)
            return []

    def setBaseCashValue(self, cashBaseValue):
        self.cashBaseValue = cashBaseValue

    def reconnect(self):
        if not self.adapterOn:
            return
        """重新连接"""
        # 首先关闭之前的连接
        self.close_connect()

        print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
              "10s后重新连接", file=sys.stderr)
        # 延时
        time.sleep(10)
        # 再执行重连任务
        # zzdai? 针对一个ticker,后期优化
        self._ws_url = self.getSubscribeUrls(self.workingTickers[0])
        self._ws = adapters.websocket.WebSocketApp(self._ws_url,
                                          on_message=self.on_message,
                                          on_error=self.on_error,
                                          on_close=self.on_close,
                                          on_open=self.on_open,
                                          header=self.getAuth())

        self.thread = Thread(target=self._ws.run_forever)
        self.thread.start()

        conn_timeout = 5
        while not self._ws.sock or not self._ws.sock.connected and conn_timeout:
            time.sleep(1)
            conn_timeout -= 1
        if not conn_timeout:
            print("[ERR]: ", trader.coinUtilities.getCurrentTimeString(), \
                  "link to", self.getAPI_name(), "failed", file=sys.stderr)

        # self._ws.run_forever()
        self.thread = Thread(target=self._ws.run_forever)
        self.thread.start()

    def close_connect(self):

        """关闭接口"""
        if self.thread and self.thread.isAlive():
            self._ws.close()

        # -------------------------- 行情订阅 -------------------------- #

    def queryPositions(self):
        # pass position query, will be obtained in wss
        # next query userinfo for balance info
        self.query_future_userinfo()

    def query_future_userinfo(self):
        try:
            info = self.restAPI.User.User_getMargin().result()
            # print(type(info), info)
        except:
            print("[ERR]: ", trader.coinUtilities.getCurrentTimeString(), \
                  "failed to get user account info for api part1", self.getAPI_name(), file=sys.stderr)
            print(info)
        else:
            # process the data
            self.positionQueryLock.write_acquire()
            try:
                iInfo = info[0]
                self.coinBalance[iInfo["currency"]] = iInfo["amount"]
            except:
                print("[ERR]: ", trader.coinUtilities.getCurrentTimeString(), \
                      "failed to get user account info for api part2", self.getAPI_name(), file=sys.stderr)
            self.positionQueryLock.write_release()

    def getFreePosition(self, ticker, expiry, diretion="long"):
        self.positionQueryLock.read_acquire()
        try:
            value = self.contractAvailable[ticker + "_" + expiry + "_" + diretion]
        except:
            self.positionQueryLock.read_release()
            return None
        self.positionQueryLock.read_release()
        return value

    def getFreezedPosition(self, ticker, expiry, diretion="long"):
        self.positionQueryLock.read_acquire()
        try:
            value = self.contractFreezed[ticker + "_" + expiry + "_" + diretion]
        except:
            self.positionQueryLock.read_release()
            return None
        self.positionQueryLock.read_release()
        return value

    def getCoinBalance(self, coin):
        self.positionQueryLock.read_acquire()
        try:
            value = self.coinBalance[coin]
        except:
            self.positionQueryLock.read_release()
            return None
        self.positionQueryLock.read_release()
        return value

    def getServerTime(self, timestamp):
        time_format = "%Y-%m-%dT%H:%M:%S.%fZ"
        time_struct = time.strptime(timestamp, time_format)
        return int(time.mktime(time_struct) * 1000) + int(timestamp[-4:-1])

    def findItemByKeys(self, keys, table, matchData):
        for item in table:
            matched = True
            for key in keys:
                if item[key] != matchData[key]:
                    matched = False
            if matched:
                return item

    def on_message(self, ws, message):
        message = json.loads(message)
        table = message['table'] if 'table' in message else None
        action = message['action'] if 'action' in message else None
        # print(message)
        # if table in []:
        #     print(trader.coinUtilities.getCurrentTimeString(), '\nThe message is')
        #     print(message)
        try:
            if 'subscribe' in message:
                print(message)
                print("Subscribed to %s." % message['subscribe'], file=sys.stdout)
            elif action:
                if action == "partial":
                    # orderBookL2_25
                    if table == "orderBookL2_25":
                        # print(message)
                        # find symbol data
                        symbol = message['data'][0]["symbol"]
                        if symbol not in self.data:
                            return

                        if table not in self.data[symbol]:
                            self.data[symbol][table] = []
                        self.data[symbol][table] += message['data']
                        self.keys[symbol][table] = message['keys']
                        depthdata = adapters.BitmexStruct.Bitmex_depth_Data()
                        expiry = self.symbolExpiryMap.get(symbol, trader.defines.bitmexFuture)
                        depthdata.initFutureDepthData(self.data[symbol][table], expiry, self.API_name)
                        # depthdata.printDepthData()
                        # add last price and total volume
                        if (not self.tickerLastPriceVolume[symbol][0] is None) and (
                                not self.tickerLastPriceVolume[symbol][1] is None):
                            depthdata.LastPrice = float(self.tickerLastPriceVolume[symbol][0])
                            depthdata.Volume = int(self.tickerLastPriceVolume[symbol][1])
                            self.onDepthData(depthdata)
                        return
                    elif table == "trade":
                        for idata in message['data']:
                            expiry = self.symbolExpiryMap.get(idata["symbol"], trader.defines.bitmexFuture)
                            dataDict = {"symbol": idata["symbol"], \
                                        "API": self.API_name, \
                                        "expiry": expiry, \
                                        "tradeId": idata["trdMatchID"], \
                                        "price": idata["price"], \
                                        "amount": idata["size"], \
                                        "timestamp": self.getServerTime(idata["timestamp"]), \
                                        "tradeType": trader.defines.tradeAsk if idata["side"] == "Sell" else trader.defines.tradeBid}
                            data = trader.coinTradeStruct.marketOrderInfo()
                            data.initFutureMarketOrderInfo(dataDict)
                            # print(data.printMarketOrderInfo())
                            self.onMarketOrderBack(data)
                            return
                    elif table == "instrument":
                        # print(message)
                        # try to find the totalVolume, lastPrice, lastPriceProtected, and fairPrice
                        for iData in message["data"]:
                            self.tickerLastPriceVolume[iData["symbol"]] = [iData["lastPrice"], iData["totalVolume"]]
                    elif table == "position":
                        self.on_position_info(message)
                    else:
                        print(message)
                        return
                elif action == 'insert':
                    # orderBookL2_25
                    if table == "orderBookL2_25":
                        # print(message)
                        symbol = message['data'][0]["symbol"]
                        if symbol not in self.data:
                            return

                        if table not in self.data[symbol]:
                            self.data[symbol][table] = []

                        self.data[symbol][table] += message['data']
                        depthdata = adapters.BitmexStruct.Bitmex_depth_Data()
                        expiry = self.symbolExpiryMap.get(symbol, trader.defines.bitmexFuture)
                        depthdata.initFutureDepthData(self.data[symbol][table], expiry, self.API_name)
                        # depthdata.printDepthData()
                        # add last price and total volume

                        if (not self.tickerLastPriceVolume[symbol][0] is None) and (
                        not self.tickerLastPriceVolume[symbol][1] is None):
                            depthdata.LastPrice = float(self.tickerLastPriceVolume[symbol][0])
                            depthdata.Volume = int(self.tickerLastPriceVolume[symbol][1])
                            self.onDepthData(depthdata)
                        return
                    elif table == "trade":
                        for idata in message['data']:
                            expiry = self.symbolExpiryMap.get(idata["symbol"], trader.defines.bitmexFuture)
                            dataDict = {"symbol": idata["symbol"], \
                                        "API": self.API_name, \
                                        "expiry": expiry, \
                                        "tradeId": idata["trdMatchID"], \
                                        "price": idata["price"], \
                                        "amount": idata["size"], \
                                        "timestamp": self.getServerTime(idata["timestamp"]), \
                                        "tradeType": trader.defines.tradeAsk if idata["side"] == "Sell" else trader.defines.tradeBid}
                            data = trader.coinTradeStruct.marketOrderInfo()
                            data.initFutureMarketOrderInfo(dataDict)
                            # print(data.printMarketOrderInfo())
                            self.onMarketOrderBack(data)
                        return
                    elif table == "instrument":
                        # print(message)
                        for iData in message["data"]:
                            # try to find the totalVolume, lastPrice, lastPriceProtected, and fairPrice
                            if "lastPrice" in iData.keys():
                                self.tickerLastPriceVolume[iData["symbol"]][0] = iData["lastPrice"]
                            if "totalVolume" in message["data"].keys():
                                self.tickerLastPriceVolume[iData["symbol"]][1] = iData["totalVolume"]
                    elif table == "order":
                        self.on_return_my_trade(message)
                        return
                    elif table == "position":
                        self.on_position_info(message)
                    else:
                        print(message)
                        return
                elif action == "update":
                    # orderBookL2_25
                    if table == "orderBookL2_25":
                        # print(message)
                        symbol = message['data'][0]["symbol"]
                        if symbol not in self.data:
                            return

                        if table not in self.data[symbol]:
                            self.data[symbol][table] = []
                            return

                        for updateData in message['data']:
                            item = self.findItemByKeys(self.keys[symbol][table], self.data[symbol][table], updateData)
                            if not item:
                                return  # No item found to update. Could happen before push
                            item.update(updateData)
                        depthdata = adapters.BitmexStruct.Bitmex_depth_Data()
                        expiry = self.symbolExpiryMap.get(symbol, trader.defines.bitmexFuture)
                        depthdata.initFutureDepthData(self.data[symbol][table], expiry, self.API_name)
                        # add last price and total volume
                        if (not self.tickerLastPriceVolume[symbol][0] is None) and (
                        not self.tickerLastPriceVolume[symbol][1] is None):
                            depthdata.LastPrice = float(self.tickerLastPriceVolume[symbol][0])
                            depthdata.Volume = int(self.tickerLastPriceVolume[symbol][1])
                            self.onDepthData(depthdata)
                        return
                    elif table == "instrument":
                        # print(message)
                        for iData in message["data"]:
                            # try to find the totalVolume, lastPrice, lastPriceProtected, and fairPrice
                            if "lastPrice" in iData.keys():
                                self.tickerLastPriceVolume[iData["symbol"]][0] = iData["lastPrice"]
                            if "totalVolume" in iData.keys():
                                self.tickerLastPriceVolume[iData["symbol"]][1] = iData["totalVolume"]
                    elif table == "order":
                        self.on_return_my_trade(message)
                        return
                    elif table == "position":
                        self.on_position_info(message)
                    else:
                        print(message)
                        return
                elif action == 'delete':
                    # orderBookL2_25
                    if table == "orderBookL2_25":
                        # print(message)
                        symbol = message['data'][0]["symbol"]
                        if symbol not in self.data:
                            return

                        if table not in self.data[symbol]:
                            self.data[symbol][table] = []
                            return

                        for deleteData in message['data']:
                            item = self.findItemByKeys(self.keys[symbol][table], self.data[symbol][table], deleteData)
                            self.data[symbol][table].remove(item)
                        depthdata = adapters.BitmexStruct.Bitmex_depth_Data()
                        expiry = self.symbolExpiryMap.get(symbol, trader.defines.bitmexFuture)
                        depthdata.initFutureDepthData(self.data[symbol][table], expiry, self.API_name)
                        # depthdata.printDepthData()
                        # add last price and total volume
                        if (not self.tickerLastPriceVolume[symbol][0] is None) and (
                        not self.tickerLastPriceVolume[symbol][1] is None):
                            depthdata.LastPrice = float(self.tickerLastPriceVolume[symbol][0])
                            depthdata.Volume = int(self.tickerLastPriceVolume[symbol][1])
                            self.onDepthData(depthdata)
                        return
                    elif table == "order":
                        self.on_return_my_trade(message)
                        return
                    elif table == "position":
                        self.on_position_info(message)
                    else:
                        print(message)
                        return
                else:
                    print("Unknown action: %s" % action, file=sys.stderr)
        except Exception as e:
            print("On message parse ", e, file=sys.stderr)

    def on_open(self, ws):
        """连接成功，接口打开"""
        print("---->>> Connect Successfully for adapter ", self.API_name, file=sys.stdout)
        time.sleep(1)

        # self.queryPositions()
        time.sleep(2.0)

    def on_close(self, ws):
        """接口断开"""
        print('---->>> onClose for adapter ', self.API_name, file=sys.stdout)

        # 重新连接
        if self.adapterOn:
            print("[ERR]: ", trader.coinUtilities.getCurrentTimeString(), "connection closed for adapter", \
                  self.API_name, file=sys.stderr)
            self.reconnect()

    def on_error(self, ws, evt):
        """错误推送"""
        print("[ERR]: ", trader.coinUtilities.getCurrentTimeString(), 'onError', file=sys.stderr)
        print(evt, file=sys.stderr)

    def on_return_my_trade(self, message):
        print(message)
        self.orderSendLock.acquire()
        self.orderSendLock.release()
        for iData in message["data"]:
            orderID = iData["orderID"]
            if orderID in self.orderID_strategyMap:
                # process trade data
                orderback = trader.coinTradeStruct.coinOrder()
                # get symbol and expiry
                filledQty = 0
                if "cumQty" in iData.keys():
                    filledQty = iData["cumQty"]
                avgPx = 0.
                if "avgPx" in iData.keys():
                    avgPx = iData["avgPx"]
                if "ordStatus" not in iData.keys():
                    continue
                orderStatus = self.OrderStatus2SystemOrderStatusDict.get(iData["ordStatus"], None)
                if orderStatus is None:
                    orderStatus = trader.defines.orderFilling
                expiry = self.symbolExpiryMap.get(iData["symbol"], trader.defines.bitmexFuture)
                data = {"apiType": self.API_name, \
                        "symbol" : iData["symbol"], \
                        "expiry" : expiry, \
                        "orderID" : orderID, \
                        "status" : orderStatus, \
                        "deal_amount" : filledQty, \
                        "orderUpdateTime" : time.time(), \
                        "strategyTag" : self.orderID_strategyMap[orderID], \
                        "fillPrice": avgPx}
                orderback.setFutureOrderFeedback(data = data, isOrderBackOnly = True)
                self.onOrderBack(orderback)
            else:
                print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                      "failed to obtain orderID on orderback: ", orderID, self.getAPI_name(),\
                      file=sys.stderr)

    def on_position_info(self, message):
        print(message)
        updatedSymbols = []
        for iData in message["data"]:
            symbol = iData["symbol"]
            qty = iData["currentQty"]
            expiry = self.symbolExpiryMap.get(symbol, trader.defines.bitmexFuture)
            tag_long = iData["symbol"] + "_" + expiry + "_long"
            tag_short = iData["symbol"] + "_" + expiry + "_short"
            self.contractAvailable[tag_long] = qty
            self.contractFreezed[tag_long] = 0
            self.contractAvailable[tag_short] = qty
            self.contractFreezed[tag_short] = 0
            updatedSymbols.append(tag_long)
            updatedSymbols.append(tag_short)
        # for not updated tags, values should be 0
        for key in self.contractAvailable.keys():
            if key in updatedSymbols:
                continue
            else:
                self.contractAvailable[key] = 0
                self.contractFreezed[key] = 0
