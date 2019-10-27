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
import adapters.HuobiStruct
import adapters.huobiRest.HuobiServices
import trader.locker
import threading
import pandas as pd
from websocket import create_connection
import gzip
import time
from queue import Queue, Empty

class HuobiSpi_spot_web(trader.traderInterface.adapterTemplate):
    def __init__(self):
        super(HuobiSpi_spot_web, self).__init__()
        self._api_key = ''  # 用户名
        self._secret_key = ''  # 密码
        self._host = ''  # 服务器地址
        self._ws = None  # websocket应用对象
        self._restURL = ''
        self.thread = None
        self.orderInfoThread = None
        self.linkTimerThread = None
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
        self.openOrderIDQueue = Queue()
        self.openOrderIDOrderStatusDict = {}

        # dict to convert the ticker names
        self.HuobiTickerToOkexTickerDict = {}
        self.OkexTickerToHuobiTickerDict = {}

        self.orderType2HuobiOrderTypeDict = {
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
            "submitting" : trader.defines.orderPending,
            "submitted" : trader.defines.orderFilling,
            "partial-filled" : trader.defines.orderFilling,
            "partial-canceled" : trader.defines.orderCancelled,
            "filled" : trader.defines.orderFilled,
            "canceled" : trader.defines.orderCancelled
        }
        # rest api
        self.restAPI = None
    # ------------------------------------------------------------
    def checkConnection(self):
        while self.dataFeedOn:
            if self.lastLinkTime == self.linkTime:
                self.checkConnectionFailedTimes += 1
                if self.checkConnectionFailedTimes > 60:
                    self.checkConnectionFailedTimes = 0
                    self.reconnect()
            else:
                self.lastLinkTime = self.linkTime
                self.checkConnectionFailedTimes = 0
            # sleep for 10 second
            time.sleep(1)


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
        linckCount = 0
        self.lastLinkTime = 1
        self.checkConnectionFailedTimes = 0
        self.adapterOn = True
        self.dataFeedOn = True
        print("host", self._host)
        while (1):
            try:
                self._ws = create_connection(self._host)
                break
            except:
                print('connect', self.API_name, 'error,retry...')
                time.sleep(5)
                linckCount += 1
                if linckCount > 5:
                    print("[ERR]: ", trader.coinUtilities.getCurrentTimeString(), \
                          "link to", self.getAPI_name(), "failed")
                    break

        # rest api
        self.restAPI = adapters.huobiRest.HuobiServices.HuobiRestAPI(self._api_key, self._secret_key)
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
        '''
        # close timer thread
        if self.linkTimerThread and self.linkTimerThread.isAlive():
            try:
                self.linkTimerThread.join()
            except:
                pass
        '''
    def stopAdapter(self):
        self.adapterOn = False
        # before close connect, save positions
        self.saveTickerPositions()
        print("saved ticker positions info for adapter", self.getAPI_name())
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

    def sendOrder(self, order, isSecondaryAPI=False):
        # check send frequency
        self.timeLimitCoolDown()
        # order will be sent through rest API first
        self.orderSendLock.acquire()
        ret = None
        try:
            #print("send order", self.OkexTickerToHuobiTickerDict[order.symbol], self.orderType2HuobiOrderTypeDict[order.orderType], str(order.price), str(order.amount))
            ret = self.restAPI.send_order(amount=str(order.amount), \
                                          symbol=self.OkexTickerToHuobiTickerDict[order.symbol], \
                                          orderType=self.orderType2HuobiOrderTypeDict[order.orderType], \
                                          price=str(order.price))

        except:
            self.orderSendLock.release()
            print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                  " order send failed for strategy ", order.getStrategyTag(), file=sys.stderr)
            return -1

        if (not ret is None) and (ret["status"] == "ok"):
            orderID = int(ret["data"])
            self.orderID_strategyMap[orderID] = order.getStrategyTag()
            order.setOrderID(orderID)
            # find position manager
            iPositionManager = self.tradeTickerPositionMap[order.symbol + "-" + order.expiry + "-" + order.getStrategyTag()]
            iPositionManager.addOpenOrder(order)
            self.orderSendLock.release()

            self.openOrderIDListLock.write_acquire()
            self.openOrderIDOrderStatusDict[orderID] = ["init", 0.]
            self.openOrderIDListLock.write_release()
            self.openOrderIDQueue.put(orderID)
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
        if order.isCancel or (order.orderID == -19860922):
            return 0
        # check send frequency
        self.timeLimitCoolDown()

        ret = None
        try:
            ret = self.restAPI.cancel_order(str(order.orderID))
        except:
            print("[WARN]:", trader.coinUtilities.getCurrentTimeString(), \
                  "restAPI exception, failed to cancel order", order.orderID, "for", self.getAPI_name(), ret)
            return -1
        if ret is None or ret["status"] != "ok":
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

    # -------------------------- 连接 --------------------------

    def reconnect(self):
        print("[ERR]: ", trader.coinUtilities.getCurrentTimeString(), \
              "adatpter", self.getAPI_name(), "link failed. will reconnect in 10s", file=sys.stderr)

        """重新连接"""
        # 首先关闭之前的连接print
        self.close_connect()

        # 延时
        time.sleep(6)

        # 再执行重连任务
        self.startAdapter()

    # ---------------------------------------------------- #
    def regiestTickers(self):
        self.HuobiTickerToOkexTickerDict = {}
        self.OkexTickerToHuobiTickerDict = {}
        for ticker in self.workingTickers:
            # here we need to convert system ticker name to huobi ticker name
            splitStr = ticker.split("_")
            huobiTicker = splitStr[0] + splitStr[1]
            # save into map
            self.HuobiTickerToOkexTickerDict[huobiTicker] = ticker
            self.OkexTickerToHuobiTickerDict[ticker] = huobiTicker
            print(self.getAPI_name(), "subscribe", huobiTicker, ticker)
            detailStr = """{"sub": "market.""" + huobiTicker + """.detail", "id": "id12"}"""
            depthStr = """{"sub": "market.""" + huobiTicker + """.depth.step0", "id": "id10"}"""
            tradeStr = """{"sub": "market.""" + huobiTicker + """.trade.detail", "id": "id10"}"""
            # init lastprice and volume
            self.tickerLastPriceVolume[huobiTicker] = [None, None]
            # subscribe data
            self._ws.send(detailStr)
            self._ws.send(depthStr)
            self._ws.send(tradeStr)

        # start a thread to query order info
        self.orderInfoThread = Thread(target=self.restQueryInfos)
        self.orderInfoThread.start()

        # then start a separate thread to collect data
        self.thread = Thread(target=self.dataCollector)
        self.thread.start()

        # finally, start a thread to
        self.linkTimerThread = Thread(target=self.checkConnection)
        self.linkTimerThread.start()

    def dataCollector(self):
        while (1):
            if self.adapterOn:
                try:
                    compressData = self._ws.recv()
                    result = gzip.decompress(compressData).decode('utf-8')
                    if result[:7] == '{"ping"':
                        # echo pong
                        ts = result[8:21]
                        pong = '{"pong":' + ts + '}'
                        self._ws.send(pong)
                        self.linkTime = time.time()
                    else:
                        self.processData(result)
                except:
                    pass
            else:
                break

    def close_connect(self):
        print('---->>> onClose for adapter ', self.API_name, file=sys.stdout)
        try:
            self._ws.close()
        except Exception as e:
            print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                  "exception obtained when closing connection for ", self.getAPI_name(), str(e))
        """关闭接口"""
        self.dataFeedOn = False
        self.adapterOn = False
        time.sleep(3)
        '''
        if self.thread and self.thread.isAlive():
            try:
                self.thread.join()
            except:
                pass
        if self.orderInfoThread and self.orderInfoThread.isAlive():
            try:
                self.orderInfoThread.join()
            except:
                pass
        if self.linkTimerThread and self.linkTimerThread.isAlive():
            try:
                self.linkTimerThread.join()
            except:
                pass
        '''

    # ------------------------------------------------------------
    def processData(self, data):
        json_data = json.loads(data)
        # print(json_data)
        if "ch" in json_data:
            ch = json_data["ch"]
            splitStr = ch.split(".")
            # print(data)
            # print(splitStr)
            if "depth" in splitStr:
                # process depth data
                self.processDepthData(json_data)
            elif "detail" in splitStr:
                if "trade" in splitStr:
                    # process trade detail
                    self.processTradeData(json_data)
                else:
                    # process tick detail data
                    self.processDetailData(json_data)
            else:
                # not defined yet
                print(json_data)
                pass

    # -----------------------------------------------------------
    def processDepthData(self, data):
        # print(data)
        split_channel = data["ch"].split(".")
        symbol = split_channel[1]
        systemSymbol = self.HuobiTickerToOkexTickerDict[symbol]
        depthData = adapters.HuobiStruct.Huobi_depth_Data()
        depthData.initSpotDepthData(systemSymbol, data, self.API_name)
        # get last price and volume
        if self.tickerLastPriceVolume[symbol][0]:
            depthData.LastPrice = float(self.tickerLastPriceVolume[symbol][0])
            depthData.Volume = float(self.tickerLastPriceVolume[symbol][1])
            self.onDepthData(depthData)

    # -----------------------------------------------------------
    def processDetailData(self, data):
        # get last price and volume
        split_channel = data["ch"].split(".")
        symbol = split_channel[1]
        self.tickerLastPriceVolume[symbol] = [data["tick"]["close"], data["tick"]["amount"]] # "vol" is amount * price
    # ------------------------------------------------------------
    def processTradeData(self, data):
        # print(data)
        # trade
        split_channel = data["ch"].split(".")
        symbol = split_channel[1]
        for idata in data["tick"]["data"]:
            dataDict = {"symbol": self.HuobiTickerToOkexTickerDict[symbol], \
                        "API": self.API_name, \
                        "tradeId": idata["id"], \
                        "price": float(idata["price"]), \
                        "amount": float(idata["amount"]), \
                        "timestamp": idata["ts"], \
                        "tradeType": trader.defines.tradeBid if idata["direction"] == "buy" else trader.defines.tradeAsk}
            tradeData = trader.coinTradeStruct.marketOrderInfo()
            tradeData.initSpotMarketOrderInfo(dataDict)
            if self.dataFeedOn:
                for strategy in self.strategies.values():
                    strategy.onRtnTrade(tradeData)

    # -----------------------------------------------------------
    def queryPositions(self):
        # query account balance
        self.timeLimitCoolDown()
        ret = None
        try:
            ret = self.restAPI.get_balance()
        except:
            print("[WARN]:", trader.coinUtilities.getCurrentTimeString(), \
                  "failed to obtain account info for", self.getAPI_name())
            return

        if (not ret is None) and (ret["status"] == "ok"):
            if ret["data"]["type"] == "spot":
                positionList = ret["data"]["list"]
                self.positionQueryLock.write_acquire()
                for iCoinData in positionList:
                    if iCoinData["type"] == "trade":
                        self.eachTickerFreePosition[iCoinData["currency"]] = float(iCoinData["balance"])
                    elif iCoinData["type"] == "frozen":
                        self.eachTickerFreezedPosition[iCoinData["currency"]] = float(iCoinData["balance"])
                    else:
                        pass
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
    # rest API query infos
    def restQueryInfos(self):
        while self.adapterOn:
            # query order status
            # print("query")
            # pop out order id in ID queue
            id = 0
            try:
                id = self.openOrderIDQueue.get(block=True, timeout=1)  # 获取事件的阻塞时间设为1秒
            except Empty:
                continue

            self.orderSendLock.acquire()
            if id in self.orderID_strategyMap:
                strategyTag = self.orderID_strategyMap[id]
                self.orderSendLock.release()
                # check send frequency
                self.timeLimitCoolDown()
                ret = None
                try:
                    # print("try to query order for id", id)
                    ret = self.restAPI.order_info(str(id))
                except:
                    print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                          "failed to get order status for id", str(id), self.getAPI_name(), " will get it later again")
                    self.openOrderIDQueue.put(id)
                    time.sleep(0.15)
                    continue

                print("[ORDERBACK]", ret, file=sys.stderr)

                if (ret is None) or (ret["status"] != "ok"):
                    print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                          "obtained error order query back", ret, file=sys.stderr)
                    self.openOrderIDQueue.put(id)
                    time.sleep(0.15)
                    continue
                orderInfo = ret["data"]
                # process the orderInfo
                filledAmount = float(orderInfo["field-amount"])
                orderStatus = orderInfo["state"]
                fillPrice = -1.0
                # if the order is partial-filled and canceled, query the trade amount again
                if orderStatus == "partial-canceled":
                    retMatch = None
                    time.sleep(0.1)
                    try:
                        retMatch = self.restAPI.order_matchresults(str(id))
                    except:
                        print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                              "failed to get partial-canceled order status for id", str(id), self.getAPI_name(),
                              " will get it later again")
                        self.openOrderIDQueue.put(id)
                        time.sleep(0.15)
                        continue
                    else:
                        # obtain the failed amount of the partially-cancelled order
                        print("[orderBack_2]:", retMatch)
                        if (retMatch is None) or (retMatch["status"] != "ok"):
                            print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                                  "obtained error partially cancel order query back", retMatch, file=sys.stderr)
                            self.openOrderIDQueue.put(id)
                            time.sleep(0.15)
                            continue
                        else:
                            # loop over all the trades to obtain the total filledAmount and the filledPrice
                            retDataList = retMatch["data"]
                            filledAmount = 0.
                            filledCashAmount = 0.
                            for iData in retDataList:
                                iFilledAmount = float(iData["filled-amount"])
                                filledAmount += iFilledAmount
                                filledCashAmount += (iFilledAmount * float(iData["price"]))
                            if filledAmount < 0.0000001:
                                print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                                      "obtain 0 filledAmount for partially cancelled order", str(id), "will re-query later again")
                                self.openOrderIDQueue.put(id)
                                time.sleep(0.15)
                                continue
                            fillPrice = filledCashAmount / filledAmount

                # check is the order status changed
                self.openOrderIDListLock.read_acquire()
                if self.openOrderIDOrderStatusDict[id][0] == orderStatus and \
                        self.openOrderIDOrderStatusDict[id][1] == filledAmount:
                    self.openOrderIDListLock.read_release()
                    # if order is not finished, push back to the queue
                    if self.OrderStatus2SystemOrderStatusDict[orderStatus] < trader.defines.orderFilled:
                        self.openOrderIDQueue.put(id)
                        # print("push id", id, "back to query queue")
                    # cool down
                    time.sleep(0.15)
                    continue
                # update the info
                self.openOrderIDOrderStatusDict[id][0] = orderStatus
                self.openOrderIDOrderStatusDict[id][1] = filledAmount
                self.openOrderIDListLock.read_release()
                # process trade data
                orderback = trader.coinTradeStruct.coinOrder()
                if filledAmount > 0.000001 and fillPrice < 0.:
                    fillPrice = float(orderInfo["field-cash-amount"]) / filledAmount

                data = {"apiType": self.API_name, \
                        "symbol": self.HuobiTickerToOkexTickerDict[orderInfo["symbol"]], \
                        "orderID": id, \
                        "status": self.OrderStatus2SystemOrderStatusDict[orderStatus], \
                        "deal_amount": filledAmount, \
                        "orderUpdateTime": time.time(), \
                        "strategyTag": strategyTag, \
                        "fillPrice": fillPrice
                        }
                orderback.setSpotOrderFeedback(data=data, isOrderBackOnly=True)
                self.onOrderBack(orderback)

                # if order is not finished, push back to the queue
                if data["status"] < trader.defines.orderFilled:
                    self.openOrderIDQueue.put(id)
                    # print("push id", id, "back to query queue")

            else:
                self.orderSendLock.release()
                print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                      "failed to find orderID", id, "in orderID_strategyMap for adapter", self.getAPI_name(), file=sys.stderr)

            # cool down the query frequency
            time.sleep(0.15)
