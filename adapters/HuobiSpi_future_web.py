# encoding: UTF-8
import time
import json
import hashlib
import sys
from threading import Thread
import adapters.websocket
from .huobiRest.HuobiDMService import HuobiDM
import gzip

# add import path
sys.path.append("../")
import trader.traderInterface
import trader.coinTradeStruct
import trader.coinUtilities
import trader.positionManager
import trader.locker
import trader.defines
import adapters.HuobiStruct
import threading
import pandas as pd
import zlib
from copy import deepcopy

class HuobiSpi_future_web(trader.traderInterface.adapterTemplate):
    def __init__(self):
        super(HuobiSpi_future_web, self).__init__()
        self._api_key = ''  # 用户名
        self._secret_key = ''  # 密码
        self._host = ''  # 服务器地址
        self._ws = None  # websocket应用对象
        self._is_login = False
        self._restURL = ''
        self.thread = None
        self.orderInfoThread = None
        self.strategies = {}
        self.API_name = ""
        self.workingTickers = []
        self.orderID_strategyMap = {}
        self.dataFeedOn = True
        self.adapterOn = True
        self.tickerLastPriceVolume = {}
        self.trader = None
        self.query_time = []
        self.query_numberLimit = 5
        self.query_timeLimit = 1  # seconds
        self.orderSendLock = threading.Lock()
        self.timeLimitLock = threading.Lock()
        self.positionQueryLock = trader.locker.RWLock()
        self.contractFreezed = {}
        self.contractAvailable = {}
        self.coinBalance = {}
        self.openFeeRate = 0.002
        self.closeFeeRate = 0.0
        self.contractSize = 20
        self.path2PositionFolder = ""
        self.allSymbolPositionMap = {}
        self.tradeTickerPositionMap = {}
        self.cashBaseValue = {}
        self.secondaryAPI_symbol_orders = {}
        self.order_client_id_count = 0
        self.openOrderIDListLock = trader.locker.RWLock()

        # rest api
        self.restAPI = None
        # create map to convert system types to okex_web types
        self.huobiDirectionType = {trader.defines.openLong_limitPrice : "buy", \
                                   trader.defines.openShort_limitPrice : "sell", \
                                   trader.defines.coverLong_limitPrice : "sell", \
                                   trader.defines.coverShort_limitPrice : "buy", \
                                   trader.defines.openLong_marketPrice : "buy", \
                                   trader.defines.openShort_marketPrice : "sell", \
                                   trader.defines.coverLong_marketPrice : "sell", \
                                   trader.defines.coverShort_marketPrice : "buy"}

        self.huobiOffset = {trader.defines.openLong_limitPrice : "open", \
                         trader.defines.openShort_limitPrice : "open", \
                         trader.defines.coverLong_limitPrice : "close", \
                         trader.defines.coverShort_limitPrice : "close", \
                         trader.defines.openLong_marketPrice : "open", \
                         trader.defines.openShort_marketPrice : "open", \
                         trader.defines.coverLong_marketPrice : "close", \
                         trader.defines.coverShort_marketPrice : "close"}

        self.huobiPriceType = {trader.defines.openLong_limitPrice : "limit", \
                                   trader.defines.openShort_limitPrice : "limit", \
                                   trader.defines.coverLong_limitPrice : "limit", \
                                   trader.defines.coverShort_limitPrice : "limit", \
                                   trader.defines.openLong_marketPrice : "opponent", \
                                   trader.defines.openShort_marketPrice : "opponent", \
                                   trader.defines.coverLong_marketPrice : "opponent", \
                                   trader.defines.coverShort_marketPrice : "opponent"}

        self.OrderStatus2SystemOrderStatusDict = {1: trader.defines.orderPending, \
                                                  2: trader.defines.orderPending,
                                                  3: trader.defines.orderFilling, \
                                                  4: trader.defines.orderFilling, \
                                                  5: trader.defines.orderCancelled, \
                                                  6: trader.defines.orderFilled, \
                                                  7: trader.defines.orderCancelled, \
                                                  11: trader.defines.orderFilling}

        self.futureContractTypes = {"this_week" : "CW", "next_week" : "NW", "quarter" : "CQ"}
        self.contractsToContractType = {"CW": "this_week", "NW":"next_week", "CQ":"quarter"}
        self.returnSymbolMap = {
            "BTC_CW": "BTC", "BTC_NW": "BTC", "BTC_CQ": "BTC",
            "ETH_CW": "ETH", "ETH_NW": "ETH", "ETH_CQ": "ETH",
            "EOS_CW": "EOS", "EOS_NW": "EOS", "EOS_CQ": "EOS"
        }
        self.returnExpiryMap = {
            "BTC_CW": "this_week", "BTC_NW": "next_week", "BTC_CQ": "quarter",
            "ETH_CW": "this_week", "ETH_NW": "next_week", "ETH_CQ": "quarter",
            "EOS_CW": "this_week", "EOS_NW": "next_week", "EOS_CQ": "quarter"
        }
        self.SymbolOpenOrderIDlDict = {
            "ETH": [],
            "BTC": [],
            "EOS": [],
        }
    # -------------------------- 连接 -------------------------- #
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
                        print("[ERR]: obtained spot position with future API for adapter", self.getAPI_name(), file=sys.stderr)
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
        # set info for position managers
        for i, pos in self.tradeTickerPositionMap.items():
            pos.setBaseCashValue(self.cashBaseValue)

        """连接服务器"""
        adapters.websocket.enableTrace(False)
        self._ws = adapters.websocket.WebSocketApp(self._host,
                                          on_message=self.on_message,
                                          on_error=self.on_error,
                                          on_close=self.on_close,
                                          on_open=self.on_open)

        #self._ws.run_forever()
        self.thread = Thread(target=self._ws.run_forever)
        self.thread.start()
        # rest api
        self.restAPI = HuobiDM(self._restURL, self._api_key, self._secret_key)

        # start a thread to query order info
        self.orderInfoThread = Thread(target=self.restQueryInfos)
        self.orderInfoThread.start()

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
            while (currentTime - firstTime) < self.query_timeLimit + 0.1:
                time.sleep(0.05)
                currentTime = time.time()
        self.query_time.append(currentTime)
        self.timeLimitLock.release()

    def sendOrder(self, order, isSecondaryAPI = False):
        self.orderSendLock.acquire()
        # generate client id
        cleintID = int(time.time()) * 1000 + self.order_client_id_count
        self.order_client_id_count = (self.order_client_id_count + 1) % 1000
        # send order
        try:
            # check send frequency
            self.timeLimitCoolDown()
            ret = self.restAPI.send_contract_order(
                symbol= order.symbol,
                contract_type=order.expiry,
                contract_code="",
                client_order_id=cleintID,
                price=order.price,
                volume=int(order.amount + 0.001),
                direction=self.huobiDirectionType[order.orderType],
                offset=self.huobiOffset[order.orderType],
                lever_rate=order.leverage,
                order_price_type=self.huobiPriceType[order.orderType]
            )
        except Exception as e:
            # try to find the order by client ID, if find, can go on trade
            # query order by client id
            print("order exception")
            try:
                # check send frequency
                self.timeLimitCoolDown()
                ret = self.restAPI.get_contract_order_info(symbol=order.symbol, client_order_id=str(cleintID))
            except Exception as ee:
                print("[ERR]:", trader.coinUtilities.getCurrentTimeString(),
                      "rest send order handshake exception and query order status by client id get exception as well", \
                      str(e), str(ee), self.getAPI_name(), file=sys.stderr)
                self.orderSendLock.release()
                return -1
            else:
                print("[DEBUG]: handshake requery ret,", ret)
                # process the return info
                if ret["status"] == "ok":
                    # send the order successfully, update the order id
                    orderID = ret["data"][0]["order_id"]
                    self.orderID_strategyMap[orderID] = order.getStrategyTag()
                    order.setOrderID(orderID)
                    # find position manager
                    iPositionManager = self.tradeTickerPositionMap[order.symbol + "-" + order.expiry + "-" + order.getStrategyTag()]
                    order.updateOrderStatus(trader.defines.orderFilling)
                    self.orderSendLock.release()
                    iPositionManager.addOpenOrder(order)
                    self.openOrderIDListLock.write_acquire()
                    self.SymbolOpenOrderIDlDict[order.symbol].append(orderID)
                    self.openOrderIDListLock.write_release()
                    return 0
            self.orderSendLock.release()
            # if failed to find the order in system, give warning
            print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                  self.getAPI_name(), "rest send order exception, order send failed ", str(e), file=sys.stderr)
            return -1
        else:
            # check status
            # print("ret order", ret)
            if (not ret is None) and (ret["status"] == "ok"):
                # update the order info
                orderID = ret["data"]["order_id"]
                self.orderID_strategyMap[orderID] = order.getStrategyTag()
                order.setOrderID(orderID)
                # find position manager
                iPositionManager = self.tradeTickerPositionMap[order.symbol + "-" + order.expiry + "-" + order.getStrategyTag()]
                order.updateOrderStatus(trader.defines.orderFilling)
                iPositionManager.addOpenOrder(order)
                self.orderSendLock.release()
                self.openOrderIDListLock.write_acquire()
                self.SymbolOpenOrderIDlDict[order.symbol].append(orderID)
                self.openOrderIDListLock.write_release()
                # self.openOrderIDQueue.put(orderID)
                return 0
            else:
                self.orderSendLock.release()
                # send order failed
                print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                      " order send failed for strategy ", order.getStrategyTag(), self.getAPI_name(), file=sys.stderr)
                print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                      " errorInfo: ", ret, file=sys.stderr)
                return -1


    def sendOrderBySecondaryAPI(self, order):
        # not supported for the api right now
        print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
              "second API is not available for ", self.getAPI_name(), file=sys.stderr)

    def cancelOrder(self, order):
        # check order is in cancelling status, do not cancel pending order with ID -19860922
        if order.isCancel:
            return 0
        # check send frequency
        self.timeLimitCoolDown()
        # cancel order
        try:
            ret = self.restAPI.cancel_contract_order(symbol=order.symbol, order_id=str(order.orderID))
        except Exception as e:
            print("[WARN]:", trader.coinUtilities.getCurrentTimeString(), \
                  "restAPI exception, failed to cancel order", order.orderID, "for", self.getAPI_name(), str(e))
            return -1
        else:
            if ret["status"] == "ok" :
                order.isCancel = True
                return 0
            else:
                print("[WARN]:", trader.coinUtilities.getCurrentTimeString(), \
                      "restAPI ret status not ok, failed to cancel order", order.orderID, "for", self.getAPI_name(), ret)
                return -1
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
                  "failed to get position manager for", symbol, expiry, "in adapter", self.getAPI_name(), file=sys.stderr)
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

    def reconnect(self):
        if not self.adapterOn:
            return
        """重新连接"""
        # 首先关闭之前的连接
        self.close_connect()

        print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
              "10s后重新连接", file=sys.stderr)
        # self.log.error(u"10s后重新连接")
        # 延时
        time.sleep(10)
        # 再执行重连任务
        self._ws = adapters.websocket.WebSocketApp(self._host,
                                          on_message=self.on_message,
                                          on_error=self.on_error,
                                          on_close=self.on_close,
                                          on_open=self.on_open)
        # self._ws.run_forever()
        self.thread = Thread(target=self._ws.run_forever)
        self.thread.start()

    def close_connect(self):
        """关闭接口"""
        if self.thread and self.thread.isAlive():
            self._ws.close()

    # ------------------------------------------------------------

    # -------------------------- 行情订阅 -------------------------- #
    def queryPositions(self):
        try:
            # check send frequency
            self.timeLimitCoolDown()
            ret = self.restAPI.get_contract_position_info()
        except Exception as e:
            print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                  "query holding position info get exception", str(e), self.getAPI_name())
        else:
            # print("query position ret", ret)
            if ret["status"] == "ok":
                updatedSymbols = []
                # process the info
                for iData in ret["data"]:
                    if iData["direction"] == "buy":
                        tag = iData["symbol"] + "_" + iData["contract_type"] + "_long"
                        self.contractAvailable[tag] = iData["available"]
                        self.contractFreezed[tag] = iData["frozen"]
                        updatedSymbols.append(tag)
                    else:
                        tag = iData["symbol"] + "_" + iData["contract_type"] + "_short"
                        self.contractAvailable[tag] = iData["available"]
                        self.contractFreezed[tag] = iData["frozen"]
                        updatedSymbols.append(tag)
                # for not updated tags, values should be 0
                for key in self.contractAvailable.keys():
                    if key in updatedSymbols:
                        continue
                    else:
                        self.contractAvailable[key] = 0
                        self.contractFreezed[key] = 0
            else:
                print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                      "query holding position info ret not OK", self.getAPI_name())

        # next query userinfo for balance info
        self.query_future_userinfo()

    def query_future_userinfo(self):
        # check send frequency
        self.timeLimitCoolDown()
        try:
            ret = self.restAPI.get_contract_account_info()
        except Exception as e:
            print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                  "query account info get exception", str(e), self.getAPI_name())
        else:
            if ret["status"] == "ok":
                for iData in ret["data"]:
                    self.coinBalance[iData["symbol"]] = float(iData["margin_available"])
            else:
                print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                      "query account info ret not OK", self.getAPI_name())

    def getFreePosition(self, ticker, expiry,  diretion = "long"):
        self.positionQueryLock.read_acquire()
        try:
            value = self.contractAvailable[ticker + "_" + expiry + "_" + diretion]
        except:
            self.positionQueryLock.read_release()
            return None
        self.positionQueryLock.read_release()
        return value

    def getFreezedPosition(self, ticker, expiry,  diretion = "long"):
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

    def subscribe_future_ticker(self, symbol, expiry):
        """订阅期货普通报价"""
        symbolsub = symbol.split("_")[0]
        self._ws.send("""{"sub": "market."""+ symbolsub + "_" + self.futureContractTypes[expiry]  +""".detail", "id": "id7"}""")

    def subscribe_future_depth(self, symbol, expiry, depth):
        """订阅期货深度报价"""
        symbolsub = symbol.split("_")[0]
        self._ws.send("""{"sub": "market.""" + symbolsub + "_" + self.futureContractTypes[expiry] + """.depth.""" + depth + """", "id": "id8"}""")

    def subscribe_future_trade(self, symbol, expiry):
        """订阅期货成交记录"""
        symbolsub = symbol.split("_")[0]
        self._ws.send("""{"sub": "market.""" + symbolsub + "_" + self.futureContractTypes[expiry] + """.trade.detail", "id": "id9"}""")

    def subscribe_future_k_line(self, symbol, expiry, interval):
        """订阅期货K线"""
        symbolsub = symbol.split("_")[0]
        self._ws.send("""{"sub": "market.""" + symbolsub + "_" + self.futureContractTypes[expiry] + """.kline."""+ interval  +"""", "id": "id10"}""")

    # -------------------------- 回报处理 ----------------------------
    def on_open(self, ws):
        # 一旦接口断开，重新连接，如果长时间没有请求，又会自动断开，因此所有的请求应该写在open函数中
        """连接成功，接口打开"""
        print("---->>> Connect Successfully for adapter ", self.API_name, file=sys.stdout)
        time.sleep(1)

        # 查询合约账户
        self.queryPositions()
        time.sleep(2.0)

        # subscribe ticker data
        subscribeTickers = []
        for ticker in set(self.workingTickers):
            # separate string to get symbol and expiry info
            splitFields =  ticker.split("-")

            # do not repeatedly subscribe tickers, because workingTickers string contain strategyTag
            if (splitFields[0]+splitFields[1]) in subscribeTickers:
                continue

            self.tickerLastPriceVolume[splitFields[0]+splitFields[1]] = [None, None]

            print("subscribe future data for ", splitFields[0] + "-" + splitFields[1], file=sys.stdout)
            # 订阅合约行情
            self.subscribe_future_ticker(splitFields[0], splitFields[1])
            # 订阅深度行情
            self.subscribe_future_depth(splitFields[0], splitFields[1], "step0")
            # 订阅成交队列
            self.subscribe_future_trade(splitFields[0], splitFields[1])
            subscribeTickers.append(splitFields[0]+splitFields[1])

    def on_message(self, ws, evt):
        """信息推送"""
        json_data = self.decompress_data(evt)
        # check heart beat
        if json_data[:7] == '{"ping"':
            ts = json_data[8:21]
            pong = '{"pong":' + ts + '}'
            ws.send(pong)
            return
        json_data = json.loads(json_data)
        channel = json_data.get("ch")
        # print(channel)
        if not channel:
            return
        split_channel = channel.split(".")
        # 根据channel调用不同的函数来处理回报
        if len(split_channel) == 3 and "detail" == split_channel[-1]:
            self.on_return_ticker_marketData(json_data, split_channel[1])
            return

        if "step" == split_channel[3][:4]:
            # depth行情
            symbol = self.returnSymbolMap[split_channel[1]]
            expiry = self.returnExpiryMap[split_channel[1]]
            data = adapters.HuobiStruct.Huobi_depth_Data()
            data.initFutureDepthData(symbol, expiry, json_data["tick"], self.API_name)
            # set last price and vol
            if not self.tickerLastPriceVolume[symbol + expiry][0] is None:
                data.LastPrice = float(self.tickerLastPriceVolume[symbol + expiry][0])
                data.Volume = int(self.tickerLastPriceVolume[symbol + expiry][1])
                self.onDepthData(data)
            return

        if ("trade" == split_channel[2]) and ("detail" == split_channel[3]):
            # print(json_data)
            # trade 行情。成交队列
            symbol = self.returnSymbolMap[split_channel[1]]
            expiry = self.returnExpiryMap[split_channel[1]]
            # print(json_data)
            for idata in json_data["tick"]["data"]:
                dataDict = {"symbol": symbol, \
                            "API": self.API_name, \
                            "expiry": expiry, \
                            "tradeId": idata["id"], \
                            "price": float(idata["price"]), \
                            "amount": int(idata["amount"]), \
                            "timestamp": json_data["tick"]["ts"], \
                            "tradeType": trader.defines.tradeBid if idata["direction"] == "buy" else trader.defines.tradeAsk}
                data = trader.coinTradeStruct.marketOrderInfo()
                data.initFutureMarketOrderInfo(dataDict)
                self.onMarketOrderBack(data)
            return

        # other info not handled
        print("[FEEDBACK]: ", json_data)

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

    def on_return_ticker_marketData(self, data, tickerWhole):
        # obtain ticker and expiry
        ticker = self.returnSymbolMap[tickerWhole]
        expiry = self.returnExpiryMap[tickerWhole]
        # update last price and volume data
        self.tickerLastPriceVolume[ticker + expiry] = [data["tick"]["close"], data["tick"]["vol"]]

    def restQueryInfos(self):
        while self.adapterOn:
            positionUpdated = False
            for symbol, orderList in self.SymbolOpenOrderIDlDict.items():
                if len(orderList) < 1:
                    continue
                orderids = deepcopy(orderList)
                orderSize = len(orderids)
                # can only query 20 orders at one time
                orderSubLists = [orderids[i:i + 20] for i in range(0, orderSize, 20)]
                # more than 1 order ids need to be separated by comma
                for iQueryList in orderSubLists:
                    if len(iQueryList) < 1:
                        continue
                    # generate query string
                    queryString = ""
                    for id in iQueryList:
                        queryString = queryString + "," + str(id)
                    # check send frequency
                    self.timeLimitCoolDown()
                    ret = self.restAPI.get_contract_order_info(symbol = symbol, order_id=queryString[1:])
                    # print("order return", ret)
                    if not ret is None:
                        if ret["status"] != "ok":
                            print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                                  "query order status failed", self.getAPI_name(), file=sys.stderr)
                            continue
                        for idata in ret["data"]:
                            orderback = trader.coinTradeStruct.coinOrder()
                            orderid = idata["order_id"]
                            orderStatus = self.OrderStatus2SystemOrderStatusDict[idata["status"]]
                            strategyTag = self.orderID_strategyMap.get(orderid, None)
                            if strategyTag is None:
                                print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                                      "failed to find orderID", orderid, "in orderID_strategyMap for adapter", \
                                      self.getAPI_name(), \
                                      file=sys.stderr)
                                continue

                            data = {"apiType": self.API_name, \
                                    "symbol": symbol, \
                                    "expiry": idata["contract_type"], \
                                    "orderID": orderid, \
                                    "status": orderStatus, \
                                    "deal_amount": float(idata["trade_volume"]), \
                                    "orderUpdateTime": time.time(), \
                                    "strategyTag": strategyTag, \
                                    "fillPrice": idata["trade_avg_price"]
                                    }
                            orderback.setFutureOrderFeedback(data=data, isOrderBackOnly=True)
                            self.onOrderBack(orderback)

                            # remove complete orders
                            if orderStatus >= trader.defines.orderFilled:
                                #print("try to remove orderid")
                                self.openOrderIDListLock.write_acquire()
                                try:
                                    #print("remove id", orderid)
                                    self.SymbolOpenOrderIDlDict[symbol].remove(orderid)
                                except:
                                    print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                                          "try to remove finished orderid", orderid, " is not in SymbolOpenOrderIDlDict",\
                                          file=sys.stderr)
                                self.openOrderIDListLock.write_release()
                    else:
                        continue
            time.sleep(0.25)

    def decompress_data(self, evt):
        return gzip.decompress(evt).decode('utf-8')
