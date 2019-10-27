# encoding: UTF-8
import time
import sys
import json
import hashlib
from threading import Thread
sys.path.append("../")
import adapters.websocket
import trader.coinTradeStruct
import trader.traderInterface
import trader.coinUtilities
import trader.positionManager
import trader.defines
import adapters.rest.OkcoinSpotAPI
import adapters.OKexStruct
import trader.locker
import threading
import pandas as pd
import zlib

class okexAPI_web_spot(trader.traderInterface.adapterTemplate):
    def __init__(self):
        super(okexAPI_web_spot, self).__init__()
        self._api_key = ''  # 用户名
        self._secret_key = ''  # 密码
        self._host = ''  # 服务器地址
        self._ws = None  # websocket应用对象
        self._is_login = False
        self._restURL = ''
        self._ok_const = adapters.OKexStruct.OkexConstants()
        self.thread = None
        self.strategies = {}
        self.API_name = ""
        self.workingTickers = []
        self.orderID_strategyMap = {}
        self.send_order_time = []
        self.cancel_order_time = []
        self.dataFeedOn = True
        self.adapterOn = True
        self.tickerLastPriceVolume = {}
        self.trader = None
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
        # rest api
        self.restAPI = None
        # create map to convert system types to okex_web types
        self.okexType = {trader.defines.openLong_limitPrice: "buy", \
                         trader.defines.openShort_limitPrice: "buy", \
                         trader.defines.coverLong_limitPrice: "sell", \
                         trader.defines.coverShort_limitPrice: "sell", \
                         trader.defines.openLong_marketPrice: "buy_market", \
                         trader.defines.openShort_marketPrice: "buy_market", \
                         trader.defines.coverLong_marketPrice: "sell_market", \
                         trader.defines.coverShort_marketPrice: "sell_market"}
        self.okexStatusMap = {-1 : trader.defines.orderCancelled, \
                              0: trader.defines.orderPending, \
                              1: trader.defines.orderFilling, \
                              2: trader.defines.orderFilled, \
                              4: trader.defines.orderFilling}

        self.okexStatusQueryMap = {-1 : trader.defines.orderCancelled, \
                                   0: trader.defines.orderPending, \
                                   1: trader.defines.orderFilling, \
                                   2: trader.defines.orderFilled, \
                                   3: trader.defines.orderFilling, \
                                   4: trader.defines.orderRejected, \
                                   5: trader.defines.orderFilling}

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

        # self._ws.run_forever()
        self.thread = Thread(target=self._ws.run_forever)
        self.thread.start()
        # rest api
        self.restAPI = adapters.rest.OkcoinSpotAPI.OKCoinSpot(self._restURL, self._api_key, self._secret_key)

        # query positions
        time.sleep(1)
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
        self.adapterOn = False
        # before close connect, save positions
        self.saveTickerPositions()
        print("saved ticker positions info for adapter", self.getAPI_name())
        self.close_connect()

    def sendOrder(self, order, isSecondaryAPI = False):
        if isSecondaryAPI:
            self.sendOrderBySecondaryAPI(order)
            return 0
        """
        币币委托
        访问频率 20次/2秒
        :symbol:       数字货币( btc_usdt )
        :price:        价格
        :amount:       委托数量
        :type_:        交易类型( 限价单:buy|sell 市价单:buy_market|sell_market)
        """
        # here we need to check send frequency first
        # if the firequency is larger than threashold value, need to pause
        currentTime = time.time()
        ret = None
        if len(self.send_order_time) >= 20:
            firstTime = self.send_order_time.pop(0)
            while (currentTime - firstTime) < 2.1:
                time.sleep(0.05)
                currentTime = time.time()

        self.send_order_time.append(currentTime)
        try:
            self.orderSendLock.acquire()
            ret = self.restAPI.trade(str(order.symbol), \
                                 str(self.okexType[order.orderType]), \
                                 str(order.price), \
                                 str(order.amount))

            # get orderID
            data = json.loads(ret)

            if data["result"]:
                self.orderID_strategyMap[int(data["order_id"])] = order.getStrategyTag()
                order.setOrderID(int(data["order_id"]))
                # find position manager
                iPositionManager = self.tradeTickerPositionMap[order.symbol + "-" + order.expiry + "-" + order.getStrategyTag()]
                iPositionManager.addOpenOrder(order)
                self.orderSendLock.release()
                return 0
            else:
                print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                      " order send failed for strategy ", order.getStrategyTag(), file=sys.stderr)
                print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                      " errorID: ", data["error_code"], file=sys.stderr)
                self.orderSendLock.release()
                return -1
        except:
            self.orderSendLock.release()
            print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                  " The handshake operation timed out for rest sends order for API: ", self.API_name,\
                  "err-code", ret, file=sys.stderr)
            # strategy need to handle this problem
            self.strategies[order.strategyTag].onSendOrderFailed(order)
            return -1

    def sendOrderBySecondaryAPI(self, order):
        # this is the backup API, we use web API to send the order out
        """
        币币委托
        访问频率 20次/2秒
        :symbol:       数字货币( btc_usdt )
        :price:        价格
        :amount:       委托数量
        :type_:        交易类型( 限价单:buy|sell 市价单:buy_market|sell_market)
        """
        # confine the send order frequency
        currentTime = time.time()
        if len(self.send_order_time) >= 20:
            firstTime = self.send_order_time.pop(0)
            while (currentTime - firstTime) < 2.1:
                time.sleep(0.05)
                currentTime = time.time()
        self.send_order_time.append(currentTime)


        params = {'symbol': str(order.symbol),
                  'price': str(order.price),
                  'amount': str(order.amount),
                  'type': str(self.okexType[order.orderType])
                  }

        channel = 'ok_spot_order'

        # check this symbol is in map or not
        if order.symbol in self.secondaryAPI_symbol_orders:
            # have order for this symbol sending in the secondaryAPI, do not send the order out
            print("[WARN]:", trader.coinUtilities.getCurrentTimeString(), \
                  "having order sending in web api for", self.getAPI_name(), "for", order.symbol, order.strategyTag)
            return
        # save the order into map
        self.secondaryAPI_symbol_orders[order.symbol] = order.getStrategyTag()
        order.setOrderID(-19860922)
        # add into position manager
        iPositionManager = self.tradeTickerPositionMap[order.symbol + "-" + order.expiry + "-" + order.getStrategyTag()]
        iPositionManager.addOpenOrder(order)
        # send the order out
        # print("send web order")
        # print(params)
        self.send_trading_request(channel, params)

    def cancelOrder(self, order):
        """
        币币交易撤单
        访问频率 20次/2秒
        :symbol:       数字货币( btc_usdt )
        :orderid:      报单id
        """
        if order.isCancel:
            # here we need to check send frequency first
            # if the firequency is larger than threashold value, need to pause
            currentTime = time.time()
            if len(self.cancel_order_time) >= 20:
                firstTime = self.cancel_order_time.pop(0)
                while (currentTime - firstTime) < 2.1:
                    time.sleep(0.05)
                    currentTime = time.time()
            self.cancel_order_time.append(currentTime)

            # query current order status, if is not cancelled, resend the cancel order out
            try:
                queryRet = json.loads(self.restAPI.orderinfo(str(order.symbol), str(order.orderID)))
                print("queryRet", queryRet)
                # check if the order status is cancelled or finished
                if queryRet["result"]:
                    queryBackOrders = queryRet["orders"]
                    for iOrderBack in queryBackOrders:
                        if self.okexStatusQueryMap[iOrderBack["status"]] >= trader.defines.orderFilled:
                            return
            except Exception as e:
                print("[ERR]:", trader.coinUtilities.getCurrentTimeString(),\
                      "failed to query order status for", str(order.symbol), str(order.orderID), str(e), file=sys.stderr)

        # check order is in cancelling status, do not cancel pending order with ID -19860922
        if order.orderID == -19860922:
            return

        # get okex str
        params = {'symbol': str(order.symbol),
                  'order_id': str(order.orderID)}

        channel = 'ok_spot_cancel_order'

        # here we need to check send frequency first
        # if the firequency is larger than threashold value, need to pause
        currentTime = time.time()
        if len(self.cancel_order_time) >= 20:
            firstTime = self.cancel_order_time.pop(0)
            while (currentTime - firstTime) < 2.1:
                time.sleep(0.05)
                currentTime = time.time()
        self.cancel_order_time.append(currentTime)

        try:
            self.send_trading_request(channel, params)
            order.isCancel = True
        except Exception as e:
            print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                  "exception when cancel order for", self.getAPI_name(), str(e))
        return 0

    def onOrderBack(self, orderBack):
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
                if iWorkingTickerIsSpot[i] and iAPIs[i] == self.getAPI_name():
                    tickerTag = iTicker + "-" + iWorkingTickerExpires[i] + "-" + strategy.getStrategyTag()
                    self.workingTickers.append(tickerTag)
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

    # -------------------------- 连接 --------------------------

    def connect(self):
        """连接服务器"""
        adapters.websocket.enableTrace(False)
        self._ws = adapters.websocket.WebSocketApp(self._host,
                                                   on_message=self.on_message,
                                                   on_error=self.on_error,
                                                   on_close=self.on_close,
                                                   on_open=self.on_open)

        # self._ws.run_forever()
        self.thread = Thread(target=self._ws.run_forever)
        self.thread.start()
        # rest api
        self.restAPI = adapters.rest.OkcoinSpotAPI.OKCoinSpot(self._restURL, self._api_key, self._secret_key)

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
        adapters.websocket.enableTrace(False)
        self._ws = adapters.websocket.WebSocketApp(self._host,
                                                   on_message=self.on_message,
                                                   on_error=self.on_error,
                                                   on_close=self.on_close,
                                                   on_open=self.on_open)

        # self._ws.run_forever()
        self.thread = Thread(target=self._ws.run_forever)
        self.thread.start()
        # rest api
        self.restAPI = adapters.rest.OkcoinSpotAPI.OKCoinSpot(self._restURL, self._api_key, self._secret_key)

    def close_connect(self):
        """关闭接口"""
        if self.thread and self.thread.isAlive():
            self._ws.close()

    def ccoin_login(self):
        """用户登录"""
        params = {'api_key': self._api_key}
        params['sign'] = self.generate_sign(params)
        # 生成请求
        dump_dict = {'event': 'login',
                     'channel': 'login',
                     'parameters': params}

        # 使用json打包并发送
        j = json.dumps(dump_dict)

        # 若触发异常则重连
        try:
            self._ws.send(j)
        except adapters.websocket.WebSocketConnectionClosedException:
            pass

    def generate_sign(self, params):
        """生成签名"""
        tmp_list = []
        for key in sorted(params.keys()):
            tmp_list.append('%s=%s' % (key, params[key]))
        tmp_list.append('secret_key=%s' % self._secret_key)
        sign = '&'.join(tmp_list)
        return hashlib.md5(sign.encode('utf-8')).hexdigest().upper()
    # ------------------------------------------------------------

    # -------------------------- 行情订阅 -------------------------- #
    def subscribe_ccoin_ticker(self, symbol):
        """订阅币币普通报价"""
        self.sub_market_data_by_channel('ok_sub_spot_%s_ticker' % (symbol))

    def subscribe_ccoin_depth(self, symbol, depth):
        """订阅币币深度报价"""
        self.sub_market_data_by_channel('ok_sub_spot_%s_depth_%s' % (symbol, depth))

    def subscribe_ccoin_trade(self, symbol):
        """订阅币币成交记录"""
        self.sub_market_data_by_channel('ok_sub_spot_%s_deals' % (symbol))

    def subscribe_ccoin_k_line(self, symbol, interval):
        """订阅币币K线"""
        self.sub_market_data_by_channel('ok_sub_spot_%s_kline_%s' % (symbol, interval))

    def sub_market_data_by_channel(self, channel):
        """发送行情请求"""
        # 生成请求
        dump_dict = {'event': 'addChannel',
                     'channel': channel}

        # 使用json打包并发送
        j = json.dumps(dump_dict)

        # 若触发异常则重连
        try:
            self._ws.send(j)
        except adapters.websocket.WebSocketConnectionClosedException:
            pass

    # process feedback of position check
    def processPositionQuery(self, data):

        pass
    # ---------------------------------------------------------------
    def on_return_my_trade(self, json_data):
        self.orderSendLock.acquire()
        self.orderSendLock.release()
        # find strategyTag from ID
        ID = int(json_data["orderId"])
        if ID in self.orderID_strategyMap:
            # process trade data
            # print(json_data)
            orderback = trader.coinTradeStruct.coinOrder()
            data = {"apiType": self.API_name, \
                    "symbol": json_data["symbol"], \
                    "orderID": ID, \
                    "status": self.okexStatusMap[json_data["status"]], \
                    "deal_amount": float(json_data["completedTradeAmount"]), \
                    "orderUpdateTime": time.time(), \
                    "strategyTag": self.orderID_strategyMap[ID], \
                    "fillPrice": float(json_data["averagePrice"])
                    }
            orderback.setSpotOrderFeedback(data=data, isOrderBackOnly=True)
            self.onOrderBack(orderback)
        else:
            # use local internal ID
            # find the symbol in secondary API map
            symbol = json_data["symbol"]
            if symbol in self.secondaryAPI_symbol_orders:
                # add this info into orderID_strategyMap
                strategyTag = self.secondaryAPI_symbol_orders[symbol]
                self.orderID_strategyMap[ID] = strategyTag
                # give this order back to strategy
                orderback = trader.coinTradeStruct.coinOrder()
                data = {"apiType": self.API_name, \
                        "symbol": json_data["symbol"], \
                        "orderID": ID, \
                        "status": self.okexStatusMap[json_data["status"]], \
                        "deal_amount": float(json_data["completedTradeAmount"]), \
                        "orderUpdateTime": time.time(), \
                        "strategyTag": strategyTag, \
                        "fillPrice": float(json_data["averagePrice"])
                        }
                orderback.setSpotOrderFeedback(data=data, isOrderBackOnly=True)
                # send it back to strategy
                self.onOrderBack(orderback)
                # need to delete the symbol out
                del self.secondaryAPI_symbol_orders[symbol]
            else:
                # error warning
                print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                      "failed to find ID ", ID, " in orderID strategy map", \
                      "and cannot find symbol", symbol, "in secondaryAPI_symbol_orders map", file=sys.stderr)
    # -------------------------- 查询、报撤单 -------------------------- #
    def queryPositions(self):
        try:
            info = json.loads(self.restAPI.userinfo())
            try:
                freeData = info["info"]["funds"]["free"]
                self.positionQueryLock.write_acquire()
                for iCoin, iValue in freeData.items():
                    self.eachTickerFreePosition[iCoin] = float(iValue)
                self.positionQueryLock.write_release()
            except:
                pass
            try:
                freezedData = info["info"]["funds"]["freezed"]
                self.positionQueryLock.write_acquire()
                for iCoin, iValue in freezedData.items():
                    self.eachTickerFreezedPosition[iCoin] = float(iValue)
                self.positionQueryLock.write_release()
            except:
                print("[ERR]: ", trader.coinUtilities.getCurrentTimeString(), \
                      "failed to get user info for api ", self.getAPI_name(), file=sys.stderr)
        except:
            print("[ERR]: ", trader.coinUtilities.getCurrentTimeString(), \
                  "failed to get user info for api ", self.getAPI_name(), file=sys.stderr)

    def getFreePosition(self, ticker, expiry, diretion = "long"):
        self.positionQueryLock.read_acquire()
        try:
            value = self.eachTickerFreePosition[ticker]
            self.positionQueryLock.read_release()
            return value
        except:
            self.positionQueryLock.read_release()
            return None
            pass


    def getFreezedPosition(self, ticker, expiry, diretion = "long"):
        self.positionQueryLock.read_acquire()
        try:
            value = self.eachTickerFreezedPosition[ticker]
            self.positionQueryLock.read_release()
            return value
        except:
            self.positionQueryLock.read_release()
            return None

    def getCoinBalance(self, coin):
        self.positionQueryLock.read_acquire()
        try:
            freeValue = self.eachTickerFreePosition[coin]
            freedvalue = self.eachTickerFreezedPosition[coin]
            self.positionQueryLock.read_release()
            return freeValue + freedvalue
        except:
            self.positionQueryLock.read_release()
            return None

    def request_ccoin_user_info(self):
        """订阅币币账户信息"""
        channel = 'ok_spot_userinfo'
        self.send_trading_request(channel, {})

    def send_trading_request(self, channel, params):
        """发送交易请求"""
        # 在参数字典中加上api_key和签名字段
        params['api_key'] = self._api_key
        params['sign'] = self.generate_sign(params)

        # 生成请求
        dump_dict = {'event': 'addChannel',
                     'channel': channel,
                     'parameters': params}

        # 使用json打包并发送
        j = json.dumps(dump_dict)

        # 若触发异常则重连
        try:
            self._ws.send(j)
        except adapters.websocket.WebSocketConnectionClosedException:
            print("[ERR]:", trader.coinUtilities.getCurrentTimeString(),\
                  "exception caught for send_trading_request")

    # -------------------------- 回报处理 -------------------------- #
    def on_message(self, ws, evt):
        """信息推送"""
        cur_time = time.time()  # 本地时间戳
        json_data = self.decompress_data(evt)
        # print(json_data)
        # 以 _ 分割, 提取depth、trade、登录后账户的合约信息
        channel = json_data.get("channel")

        # 根据channel调用不同的函数来处理回报
        if not channel:
            # channel为none时直接返回
            return
        split_channel = channel.split("_")

        if channel == "ok_spot_userinfo":
            # 账户信息回报，只推一次
            # print(json_data)
            # get current position for each coin
            coins = json_data["data"]["info"]["funds"]["free"]
            self.positionQueryLock.write_acquire()
            for iCoin, iValue in coins.items():
                self.eachTickerFreePosition[iCoin] = float(iValue)
            self.positionQueryLock.write_release()
        if not self._is_login:
            if channel == "login" and json_data["data"]["result"]:
                # 登录信息
                print("---->>>> Login Success for adapter ", self.API_name, file=sys.stdout)
                self._is_login = 1
        else:  # 登录成功后才能开始处理行情

            if "balance" in split_channel:
                # 合约账户信息。相当于query的回报
                pass
                return

            if channel == "ok_futureusd_trade":
                # 发单回报
                if not json_data["data"]["result"]:
                    print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                          "!!!!!order error. id: %d!!!!!" % json_data["data"], file=sys.stderr)

            if ("ok_sub_spot" in channel) and ("order" in split_channel):
                # 成交回报。相当于OnRtnOrder + OnRtnTrade
                self.on_return_my_trade(json_data["data"])
                return

            if ("ok_sub_spot" in channel) and ("ticker" in split_channel):
                # get last price and volume
                symbol = split_channel[3] + "_" + split_channel[4]
                self.tickerLastPriceVolume[symbol] = [json_data["data"]["last"], json_data["data"]["vol"]]
                return
            if "depth" in split_channel and "20" in split_channel:
                # depth 20行情
                symbol = "_".join(split_channel[3:5])
                data = adapters.OKexStruct.OK_depth_Data()
                data.initSpotDepthData(symbol, json_data["data"], self.API_name)
                # get last price and volume
                data.LastPrice = float(self.tickerLastPriceVolume[symbol][0])
                data.Volume = float(self.tickerLastPriceVolume[symbol][1])
                self.onDepthData(data)
                return

            if "deals" in split_channel and "sub" in split_channel:
                # trade 行情。成交队列
                symbol = "_".join(split_channel[3:5])
                for idata in json_data["data"]:
                    dataDict = {"symbol": symbol, \
                                "API": self.API_name, \
                                "tradeId": idata[0], \
                                "price": float(idata[1]), \
                                "amount": float(idata[2]), \
                                "timestamp": idata[3], \
                                "tradeType": trader.defines.tradeAsk if idata[4] == "ask" else trader.defines.tradeBid}
                    data = trader.coinTradeStruct.marketOrderInfo()
                    data.initSpotMarketOrderInfo(dataDict)
                    self.onMarketOrderBack(data)
                return


    def on_open(self, ws):
        # 一旦接口断开，重新连接，如果长时间没有请求，又会自动断开，因此所有的请求应该写在open函数中
        """连接成功，接口打开"""
        print("---->>> Connect Successfully for ", self.API_name, file=sys.stdout)
        time.sleep(1)

        # 账户登录
        self.ccoin_login()

        # 查询合约账户
        self.request_ccoin_user_info()

        # subscribe data
        subscribeTickers = []
        for ticker in self.workingTickers:
            if not "-future" in ticker:
                # separate string to get symbol and expiry info
                splitFields = ticker.split("-")

                # do not repeatedly subscribe tickers, because workingTickers string contain strategyTag
                if (splitFields[0]) in subscribeTickers:
                    continue

                self.tickerLastPriceVolume[splitFields[0]] = [None, None]

                # 订阅行情数据
                self.subscribe_ccoin_ticker(splitFields[0])

                # 订阅深度行情
                print("subscribe data for", splitFields[0], file=sys.stdout)
                self.subscribe_ccoin_depth(splitFields[0], 20)

                # 订阅成交队列
                self.subscribe_ccoin_trade(splitFields[0])

                subscribeTickers.append(splitFields[0])

    def on_close(self, ws):
        """接口断开"""
        print('---->>> onClose for adapter ', self.API_name, file=sys.stdout)

        # try to close ws
        try:
            if self.thread and self.thread.isAlive():
                self._ws.close()
        except:
            pass
        # self.log.error(u"服务器连接断开")

        # 重新连接
        if self.adapterOn:
            print("[ERR]: ",trader.coinUtilities.getCurrentTimeString(), \
                  '---->>> onClose for adapter ', self.API_name, file=sys.stderr)
            self.reconnect()

    def on_error(self, ws, evt):
        """错误推送"""
        print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), 'onError', file=sys.stderr)
        print(evt, file=sys.stderr)

        # 记录错误日志
        # self.log.error(evt)

    def decompress_data(self, evt):
        """解压缩推送收到的数据"""
        inflated = self.inflate(evt)
        data = json.loads(inflated)
        return data[0]
    # ----------------------------------------------------------
    def decompress_ret(self, evt):
        inflated = self.inflate(evt)
        return json.loads(inflated)

    def inflate(self, data):
        decompress = zlib.decompressobj(
            -zlib.MAX_WBITS  # see above
        )
        inflated = decompress.decompress(data)
        inflated += decompress.flush()
        return inflated
