# encoding: UTF-8
import time
import json
import hashlib
import sys
from threading import Thread

# add import path
sys.path.append("../")
import adapters.websocket
import trader.traderInterface
import trader.coinTradeStruct
import trader.coinUtilities
import trader.positionManager
import trader.locker
import trader.defines
import adapters.rest.OkcoinFutureAPI
import adapters.OKexStruct
import threading
import pandas as pd
import zlib

class okexAPI_web_future(trader.traderInterface.adapterTemplate):
    def __init__(self):
        super(okexAPI_web_future, self).__init__()
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
        self.webInterfaceOrderTag_OrderMap = {}
        self.cancel_order_time = []
        self.symbolSendOrderTimeListMap = {}
        self.dataFeedOn = True
        self.adapterOn = True
        self.tickerLastPriceVolume = {}
        self.trader = None
        self.orderSendLock = threading.Lock()
        self.positionQueryLock = trader.locker.RWLock()
        self.contractFreezed = {}
        self.contractAvailable = {}
        self.coinBalance = {}
        self.openFeeRate = 0.002
        self.closeFeeRate = 0.0
        self.contractSize = 10
        self.path2PositionFolder = ""
        self.allSymbolPositionMap = {}
        self.tradeTickerPositionMap = {}
        self.cashBaseValue = {}
        self.secondaryAPI_symbol_orders = {}
        self.queryPositionAvailable = True

        # rest api
        self.restAPI = None
        # create map to convert system types to okex_web types
        self.okexType = {trader.defines.openLong_limitPrice : 1, \
                         trader.defines.openShort_limitPrice : 2, \
                         trader.defines.coverLong_limitPrice : 3, \
                         trader.defines.coverShort_limitPrice : 4, \
                         trader.defines.openLong_marketPrice : 1, \
                         trader.defines.openShort_marketPrice : 2, \
                         trader.defines.coverLong_marketPrice : 3, \
                         trader.defines.coverShort_marketPrice : 4}
        self.okex_matchPrice = {trader.defines.openLong_limitPrice : 0, \
                         trader.defines.openShort_limitPrice : 0, \
                         trader.defines.coverLong_limitPrice : 0, \
                         trader.defines.coverShort_limitPrice : 0, \
                         trader.defines.openLong_marketPrice : 1, \
                         trader.defines.openShort_marketPrice : 1, \
                         trader.defines.coverLong_marketPrice : 1, \
                         trader.defines.coverShort_marketPrice : 1}
        self.okexStatusMap = {-1: trader.defines.orderCancelled, \
                              0: trader.defines.orderPending, \
                              1: trader.defines.orderFilling, \
                              2: trader.defines.orderFilled, \
                              4: trader.defines.orderFilling}

        self.futureSymbols = ["btc_usd", "ltc_usd", "eth_usd", "etc_usd", "bch_usd"]
        self.futureCoins = [(iSymbol.split("_"))[0] for iSymbol in self.futureSymbols]
        self.futureSymbolsToCoinDict = dict(zip(self.futureSymbols, self.futureCoins))
        self.CoinToFutureSymbolsDict = dict(zip(self.futureCoins, self.futureSymbols))
        self.futureContractTypes = ["this_week", "next_week", "quarter"]

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
        self.restAPI = adapters.rest.OkcoinFutureAPI.OKCoinFuture(self._restURL, self._api_key, self._secret_key)
        # query init positions
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
        print("saved ticker positions info for adapter", self.getAPI_name(), file=sys.stdout)
        self.close_connect()

    def sendOrder(self, order, isSecondaryAPI = False):
        if isSecondaryAPI:
            self.sendOrderBySecondaryAPI(order)
            return 0
        # 注意:此处需要后期验证以对手价成交时的细节处理
        """
        期货委托
        访问频率: 5次/秒
        :symbol:       数字货币( btc ltc eth etc bch )
        :expiry:       交割时间( this_week:当周 next_week:下周 quarter:季度)
        :price:        价格
        :amount:       委托数量
        :orderType:        交易类型( 1:开多 2:开空 3:平多 4:平空 )
        :order_type:   是否为对手价( 0:不是 1:是 当取值为1时,price无效 )
        :leverage:     杠杆倍数(10\20 默认10)
        """
        # here we need to check send frequency first
        # if the firequency is larger than threashold value, need to pause
        # get ticker send time list
        send_order_time = self.symbolSendOrderTimeListMap[order.symbol]
        currentTime = time.time()
        if len(send_order_time) >= 5:
            firstTime = send_order_time.pop(0)
            while currentTime - firstTime < 1.1:
                time.sleep(0.05)
                currentTime = time.time()

        send_order_time.append(currentTime)

        try:
            self.orderSendLock.acquire()
            # rest trade
            #future_trade(self, symbol, contractType, price='', amount='', tradeType='', matchPrice='', leverRate='')
            ret = self.restAPI.future_trade(symbol= str(order.symbol + "_usd"), \
                                            contractType = str(order.expiry), \
                                            price = str(order.price), \
                                            amount = str(order.amount), \
                                            tradeType = str(self.okexType[order.orderType]), \
                                            matchPrice = str(self.okex_matchPrice[order.orderType]), \
                                            leverRate = str(order.leverage))
            # get orderID
            data = json.loads(ret)
            if data["result"]:
                self.orderID_strategyMap[int(data["order_id"])] = order.getStrategyTag()
                order.setOrderID(int(data["order_id"]))
                # find position manager
                iPositionManager = self.tradeTickerPositionMap[order.symbol + "-" + order.expiry + "-" + order.getStrategyTag()]
                iPositionManager.addOpenOrder(order)
            else:
                print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                      " order send failed for strategy ", order.getStrategyTag(), file=sys.stderr)
                print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                      " errorID: ", data["error_code"], file=sys.stderr)
            self.orderSendLock.release()
        except:
            self.orderSendLock.release()
            print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                  " The handshake operation timed out for rest sends order for API: ", self.API_name, \
                  file=sys.stderr)
            # strategy need to handle this problem
            self.strategies[order.strategyTag].onSendOrderFailed(order)

    def sendOrderBySecondaryAPI(self, order):
        """
        期货委托
        访问频率: 5次/秒
        :symbol:       数字货币( btc ltc eth etc bch )
        :expiry:       交割时间( this_week:当周 next_week:下周 quarter:季度)
        :price:        价格
        :amount:       委托数量
        :type_:        交易类型( 1:开多 2:开空 3:平多 4:平空 )
        :order_type:   是否为对手价( 0:不是 1:是 当取值为1时,price无效 )
        :leverage:     杠杆倍数(10\20 默认10)
        """

        # check this symbol is in map or not
        if order.symbol in self.secondaryAPI_symbol_orders:
            # have order for this symbol sending in the secondaryAPI, do not send the order out
            print("[WARN]:", trader.coinUtilities.getCurrentTimeString(), \
                  "having order sending in web api for", self.getAPI_name(), "for", order.symbol, order.strategyTag)
            return

        # need to check order frequency here
        # here we need to check send frequency first
        # if the firequency is larger than threashold value, need to pause
        # get ticker send time list
        send_order_time = self.symbolSendOrderTimeListMap[order.symbol]
        currentTime = time.time()
        if len(send_order_time) >= 5:
            firstTime = send_order_time.pop(0)
            while currentTime - firstTime < 1.1:
                time.sleep(0.05)
                currentTime = time.time()
        send_order_time.append(currentTime)

        params = {'symbol': str(order.symbol + "_usd"),
                  'contract_type': str(order.expiry),
                  'price': str(order.price),
                  'amount': str(order.amount),
                  'type': str(self.okexType[order.orderType]),
                  'match_price': str(self.okex_matchPrice[order.orderType]),
                  'lever_rate': str(order.leverage)}

        channel = 'ok_futureusd_trade'


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
        合约交易撤单
        访问频率 10次/秒
        :symbol:       数字货币( btc_usd ltc_usd eth_usd etc_usd bch_usd )
        :expiry:       交割时间( this_week:当周 next_week:下周 quarter:季度)
        :orderid:      报单id
        """
        # check order is in cancelling status, do not cancel pending order with ID -19860922
        if order.isCancel or (order.orderID == -19860922):
            return
        # here we need to check send frequency first
        # if the firequency is larger than threashold value, need to pause
        # 5次/s for each symbol
        currentTime = time.time()
        if len(self.cancel_order_time) >= 10:
            firstTime = self.cancel_order_time.pop(0)
            while currentTime - firstTime < 1.1:
                time.sleep(0.05)
                currentTime = time.time()

        self.cancel_order_time.append(currentTime)

        # get okex str
        params = {'symbol': str(order.symbol + "_usd"),
                  'contract_type': str(order.expiry),
                  'order_id': str(order.orderID)}

        channel = 'ok_futureusd_cancel_order'
        order.isCancel = True
        self.send_trading_request(channel, params)

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
        #self._ws.close()
        if self.thread and self.thread.isAlive():
            self._ws.close()

    def future_login(self):
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
    def queryPositions(self):
        for iSymbol in self.futureSymbols:
            for iContractType in self.futureContractTypes:
                info = {}
                try:
                    # info = self.decompress_ret(self.restAPI.future_userinfo_4fix())
                    info = json.loads(self.restAPI.future_position(iSymbol, iContractType))
                except:
                    print("[ERR]: ", trader.coinUtilities.getCurrentTimeString(), \
                          "failed to get user position info for", iSymbol, self.getAPI_name(), file=sys.stderr)
                else:
                    # process the info
                    self.positionQueryLock.write_acquire()
                    try:
                        if (not "holding" in info) or (len(info["holding"]) == 0):
                            longTag = self.futureSymbolsToCoinDict[iSymbol] + "_" + iContractType + "_long"
                            shortTag = self.futureSymbolsToCoinDict[iSymbol] + "_" + iContractType + "_short"
                            self.contractAvailable[longTag] = 0
                            self.contractFreezed[longTag] = 0
                            self.contractAvailable[shortTag] = 0
                            self.contractFreezed[shortTag] = 0
                            self.positionQueryLock.write_release()
                            time.sleep(0.3)
                            continue

                        allInfo = info["holding"]
                        for iHolding in allInfo:
                            longTag = self.futureSymbolsToCoinDict[iHolding["symbol"]] + "_" + iHolding["contract_type"] + "_long"
                            shortTag = self.futureSymbolsToCoinDict[iHolding["symbol"]] + "_" + iHolding["contract_type"] + "_short"
                            self.contractAvailable[longTag] = float(iHolding["buy_available"])
                            self.contractFreezed[longTag] = float(iHolding["buy_amount"]) - float(iHolding["buy_available"])
                            self.contractAvailable[shortTag] = float(iHolding["sell_available"])
                            self.contractFreezed[shortTag] = float(iHolding["sell_amount"]) - float(iHolding["sell_available"])
                    except:
                        print("[ERR]: ", trader.coinUtilities.getCurrentTimeString(), \
                              "failed to get user info for", iSymbol, self.getAPI_name(), file=sys.stderr)
                    self.positionQueryLock.write_release()
                # sleep to cool down query frequency
                time.sleep(0.3)

        # next query userinfo for balance info
        self.query_future_userinfo()

    def query_future_userinfo(self):
        info = {}
        try:
            info = json.loads(self.restAPI.future_userinfo())
        except:
            print("[ERR]: ", trader.coinUtilities.getCurrentTimeString(), \
                  "failed to get user account info for api ", self.getAPI_name(), file=sys.stderr)
            print(info)
        else:
            # process the data
            self.positionQueryLock.write_acquire()
            try:
                for iTicker, iInfo in info["info"].items():
                    self.coinBalance[iTicker] = float(iInfo["account_rights"])
            except:
                print("[ERR]: ", trader.coinUtilities.getCurrentTimeString(), \
                      "failed to get user account info for api ", self.getAPI_name(), file=sys.stderr)
            self.positionQueryLock.write_release()


    def getFreePosition(self, ticker, expiry,  diretion = "long"):
        value = 0.0
        self.positionQueryLock.read_acquire()
        try:
            value = self.contractAvailable[ticker + "_" + expiry + "_" + diretion]
        except:
            self.positionQueryLock.read_release()
            return None
        self.positionQueryLock.read_release()
        return value

    def getFreezedPosition(self, ticker, expiry,  diretion = "long"):
        value = 0.0
        self.positionQueryLock.read_acquire()
        try:
            value = self.contractFreezed[ticker + "_" + expiry + "_" + diretion]
        except:
            self.positionQueryLock.read_release()
            return None
        self.positionQueryLock.read_release()
        return value

    def getCoinBalance(self, coin):
        value = 0.0
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
        self.sub_market_data_by_channel('ok_sub_futureusd_%s_ticker_%s' % (symbol, expiry))

    def subscribe_future_depth(self, symbol, expiry, depth):
        """订阅期货深度报价"""
        self.sub_market_data_by_channel('ok_sub_futureusd_%s_depth_%s_%s' % (symbol, expiry, depth))

    def subscribe_future_trade(self, symbol, expiry):
        """订阅期货成交记录"""
        self.sub_market_data_by_channel('ok_sub_futureusd_%s_trade_%s' % (symbol, expiry))

    def subscribe_future_k_line(self, symbol, expiry, interval):
        """订阅期货K线"""
        self.sub_market_data_by_channel('ok_sub_futureusd_%s_kline_%s_%s' % (symbol, expiry, interval))

    def subscribe_future_index(self, symbol):
        """订阅期货指数"""
        self.sub_market_data_by_channel('ok_sub_futureusd_%s_index' % (symbol))

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

    # -------------------------- 查询、报撤单 -------------------------- #
    def request_future_user_info(self):
        """订阅期货账户信息"""
        channel = 'ok_futureusd_userinfo'
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
            ret = self._ws.send(j)
            print(channel, ret)
        except adapters.websocket.WebSocketConnectionClosedException:
            pass
    # ----------------------------------------------------------
    def on_return_user_info(self, json_data):
        # print(json_data)
        # get open positions infos
        if json_data["data"]["result"]:
            # read the info one by one
            info = json_data["data"]["info"]
            for coin, coinInfo in info.items():
                self.coinBalance[coin] = float(coinInfo["balance"])
                contracts = coinInfo["contracts"]
                # get each contract
                for iContract in contracts:
                    self.contractFreezed[coin + "_" + iContract["contract_type"]] = float(iContract["freeze"])
                    self.contractAvailable[coin + "_" + iContract["contract_type"]] = float(iContract["available"])
        else:
            # false return
            print("[ERR]: ", trader.coinUtilities.getCurrentTimeString(), "failed to obtain user info")
            pass

    # ---------------------------------------------------------------
    def on_return_my_trade(self, json_data):
        self.orderSendLock.acquire()
        self.orderSendLock.release()
        # print(json_data)
        # find strategyTag from ID
        ID = int(json_data["orderid"])
        if ID in self.orderID_strategyMap:
            # process trade data
            orderback = trader.coinTradeStruct.coinOrder()
            # get symbol and expiry
            contract_name = json_data["contract_name"]
            endIndex = 0
            for i, ch in enumerate(contract_name):
                if ch.isdigit():
                    endIndex = i
                    break
            if endIndex == 0:
                print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                      " failed to obtain symbol from order return, orderID: ", ID, \
                      file=sys.stderr)
                return
            data = {"apiType": self.API_name, \
                    "symbol" : (contract_name[:endIndex]).lower(), \
                    "expiry" : json_data["contract_type"], \
                    "orderID" : ID, \
                    "status" : self.okexStatusMap[json_data["status"]], \
                    "deal_amount" : json_data["deal_amount"], \
                    "orderUpdateTime" : time.time(), \
                    "strategyTag" : self.orderID_strategyMap[ID], \
                    "fillPrice": float(json_data["price_avg"])}
            orderback.setFutureOrderFeedback(data = data, isOrderBackOnly = True)
            self.onOrderBack(orderback)
        else:
            # use local internal ID
            # find the symbol in secondary API map
            contract_name = json_data["contract_name"]
            endIndex = 0
            for i, ch in enumerate(contract_name):
                if ch.isdigit():
                    endIndex = i
                    break
            if endIndex == 0:
                print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                      " failed to obtain symbol from order return, orderID: ", ID, \
                      file=sys.stderr)
                return
            symbol = (contract_name[:endIndex]).lower()
            if symbol in self.secondaryAPI_symbol_orders:
                # add this info into orderID_strategyMap
                strategyTag = self.secondaryAPI_symbol_orders[symbol]
                self.orderID_strategyMap[ID] = strategyTag
                # give this order back to strategy
                orderback = trader.coinTradeStruct.coinOrder()
                data = {"apiType": self.API_name, \
                        "symbol": symbol, \
                        "expiry": json_data["contract_type"], \
                        "orderID": ID, \
                        "status": self.okexStatusMap[json_data["status"]], \
                        "deal_amount": json_data["deal_amount"], \
                        "orderUpdateTime": time.time(), \
                        "strategyTag": strategyTag, \
                        "fillPrice": float(json_data["price_avg"])}
                orderback.setFutureOrderFeedback(data=data, isOrderBackOnly=True)
                # send it back to strategy
                self.onOrderBack(orderback)
                # need to delete the symbol out
                del self.secondaryAPI_symbol_orders[symbol]
            else:
                print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                      "failed to find ID ", ID, " in orderID strategy map", \
                      "and cannot find symbol", symbol, "in secondaryAPI_symbol_orders map", \
                      "in api", self.getAPI_name(), file=sys.stderr)
    # ---------------------------------------------------------------
    def on_return_ticker_marketData(self, data, ticker, expiry):
        # update last price and volume data
        self.tickerLastPriceVolume[ticker + expiry] = [data["data"]["last"], data["data"]["vol"]]

    # -------------------------- 回报处理 ----------------------------
    def on_message(self, ws, evt):
        """信息推送"""
        json_data = self.decompress_data(evt)
        # print (json_data)
        # 以 _ 分割, 提取depth、trade、登录后账户的合约信息
        channel = json_data.get("channel")
        # print(channel)
        # 根据channel调用不同的函数来处理回报
        if not channel:
            # channel为none时直接返回
            return
        split_channel = channel.split("_")

        if channel == "ok_futureusd_userinfo":
            # 账户信息回报，只推一次
            self.on_return_user_info(json_data)
            return

        if not self._is_login:
            if channel == "login" and json_data["data"]["result"]:
                # 登录信息
                print("---->>>> Login Success for", self.API_name, file=sys.stdout)
                self._is_login = True
                return
        else:  # 登录成功后才能开始处理行情

            if channel == "ok_sub_futureusd_userinfo":
                # 合约账户信息。相当于query的回报
                return

            if ("ok_sub_futureusd" in channel) and ("ticker" in channel):
                if len(split_channel) == 7:
                    self.on_return_ticker_marketData(json_data, split_channel[3], \
                                                     split_channel[-2] + "_" + split_channel[-1])
                else:
                    self.on_return_ticker_marketData(json_data, split_channel[3], \
                                                     split_channel[-1])
                return

            if channel == "ok_sub_futureusd_trades":
                # print(json_data)
                # 成交回报。相当于OnRtnOrder + OnRtnTrade
                self.on_return_my_trade(json_data["data"])
                return

            if "depth" in split_channel and "20" in split_channel:
                # depth 20行情
                symbol = split_channel[3]
                expiry = channel[27:-3]
                data = adapters.OKexStruct.OK_depth_Data()
                data.initFutureDepthData(symbol, expiry, json_data["data"], self.API_name)
                # set last price and vol
                if not self.tickerLastPriceVolume[symbol+expiry][0] is None:
                    data.LastPrice = float(self.tickerLastPriceVolume[symbol+expiry][0])
                    data.Volume = float(self.tickerLastPriceVolume[symbol+expiry][1])
                    self.onDepthData(data)
                return

            if ("trade" in split_channel) and ("sub" in split_channel) and (not "trades" in split_channel):
                # trade 行情。成交队列
                symbol = split_channel[3]
                expiry = channel[27:]
                # print(json_data)
                for idata in json_data["data"]:
                    dataDict = {"symbol" : symbol, \
                                "API" : self.API_name, \
                                "expiry" : expiry, \
                                "tradeId" : idata[0], \
                                "price" : idata[1], \
                                "amount" : idata[2], \
                                "timestamp" : idata[3], \
                                "tradeType" : trader.defines.tradeAsk if idata[4] == "ask" else trader.defines.tradeBid}
                    data = trader.coinTradeStruct.marketOrderInfo()
                    data.initFutureMarketOrderInfo(dataDict)
                    self.onMarketOrderBack(data)
                return
            if ("trade" in split_channel) and (not "sub" in split_channel) and (not "trades" in split_channel):
                # rest trade back, do not need to handle
                return
            if "positions" in split_channel:
                # position change feedback, do not need to handle right now
                # can add position check here (need to think how to do it)
                return

            # other info not handled
            print("[FEEDBACK]: ", json_data)

    def on_open(self, ws):
        # 一旦接口断开，重新连接，如果长时间没有请求，又会自动断开，因此所有的请求应该写在open函数中
        """连接成功，接口打开"""
        print("---->>> Connect Successfully for adapter ", self.API_name, file=sys.stdout)
        time.sleep(1)

        # 账户登录
        self.future_login()

        # 查询合约账户
        # self.request_future_user_info()
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

            # create maps for each symbol
            self.symbolSendOrderTimeListMap[splitFields[0]] = []

            # 订阅深度行情
            print ("subscribe future data for ", splitFields[0]+"-"+splitFields[1], file=sys.stdout)
            self.subscribe_future_depth(splitFields[0], splitFields[1], self._ok_const.DEPTH_20)

            # 订阅成交队列
            self.subscribe_future_trade(splitFields[0], splitFields[1])

            # 订阅合约行情
            self.subscribe_future_ticker(splitFields[0], splitFields[1])

            subscribeTickers.append(splitFields[0]+splitFields[1])


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
            print("[ERR]: ", trader.coinUtilities.getCurrentTimeString(), "connection closed for adapter", \
                  self.API_name, file=sys.stderr)
            self.reconnect()

    def on_error(self, ws, evt):
        """错误推送"""
        print("[ERR]: ", trader.coinUtilities.getCurrentTimeString(), 'onError', file=sys.stderr)
        print(evt, file=sys.stderr)

        # 记录错误日志
        # self.log.error(evt)

    def decompress_data(self, evt):
        inflated = self.inflate(evt)
        return (json.loads(inflated))[0]

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