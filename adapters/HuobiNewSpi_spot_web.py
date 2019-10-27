# -*- coding: utf-8 -*-
import sys
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
from queue import Queue, Empty
import copy
import json
import zlib
import gzip
import time
import websocket
import base64
import hmac
import hashlib
from urllib import parse
from datetime import datetime


class HuobiNewSpi_spot_web(trader.traderInterface.adapterTemplate):
    def __init__(self):
        super(HuobiNewSpi_spot_web, self).__init__()
        self._api_key = ''  # 用户名
        self._secret_key = ''  # 密码
        self._host = ''  # 服务器地址
        # self._apiHost = "api.huobipro.com"
        self._apiHost = "api.huobi.pro"
        self._apiPath = "/ws/v1"
        self._dataHost = "wss://api.huobi.com/ws"
        self._ws = None  # websocket应用对象, for account info
        self._wsData = None
        self._restURL = ''
        self.dataThread = None
        self.accountThread = None
        self.API_name = ""
        self._authdata = None
        self._cidCount = 10
        self._accoutnWsConnecting = False
        self._accountAuthed = False

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
        self.openOrderIDQueue = []
        self.orderId_orderStatusMap = {}
        self.queryOrderStatusQueue = Queue()

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
        print("host", self._host)
        print("_apiHost", self._apiHost)
        print("_apiPath", self._apiPath)

        websocket.enableTrace(False)
        self._ws = websocket.WebSocketApp(self._host,
                                          on_message=self.on_message,
                                          on_error=self.on_error,
                                          on_close=self.on_close,
                                          on_open=self.on_open)

        # start ws for account
        self.accountThread = Thread(target=self._ws.run_forever)
        self.accountThread.start()

        self._wsData =  websocket.WebSocketApp(self._dataHost,
                                          on_message=self.on_messageData,
                                          on_error=self.on_errorData,
                                          on_close=self.on_closeData,
                                          on_open=self.on_openData)

        # start ws for data
        self.dataThread = Thread(target=self._wsData.run_forever)
        self.dataThread.start()

        # rest api
        self.restAPI = adapters.huobiRest.HuobiServices.HuobiRestAPI(self._api_key, self._secret_key)


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
        # unsubscribe
        if self._wsData is None:
            return
        for ticker in self.workingTickers:
            # here we need to convert system ticker name to huobi ticker name
            splitStr = ticker.split("_")
            huobiTicker = splitStr[0] + splitStr[1]

            self._cidCount = self._cidCount + 1
            detailStr = """{"unsub": "market.""" + huobiTicker + """.detail", "id":""" + '"' + str(
                self._cidCount) + '"}'
            print("unsub:", detailStr)
            try:
                self._wsData.send(detailStr)
            except Exception as e:
                print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                      "ubsub exception for", self.getAPI_name(), str(e))

            self._cidCount = self._cidCount + 1
            depthStr = """{"unsub": "market.""" + huobiTicker + """.depth.step0", "id":""" + '"' + str(
                self._cidCount) + '"}'
            print("unsub:", depthStr)
            try:
                self._wsData.send(depthStr)
            except Exception as e:
                print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                      "ubsub exception for", self.getAPI_name(), str(e))

            self._cidCount = self._cidCount + 1
            tradeStr = """{"unsub": "market.""" + huobiTicker + """.trade.detail", "id":""" + '"' + str(
                self._cidCount) + '"}'
            print("unsub:", tradeStr)
            self._wsData.send(tradeStr)

    def stopAdapter(self):
        self.adapterOn = False
        # before close connect, save positions
        self.saveTickerPositions()
        print("saved ticker positions info for adapter", self.getAPI_name())
        self.close_connect()

    def sendOrder(self, order, isSecondaryAPI=False):
        # check send frequency
        self.timeLimitCoolDown()
        # order will be sent through rest API first
        self.orderSendLock.acquire()
        ret = None
        try:
            if not self._accoutnWsConnecting:
                self.orderSendLock.release()
                print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                      "try to send order while account ws is not connected for", self.getAPI_name())
                return
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
            self.orderId_orderStatusMap[orderID] = trader.defines.orderInit
            self.openOrderIDQueue.append(orderID)
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

    # -------------------------- self defined functions --------------------------
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
            # init lastprice and volume
            self.tickerLastPriceVolume[huobiTicker] = [None, None]

            self._cidCount = self._cidCount + 1
            detailStr = """{"sub": "market.""" + huobiTicker + """.detail", "id":""" + '"' + str(self._cidCount) + '"}'
            print("sub:", detailStr)
            self._wsData.send(detailStr)
            time.sleep(0.1)

            self._cidCount = self._cidCount + 1
            depthStr = """{"sub": "market.""" + huobiTicker + """.depth.step0", "id":""" + '"' + str(self._cidCount) + '"}'
            print("sub:", depthStr)
            self._wsData.send(depthStr)
            time.sleep(0.1)

            self._cidCount = self._cidCount + 1
            tradeStr = """{"sub": "market.""" + huobiTicker + """.trade.detail", "id":""" + '"' + str(self._cidCount) + '"}'
            print("sub:", tradeStr)
            self._wsData.send(tradeStr)
            time.sleep(0.1)

    def registAccountInfo(self):
        # wait until auth finished
        count = 0
        while not self._accountAuthed and count < 10:
            print("waiting for auth ...")
            time.sleep(1)
            count += 1
        if count >= 10:
            print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                  "failed to auth for adapter", self.getAPI_name())
            return
        # for orders
        '''
        for ticker in self.workingTickers:
            # here we need to convert system ticker name to huobi ticker name
            splitStr = ticker.split("_")
            huobiTicker = splitStr[0] + splitStr[1]
            self._cidCount = self._cidCount + 1
            orderStr = """{"op": "sub", "cid": """ + '"' + str(self._cidCount) + '"' + """, "topic": "orders.""" + str(huobiTicker) +'"}'
            print("sub:", orderStr)
            self._ws.send(orderStr)
            time.sleep(0.1)
        '''

        self._cidCount = self._cidCount + 1
        orderStr = """{"op": "sub", "cid": """ + '"' + str(self._cidCount) + '"' + """, "topic": "orders.*""" + '"}'
        print("sub:", orderStr)
        self._ws.send(orderStr)

        # for accounts
        self._cidCount = self._cidCount + 1
        accountStr = """{"op": "sub", "cid": """ + '"' + str(self._cidCount) + '"' + """, "topic": "accounts""" + '"}'
        print("sub:", accountStr)
        self._ws.send(accountStr)

        # query positions
        self._cidCount = self._cidCount + 1
        queryPosMessage = """{"op": "req","topic": "accounts.list", "cid": """ + '"' + str(self._cidCount) + '"}'
        print("query", queryPosMessage)
        self._ws.send(queryPosMessage)

    def _auth(self, auth):
        authenticaton_data = auth[1]
        _accessKeySecret = auth[0]
        authenticaton_data['Signature'] = self._sign(authenticaton_data, _accessKeySecret)
        # print(authenticaton_data)
        return json.dumps(authenticaton_data)  # 鉴权一次中止操作

    # 签名
    def _sign(self, param=None, _accessKeySecret=None):
        # 签名:
        if param is None:
            params = {}
        params = {}
        params['SignatureMethod'] = param.get('SignatureMethod') if type(param.get('SignatureMethod')) == type(
            'a') else '' if param.get('SignatureMethod') else ''
        params['SignatureVersion'] = param.get('SignatureVersion') if type(param.get('SignatureVersion')) == type(
            'a') else '' if param.get('SignatureVersion') else ''
        params['AccessKeyId'] = param.get('AccessKeyId') if type(param.get('AccessKeyId')) == type(
            'a') else '' if param.get('AccessKeyId') else ''
        params['Timestamp'] = param.get('Timestamp') if type(param.get('Timestamp')) == type('a') else '' if param.get(
            'Timestamp') else ''
        # print(params)
        # 排序:
        keys = sorted(params.keys())
        # 加入&
        qs = '&'.join(['%s=%s' % (key, self._encode(params[key])) for key in keys])
        # 请求方法，域名，路径，参数 后加入`\n`
        payload = '%s\n%s\n%s\n%s' % ('GET', self._apiHost, self._apiPath, qs)
        dig = hmac.new(_accessKeySecret, msg=payload.encode('utf-8'), digestmod=hashlib.sha256).digest()
        return base64.b64encode(dig).decode()

    def _utc(self):
        return datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')

    def _encode(self, s):
        # return urllib.pathname2url(s)
        return parse.quote(s, safe='')

    # 获取当前时间
    def get_now_time(self):
        timestamp = int(time.time())
        time_local = time.localtime(timestamp)
        now_time = time.strftime("%Y-%m-%d %H:%M:%S", time_local)
        return now_time

    def get_authdata(self):
       authdata = [
            self._secret_key.encode('utf-8'),
            {
                "op": "auth",
                "AccessKeyId": self._api_key,
                "SignatureMethod": "HmacSHA256",
                "SignatureVersion": "2",
                "Timestamp": self._utc()
            }
        ]
       return authdata

    def sub_account(self):
        self._cidCount = self._cidCount + 1
        accountData = {
            "op" : "sub",
            "cid": str(self._cidCount + 1),
            "topic": "accounts"
        }
        return accountData
    # -----------------------------------------------------------------
    # process messages
    def on_message(self, ws, message):
        data = str(zlib.decompressobj(31).decompress(message), encoding="utf-8")
        # data = gzip.decompress(message).decode('utf-8')
        print(data)
        if 'ping' in data:
            ping_id = json.loads(data).get('ts')
            pong_data = '{"op":"pong","ts": %s}' % ping_id
            ws.send(pong_data)
        elif 'err-code' in data:
            errCod = json.loads(data).get('err-code')
            if int(errCod) != 0:
                print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                      "get error code feedback:", data)
            elif "topic" in data:
                json_data = json.loads(data)
                topic = json_data["topic"]
                if topic == "accounts.list":
                    accountInfo = json_data["data"]
                    for iAccountInfo in accountInfo:
                        if iAccountInfo["type"] == "spot":
                            balances = iAccountInfo["list"]
                            self.positionQueryLock.write_acquire()
                            for iBalance in balances:
                                if iBalance["type"] == "trade":
                                    # update available amount
                                    self.eachTickerFreePosition[iBalance["currency"]] = float(iBalance["balance"])
                                elif iBalance["type"] == "frozen":
                                    self.eachTickerFreezedPosition[iBalance["currency"]] = float(iBalance["balance"])
                                else:
                                    print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                                          "obtained unknown balance type in", data)
                            self.positionQueryLock.write_release()
                        else:
                            # for other type of balances
                            pass
                elif topic == "orders.*":
                    print("order regiested")
                    self._accoutnWsConnecting = True
                else:
                    # for other req topics
                    pass
            else:
                if ("op" in data) and ("auth" in data) and("user-id" in data):
                    self._accountAuthed = True
                    print("authed feedback")
                    # regiest tickers
                    self.registAccountInfo()
                print("unprocessed data", data)
        else:
            # process the message
            json_data = json.loads(data)
            if ("topic" in json_data.keys()) and ("data" in json_data.keys()):
                topic = json_data["topic"]
                if "orders." in topic:
                    # process order feedback
                    orderInfo = json_data["data"]
                    # find orderID
                    orderID = int(orderInfo["order-id"])
                    self.orderSendLock.acquire()
                    if orderID in self.orderID_strategyMap:
                        strategyTag = self.orderID_strategyMap[orderID]
                        self.orderSendLock.release()

                        # need to check the order back is partial filled and cancel or not, need special treatment for this case
                        orderStatus = orderInfo["order-state"]
                        if orderStatus == "partial-canceled":
                            if self.handlePartialCanceledOrder(orderID, strategyTag, self.HuobiTickerToOkexTickerDict[orderInfo["symbol"]], json_data["ts"]):
                                # if order is finished, delete from open order Queue
                                if orderID in self.openOrderIDQueue:
                                    self.orderSendLock.acquire()
                                    self.openOrderIDQueue.remove(orderID)
                                    self.orderSendLock.release()
                            else:
                                # save the id into query queue, will request the status later again
                                self.queryOrderStatusQueue.put((orderID, strategyTag, self.HuobiTickerToOkexTickerDict[orderInfo["symbol"]], json_data["ts"]))
                            return

                        # check order status is finished or not, only need to send the feedback that the status is NOT finished
                        if self.orderId_orderStatusMap[orderID] >= trader.defines.orderFilled:
                            return

                        orderback = trader.coinTradeStruct.coinOrder()
                        orderbackDict = {"apiType": self.API_name, \
                                         "symbol": self.HuobiTickerToOkexTickerDict[orderInfo["symbol"]], \
                                         "orderID": orderID, \
                                         "status": self.OrderStatus2SystemOrderStatusDict[orderStatus], \
                                         "deal_amount": float(orderInfo["filled-amount"]), \
                                         "orderUpdateTime": json_data["ts"], \
                                         "strategyTag": strategyTag, \
                                         "fillPrice": float(orderInfo["price"])
                                         }
                        orderback.setSpotOrderFeedback(data=orderbackDict, isOrderBackOnly=True)
                        self.onOrderBack(orderback)
                        # update order status
                        self.orderId_orderStatusMap[orderID] = orderbackDict["status"]
                        # if order is finished, delete from open order Queue
                        if orderbackDict["status"] >= trader.defines.orderFilled:
                            if orderID in self.openOrderIDQueue:
                                self.orderSendLock.acquire()
                                self.openOrderIDQueue.remove(orderID)
                                self.orderSendLock.release()
                    else:
                        self.orderSendLock.release()
                        print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                              "failed to find orderID", orderID, "in orderID_strategyMap for adapter", self.getAPI_name())
                elif "accounts" in topic:
                    # process account update info
                    accountInfo = json_data["data"]["list"]
                    self.positionQueryLock.write_acquire()
                    for iBalance in accountInfo:
                        if iBalance["type"] == "trade":
                            # update available amount
                            self.eachTickerFreePosition[iBalance["currency"]] = float(iBalance["balance"])
                        elif iBalance["type"] == "frozen":
                            self.eachTickerFreezedPosition[iBalance["currency"]] = float(iBalance["balance"])
                        else:
                            print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                                  "obtained unknown balance type in", data)
                    self.positionQueryLock.write_release()
                else:
                    # other feedbacks
                    print(data, file=sys.stderr)
            else:
                # unknown feedback, write down
                print(data, file=sys.stderr)

    def on_error(self, ws, errorMsg):
        # data = str(zlib.decompressobj(31).decompress(errorMsg), encoding="utf-8")
        # data = gzip.decompress(errorMsg).decode('utf-8')
        print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), "on_error", self.getAPI_name(), errorMsg)

    def on_close(self, ws):
        """接口断开"""
        print('[WARN]: onClose for　account ws of adapter ', self.API_name, file=sys.stdout)
        self._accoutnWsConnecting = False
        self._accountAuthed = False

        # 重新连接
        if self.adapterOn:
            print("[ERR]: ", trader.coinUtilities.getCurrentTimeString(), \
                  '---->>> onClose for account ws of adapter ', self.API_name, file=sys.stderr)
            print("will reconnect in 5 s", file=sys.stderr)
            time.sleep(5)
            self.reconnectAccountWs()

    def reconnectAccountWs(self):
        if not self.adapterOn:
            return
        # try to close old connection
        self.close_AccountConnect()
        # open new connection
        self._ws = websocket.WebSocketApp(self._host,
                                          on_message=self.on_message,
                                          on_error=self.on_error,
                                          on_close=self.on_close,
                                          on_open=self.on_open)

        # start ws for account
        self.accountThread = Thread(target=self._ws.run_forever)
        self.accountThread.start()

    def on_open(self, ws):
        print("---->>> Connect Successfully for account ws of", self.API_name, file=sys.stdout)
        self.dataFeedOn = True
        time.sleep(1)
        # auth account info
        _auth = self._auth(self.get_authdata())
        ws.send(_auth)

        time.sleep(1)
        # here we query open order status, if it is reconnecting, need to make sure we did not miss any feedback
        try:
            self.queryOpenOrderStatus()
        except Exception as e:
            print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                  "exception caught in adapter when connecting", self.getAPI_name(), str(e))
        # time.sleep(2)
        # self._accoutnWsConnecting = True

    # process messages
    def on_messageData(self, ws, message):
        # data = str(zlib.decompressobj(31).decompress(message), encoding="utf-8")
        data = gzip.decompress(message).decode('utf-8')
        # print(data)
        if data[:7] == '{"ping"':
            # print(data)
            # echo pong
            ts = data[8:21]
            pong = '{"pong":' + ts + '}'
            ws.send(pong)

            # query status for open orders in queue
            self.queryQueuedOpenOrderStatus()
        else:
            # process the message
            json_data = json.loads(data)
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

    def on_errorData(self, ws, errorMsg):
        # data = str(zlib.decompressobj(31).decompress(errorMsg), encoding="utf-8")
        # data = gzip.decompress(errorMsg).decode('utf-8')
        print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), "on_errorData", self.getAPI_name(), errorMsg)

    def on_closeData(self, ws):
        """接口断开"""
        print('[WARN]: onClose for　data ws of adapter ', self.API_name, file=sys.stdout)
        # 重新连接
        if self.adapterOn:
            print("[ERR]: ", trader.coinUtilities.getCurrentTimeString(), \
                  '---->>> onClose for data ws of adapter ', self.API_name, file=sys.stderr)
            print("will reconnect in 5 s", file=sys.stderr)
            self.dataFeedOn = False
            time.sleep(5)
            self.reconnectDataWs()

    def reconnectDataWs(self):
        if not self.adapterOn:
            return
        # try to close old
        self.close_DataConnect()
        # open new connect
        self._wsData = websocket.WebSocketApp(self._dataHost,
                                              on_message=self.on_messageData,
                                              on_error=self.on_errorData,
                                              on_close=self.on_closeData,
                                              on_open=self.on_openData)

        # start ws for data
        self.dataThread = Thread(target=self._wsData.run_forever)
        self.dataThread.start()


    def on_openData(self, ws):
        print("---->>> Connect Successfully for data", self.API_name, file=sys.stdout)

        # check account ws is connected
        count = 0
        while not self._accoutnWsConnecting and count < 10:
            print("connecting account ws ...")
            time.sleep(1)
            count += 1
        if count >= 10:
            print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                  "wss failed to connect for adatper", self.getAPI_name())
            return

        time.sleep(5)
        self.dataFeedOn = True
        # regiest tickers
        self.regiestTickers()

        # query positions
        # self.queryPositions()

    def close_AccountConnect(self):
        self.dataFeedOn = False
        self._accoutnWsConnecting = False
        time.sleep(1)
        if self.accountThread and self.accountThread.isAlive():
            try:
                self._ws.close()
            except Exception as e:
                print("Exception for account ws of ", self.getAPI_name(), str(e))

    def close_DataConnect(self):
        self.dataFeedOn = False

        time.sleep(1)
        if self.dataThread and self.dataThread.isAlive():
            try:
                self._wsData.close()
            except Exception as e:
                print("Exception for data ws of ", self.getAPI_name(), str(e))

    def close_connect(self):
        """关闭接口"""
        self.close_DataConnect()
        self.close_AccountConnect()

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
        self.tickerLastPriceVolume[symbol] = [data["tick"]["close"], data["tick"]["amount"]]  # "vol" is amount * price
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

    def queryOpenOrderStatus(self):
        '''
        used to query open order status when wss link failed and automatically relinking
        :return:
        '''
        finishedID = []
        for id in self.openOrderIDQueue:
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
                      "failed to get order status when relinking for id", str(id), self.getAPI_name())
                continue

            print("[ORDERBACK]", ret, file=sys.stderr)

            if (ret is None) or (ret["status"] != "ok"):
                print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                      "obtained error order query back when relinking", ret, self.getAPI_name(), file=sys.stderr)
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
                    continue
                else:
                    # obtain the failed amount of the partially-cancelled order
                    print("[orderBack_2]:", retMatch)
                    if (retMatch is None) or (retMatch["status"] != "ok"):
                        print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                              "obtained error partially cancel order query back", retMatch, file=sys.stderr)
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
                                  "obtain 0 filledAmount for partially cancelled order", str(id),
                                  "will re-query later again")
                            continue
                        fillPrice = filledCashAmount / filledAmount

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

                # if order is finished, remove from the queue
                if data["status"] >= trader.defines.orderFilled:
                    finishedID.append(id)

        # remove finished ID from list
        for iId in finishedID:
            self.openOrderIDQueue.remove(iId)

    def handlePartialCanceledOrder(self, orderID, strategyTag, systemSymbol, timeStamp):
        retMatch = None
        try:
            self.timeLimitCoolDown()
            retMatch = self.restAPI.order_matchresults(str(orderID))
        except:
            print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                  "failed to get partial-canceled order status for id", str(id), self.getAPI_name(),
                  " will get it later again", file=sys.stderr)
            return False
        else:
            # obtain the failed amount of the partially-cancelled order
            print("[orderBack_2]:", retMatch)
            if (retMatch is None) or (retMatch["status"] != "ok"):
                print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                      "obtained error partially cancel order query back", retMatch, file=sys.stderr)
                return False
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
                    return False
                fillPrice = filledCashAmount / filledAmount
                # send orderback to strategy
                orderback = trader.coinTradeStruct.coinOrder()
                orderbackDict = {"apiType": self.API_name, \
                                 "symbol": systemSymbol, \
                                 "orderID": orderID, \
                                 "status": trader.defines.orderCancelled, \
                                 "deal_amount": filledAmount, \
                                 "orderUpdateTime": timeStamp, \
                                 "strategyTag": strategyTag, \
                                 "fillPrice": fillPrice
                                 }
                orderback.setSpotOrderFeedback(data=orderbackDict, isOrderBackOnly=True)
                # update order status
                self.orderId_orderStatusMap[orderID] = trader.defines.orderCancelled
                self.onOrderBack(orderback)
                return True

    def queryQueuedOpenOrderStatus(self):
        # firstly, copy out the query order queue
        recheckQueryList = []
        while not self.queryOrderStatusQueue.empty():
            data = None
            try:
                data = self.queryOrderStatusQueue.get(block=True, timeout=1)  # 获取事件的阻塞时间设为1秒
            except Empty:
                break
            else:
                if data:
                    # query order status
                    if not self.handlePartialCanceledOrder(data[0], data[1], data[2], data[3]):
                        # query failed, save to buffer, need to query it again later
                        recheckQueryList.append(data)
        # add recheck queries to query queue
        for iQuery in recheckQueryList:
            self.queryOrderStatusQueue.put(iQuery)
