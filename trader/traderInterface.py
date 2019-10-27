# encoding: UTF-8
import sys
from abc import ABCMeta, abstractmethod
sys.path.append("../")
import trader.coinTradeStruct
import configparser
import importlib
import trader.defines
import glob
import time
import datetime
import trader.coinUtilities

from queue import Queue, Empty
from threading import Thread
import sys, json, os
import numpy as np

# pure base class to define the adapter template
class adapterTemplate(object):
    __metaclass__ = ABCMeta

    def __init__(self):
        pass

    @abstractmethod
    def initAdapter(self):
        '''
        init adapter, read account info
        :return:
        '''
        pass
    @abstractmethod
    def stopDataFeed(self):
        '''
        stop feed data to strategy
        used when logout the system
        :return:
        '''
        pass
    @abstractmethod
    def addStrategy(self, strategy):
        '''
        subscribe strategy to this adapter
        so that the data and trade will be sent to the strategy from the adapter
        :param strategy:
        :return:
        '''
        pass

    @abstractmethod
    def getAPI_name(self):
        pass
    @abstractmethod
    def getWorkingTickers(self):
        pass
    @abstractmethod
    def startAdapter(self):
        '''
        start login and getting data and trade
        :return:
        '''
        pass
    @abstractmethod
    def stopAdapter(self):
        '''
        stop adapter and exit
        used when logout
        :return:
        '''
        pass
    @abstractmethod
    def sendOrder(self, order, isSecondaryAPI = False):
        pass
    @abstractmethod
    def sendOrderBySecondaryAPI(self, order):
        pass
    @abstractmethod
    def cancelOrder(self, order):
        pass
    @abstractmethod
    def onOrderBack(self, orderBack):
        pass
    @abstractmethod
    def onTradeBack(self, tradeBack):
        pass
    @abstractmethod
    def onErrorBack(self, errorMsg):
        pass
    @abstractmethod
    def onMarketOrderBack(self, marketTrade):
        '''
        for last market deal
        :param errorMsg:
        :return:
        '''
        pass
    @abstractmethod
    def onDepthData(self, depthData):
        pass
    @abstractmethod
    def setTrader(self, trader):
        '''
        link adapter to trade interface
        :param trader:
        :return:
        '''
    @abstractmethod
    def queryPositions(self):
        '''
        interface for tradeInterface to query position from market
        :return:
        '''
        pass
    @abstractmethod
    def getFreePosition(self, ticker, expiry, diretion = "long"):
        '''
        used in strategy to get the info of the account
        for spot, use the ticker name without the base coin
        :param ticker:
        :return:
        '''
        pass
    @abstractmethod
    def getFreezedPosition(self, ticker, expiry, direction = "long"):
        pass

    @abstractmethod
    def getCoinBalance(self, coin):
        '''
        for spot, return FreePosition + FreezedPosition
        for future, return balance
        :param coin:
        :return:
        '''
        pass
    @abstractmethod
    def loadLocalPositions(self):
        pass

    @abstractmethod
    def getPositionManager(self, symbol, expiry, strategyTag):
        pass

    @abstractmethod
    def getTickerLongPosition(self, symbol, expiry, strategyTag):
        pass

    @abstractmethod
    def getTickerShortPosition(self, symbol, expiry, strategyTag):
        pass

    @abstractmethod
    def getTickerOpenOrders(self, symbol, expiry, strategyTag):
        pass

    @abstractmethod
    def printEachPosition(self):
        pass

    @abstractmethod
    def printAdapterPosition(self):
        pass

    @abstractmethod
    def saveTickerPositions(self):
        pass

    @abstractmethod
    def setBaseCashValue(self, cashBaseValue):
        pass

####################################################
# traderInterface to manage all the adapters
#####################################################
class traderInterface(object):
    def __init__(self):
        self.tradeAdapterMap = {}
        self.logFolder = ""
        self.strategyConfigureFolder = ""
        self.path2orderLog =""
        self.path2PositionFolder = ""
        self.path2orderFeedbackLog = ""
        self.path2TradeFeedbackLog = ""
        self.path2MessageLog = ""
        self.AllStrategies = []
        self.__running = True
        self.cashBaseValue = {}

        # message queues
        self.__orderQueue = Queue()
        self.__orderBackQueue = Queue()
        self.__tradeBackQueue = Queue()
        self.__messageQueue = Queue()
        self.__logThread = Thread(target = self.__saveLog2File)
        self.__orderFile = None
        self.__orderBackFile = None
        self.__tradeBackFile = None
        self.__messageFile = None

    # .....................................................
    ### query infos
    # .....................................................
    def getPositionManager(self, symbol, expiry, strategyTag, API):
        if self.tradeAdapterMap[API]:
            return self.tradeAdapterMap[API].getPositionManager(symbol, expiry, strategyTag)
        else:
            return None

    def getTickerLongPosition(self, symbol, expiry, strategyTag, API):
        if self.tradeAdapterMap[API]:
            return self.tradeAdapterMap[API].getTickerLongPosition(symbol, expiry, strategyTag)
        else:
            return None

    def getTickerShortPosition(self, symbol, expiry, strategyTag, API):
        if self.tradeAdapterMap[API]:
            return self.tradeAdapterMap[API].getTickerShortPosition(symbol, expiry, strategyTag)
        else:
            return None

    def getTickerOpenOrders(self, symbol, expiry, strategyTag, API):
        if self.tradeAdapterMap[API]:
            return self.tradeAdapterMap[API].getTickerOpenOrders(symbol, expiry, strategyTag)
        else:
            return None
    # .....................................................
    # save get logs from queue and save into file
    def __saveLog2File(self):

        while self.__running:
            self.__saveLog()
            time.sleep(3)

        # out of the loop, close files
        self.__orderFile.close()
        self.__orderBackFile.close()
        self.__tradeBackFile.close()
        self.__messageFile.close()
        print("saved logs", file=sys.stdout)
    #.......................................................
    # init traders
    def initTraders(self):
        # read platform config file to init trade adapters
        configurePath = glob.glob(trader.defines.traderConfigureFile)
        config = configparser.ConfigParser()
        config.read(configurePath)
        # get config
        self.logFolder = config["configure"]["path2LogFile"]
        self.strategyConfigureFolder = config["configure"]["path2StrategyConfig"]
        self.path2PositionFolder = config["configure"]["path2Position"]
        self.path2Cache = config["configure"]["path2Cache"]

        # get cash base value
        self.cashBaseValue = dict(config.items("baseCashValues"))
        # convert string to float
        self.cashBaseValue = dict(zip(self.cashBaseValue.keys(), \
                                      [float(value) for value in self.cashBaseValue.values()]))

        # create log files
        timeNow = datetime.datetime.now()
        self.path2orderLog = self.logFolder + "/order_" + timeNow.strftime("%Y%m%d_%H%M%S") + ".csv"
        self.path2orderFeedbackLog = self.logFolder + "/orderBack_" + timeNow.strftime("%Y%m%d_%H%M%S") + ".csv"
        self.path2TradeFeedbackLog = self.logFolder + "/tradeBack_" + timeNow.strftime("%Y%m%d_%H%M%S") + ".csv"
        self.path2MessageLog = self.logFolder + "/message_" + timeNow.strftime("%Y%m%d_%H%M%S") + ".csv"
        self.__orderFile = open(self.path2orderLog, "w")
        self.__orderFile.write("symbol, expiry, amount, price, orderType, leverage, strategyTag, orderID, isCancel\n")
        self.__orderFile.flush()
        self.__orderBackFile = open(self.path2orderFeedbackLog, "w")
        self.__orderBackFile.write("createTime, updateTime, symbol, expiry, filledAmount, price, orderType, leverage, strategyTag, orderStatus, orderID, fillPrice\n")
        self.__orderBackFile.flush()
        self.__tradeBackFile = open(self.path2TradeFeedbackLog, "w")
        self.__tradeBackFile.write("symbol, expire, tradeAmount, tradePrice, strategyTag, orderID\n")
        self.__tradeBackFile.flush()
        self.__messageFile = open(self.path2MessageLog, "w")

        # get api
        for iAPI in trader.defines.All_API_List:
            apiAvailable = config.getboolean(iAPI, "isActive")
            if apiAvailable:
                # create api and save to map
                moduleName = config[iAPI]["apiModule"]
                apiName = config[iAPI]["apiName"]
                module = importlib.import_module(moduleName)
                iclass = getattr(module, apiName)
                newAPI = iclass()
                self.tradeAdapterMap[iAPI] = newAPI
                #init API
                apiKey = config[iAPI]["apiKey"]
                secretKey = config[iAPI]["secretKey"]
                webURL = config[iAPI]["webURL"]
                openFeeRate = config.getfloat(iAPI, "openFeeRate")
                closeFeeRate = config.getfloat(iAPI, "closeFeeRate")
                restURL = config[iAPI]["restURL"]
                loginInfoDict = {"apiKey":apiKey, "secretKey":secretKey, \
                             "webURL":webURL, "apiName":apiName, "restURL":restURL, \
                                 "openFeeRate":openFeeRate, "closeFeeRate":closeFeeRate, \
                                 "path2PositionFolder":self.path2PositionFolder}
                self.tradeAdapterMap[iAPI].initAdapter(loginInfoDict)
                self.tradeAdapterMap[iAPI].setBaseCashValue(self.cashBaseValue)

            else:
                self.tradeAdapterMap[iAPI] = None

        return True

    # .......................................................
    # init strategies
    def initStrategies(self):
        '''
        need to be called after initTraders
        :return:
        '''
        # find all configure files in the folder config
        configureFiles = glob.glob(self.strategyConfigureFolder + "/*.ini")
        if os.path.isfile(self.path2Cache):
            cache = json.load(open(self.path2Cache, "r"))
        else:
            cache = {}
        for iFile in configureFiles:
            # read configure and create strategies one by one
            config = configparser.ConfigParser()
            config.read(iFile)
            iStrategyModule = config["Strategy"]["strategyModule"]
            iStrategy = config["Strategy"]["strategyName"]
            iStrategyTag = config["Strategy"]["strategyTag"]
            # create strategy instance
            module = importlib.import_module(iStrategyModule)
            iclass = getattr(module, iStrategy)
            iStrategyInstance = iclass(iFile, cache.get(iStrategyTag, {}))
            # ini strategy
            iStrategyInstance.setStrategyTag(iStrategyTag)
            initSuccess = iStrategyInstance.initStrategy()
            if not initSuccess:
                print("[ERR]: ", trader.coinUtilities.getCurrentTimeString(), "load configure file failed for ", \
                      iStrategyTag, "in file", iFile)
                continue

            self.AllStrategies.append(iStrategyInstance)
        return True
    # .......................................................
    # link strategies with traders
    def linkStrategies(self):
        '''
        will be called after initStrategies
        go through each strategy and bind the strategy with the required api
        '''
        for iStrategy in self.AllStrategies:
            # get strategy required api
            strategyAPIs = iStrategy.getAPIs()
            for iAPI in strategyAPIs:
                if self.tradeAdapterMap[iAPI]:
                    self.tradeAdapterMap[iAPI].addStrategy(iStrategy)
                else:
                    print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), " api ", \
                          iAPI, "is not init", file=sys.stderr)
        return True
    #.........................................................
    # link trader
    def linkTrader(self, trader):
        '''
        will be called after linkStrategies
        go through each strategy and bind the strategy with trader
        '''
        for iStrategy in self.AllStrategies:
            iStrategy.setTrader(trader)
        for iAdapter in self.tradeAdapterMap.values():
            if not iAdapter is None:
                iAdapter.setTrader(trader)
        return True
    # .......................................................
    # start trade adapters
    def startAdapters(self):
        # start log thread
        self.__logThread.start()
        # start adapters
        for adapterName, adapter in self.tradeAdapterMap.items():
            if adapter:
                adapter.startAdapter()
    # .......................................................
    # send order
    def sendOrder(self, order, isSecondaryAPI = False):
        api = self.tradeAdapterMap[order.getAPI_name()]
        if not api is None:
            retVal = api.sendOrder(order, isSecondaryAPI)
            self.writeOrderInfo2File(order)
            return retVal
        else:
            print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), " api ", order.getAPI_name(), \
                  " is not actived for order from strategy ", \
                  order.getStrategyTag(), file=sys.stderr)
            return False

    # .......................................................
    # cancel order
    def cancelOrder(self, order):
        api = self.tradeAdapterMap[order.getAPI_name()]
        if api:
            retVal = api.cancelOrder(order)
            self.writeOrderInfo2File(order)
            return retVal
        else:
            print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), " api ", \
                  order.getAPI_name(), " is not actived for order from strategy ", \
                  order.getStrategyTag(), file=sys.stderr)
            return False

    # .......................................................
    def getFreePosition(self, ticker, expiry, api, direction = "long"):
        if self.tradeAdapterMap[api]:
            return self.tradeAdapterMap[api].getFreePosition(ticker, expiry, direction)
        else:
            print("[ERR]: ", trader.coinUtilities.getCurrentTimeString(), \
                  "api", api, " is not activated for position query")
            return 0.0

    def getFreezedPosition(self, ticker, expiry, api, direction = "long"):
        if self.tradeAdapterMap[api]:
            return self.tradeAdapterMap[api].getFreezedPosition(ticker, expiry, direction)
        else:
            print("[ERR]: ", trader.coinUtilities.getCurrentTimeString(), \
                  "api", api, " is not activated for position query")
            return 0.0

    def getCoinBalance(self, coin, api):
        if self.tradeAdapterMap[api]:
            return self.tradeAdapterMap[api].getCoinBalance(coin)
        else:
            print("[ERR]: ", trader.coinUtilities.getCurrentTimeString(), \
                  "api", api, " is not activated for CoinBalance")
            return 0.0
    # ........................................................
    # query positions
    def queryPositions(self):
        for iAdapater in self.tradeAdapterMap.values():
            if iAdapater:
                iAdapater.queryPositions()
    #.........................................................
    # we do position match here
    def matchPositions(self, adapterPositions):
        '''
        1. position match is done in every 1 minute, right after position query
        2. only match the positions we have in strategies
        3. give a tolerance to the match because float precisions
        4. if 3 time mismatch, give warning
        :return:
        '''
        # loop over each adapter
        for iAdapter in self.tradeAdapterMap.values():
            if iAdapter:
                #iAdapterPosition
                #iAapter.matchPositions()
                pass

        pass

    # .........................................................
    # exit system
    def exitSystem(self):
        # stop sending data
        print("------ begin exit system", file=sys.stdout)
        for iAdapater in self.tradeAdapterMap.values():
            if iAdapater:
                print("stop data for", iAdapater.getAPI_name(), file=sys.stdout)
                try:
                    iAdapater.stopDataFeed()
                except Exception as e:
                    print("[ERR]: failed to stop data feeder for", iAdapater.getAPI_name(), str(e), file=sys.stderr)
        print("exited data feeder", file=sys.stdout)
        sys.stdout.flush()
        sys.stderr.flush()
        time.sleep(2)

        # exit strategies
        cache = json.load(open(self.path2Cache, "r"))
        for iStrategy in self.AllStrategies:
            # TODO: deep update is better
            if iStrategy.getStrategyTag() in cache:
                cache[iStrategy.getStrategyTag()].update(iStrategy.getState())
            else:
                cache[iStrategy.getStrategyTag()] = iStrategy.getState()
            try:
                iStrategy.exitStrategy()
            except:
                print("[ERR]: failed to exitStrategy for", iStrategy.getStrategyTag())

        json.dump(cache, open(self.path2Cache, "w"), indent=4, sort_keys=True)
        print("exited strategies", file=sys.stdout)
        sys.stdout.flush()
        sys.stderr.flush()
        time.sleep(3)

        # exit adapters
        for iAdapater in self.tradeAdapterMap.values():
            if iAdapater:
                print("begin exit adapter", iAdapater.getAPI_name(), file=sys.stdout)
                try:
                    iAdapater.stopAdapter()
                except Exception as e:
                    print("[ERR]: failed to stop adapter", str(e), iAdapater.getAPI_name())
        sys.stdout.flush()
        sys.stderr.flush()

        # stop the log thread
        self.__running = False
        # save system info, if any
        time.sleep(5)
        try:
            self.__logThread.join(timeout=1.0)
        except Exception as e:
            print("Exception", str(e))

    # ........................................................
    # save order info into log queue
    def writeOrderInfo2File(self, order):
        message = order.printOrderInfo()
        self.__orderQueue.put(message)
    def writeOrderBackInfo2File(self, orderBack):
        message = orderBack.printOrderBackInfo()
        self.__orderBackQueue.put(message)
    def writeTradeBackInfo2File(self, tradeBack):
        message = tradeBack.printTradeBackInfo()
        self.__tradeBackQueue.put(message)
    def writeUserMessage2File(self, mesg):
        self.__messageQueue.put(mesg)
    # ........................................................
    def __saveLog(self):
        try:
            if not self.__orderFile is None:
                while True:
                    message = self.__orderQueue.get(block=True, timeout=1)
                    self.__orderFile.write(message)
                    self.__orderFile.flush()
        except Empty:
            pass

        try:
            if not self.__orderBackQueue is None:
                while True:
                    message = self.__orderBackQueue.get(block=True, timeout=1)
                    self.__orderBackFile.write(message)
                    self.__orderBackFile.flush()
        except Empty:
            pass

        try:
            if not self.__tradeBackQueue is None:
                while True:
                    message = self.__tradeBackQueue.get(block=True, timeout=1)
                    self.__tradeBackFile.write(message)
                    self.__tradeBackFile.flush()
        except Empty:
            pass

        try:
            if not self.__messageQueue is None:
                while True:
                    message = self.__messageQueue.get(block=True, timeout=1)
                    self.__messageFile.write(message)
                    self.__messageFile.flush()
        except Empty:
            pass
    # ........................................................
    # function to print position info
    def printPositionInfo(self):
        # get current time
        currentTimeString = trader.coinUtilities.getCurrentTimeString()
        # loop over each strategy to print position info for each strategy
        # for each strategy
        # header
        # if np.random.randint(100) > 60:
        #     print("############################################ strategy", currentTimeString,\
        #           "##################################################", file=sys.stdout)
        #     print("{0: <8s} {1: <10s} {2: <20s} {3: <10s} {4: <10s} {5: <10s} {6: <10s} {7: <8s}".format( \
        #         "symbol", "expiry", "api", "long", "short", "strategy", "gross", "fee"), file=sys.stdout)
        #     for i, adapter in self.tradeAdapterMap.items():
        #         if adapter:
        #             adapter.printEachPosition()

        #     # for all strategies of each adapter
        #     print("############################################ adapters", currentTimeString, \
        #           "###################################################", file=sys.stdout)
        #     print("{0: <8s} {1: <10s} {2: <20s} {3: <10s} {4: <10s} {5: <10s} {6: <10s} {7: <8s}".format( \
        #         "symbol", "expiry", "api", "long", "short", "strategy", "gross", "fee"), file=sys.stdout)
        #     for i, adapter in self.tradeAdapterMap.items():
        #         if adapter:
        #             adapter.printAdapterPosition()

        #     print("#####################################################################################################", \
        #           file=sys.stdout)

        # sys.stdout.flush()
