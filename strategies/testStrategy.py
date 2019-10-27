# encoding: UTF-8
import sys
sys.path.append("../")
import strategies.baseStrategy
import configparser
import threading
import trader.coinUtilities
import trader.coinTradeStruct
import trader.defines


class testStrategy(strategies.baseStrategy.baseStrategy):
    # 目前假设每次固定间隔推送数据
    """
    this strategy we use a simple moving average algo as a demo
    """

    def __init__(self, path2Config, cache):
        super(testStrategy, self).__init__(path2Config, cache)

        self.testStrategyThreadLock = threading.Lock()
        self.cur_depth = None
        self.N = 0
        self.activeTicker = ""
        self.activeTickerIsSpot = True
        self.activeTickerExpire = ""
        self.activeTickerAPI = ""
        self.activeMinAmount = 0.
        self.activeContractSize = 1
        self.passiveTicker = ""
        self.passiveTickerIsSpot = True
        self.passiveTickerExpire = ""
        self.passiveTickerAPI = ""
        self.passiveMinAmount = 0.
        self.passiveContractSize = 1
        self.tradeCount_active = 0
        self.tradeCount_passive = 0
        self.tradeMaxCount = 2
        self.tradeFrequencyCount = 0
        self.tradeFrequency = 60

        self.shortWindowSize = 0
        self.longWindowSize = 0
        self.activeShortMovingWindow = None
        self.activeLongMovingWindow = None
        self.passiveShortMovingWindow = None
        self.passiveLongMovingWindow = None

        self.activeTraded = False
        self.passiveTraded = False

    ########################################
    def initStrategy(self):
        '''
        init strategy here
        user can read configure file in this function
        :return:
        '''
        config = configparser.ConfigParser()
        config.read(self.path2Configure)
        # ticker info
        self.activeTicker = config["Settings"]["activeTicker"]
        self.activeTickerIsSpot = config.getboolean("Settings", "activeTickerIsSpot")
        if not self.activeTickerIsSpot:
            self.activeTickerExpire = config["Settings"]["activeTickerExpire"]
        else:
            self.activeTickerExpire = "None"
        self.activeMinAmount = config.getfloat("Settings", "activeMinAmount")
        self.activeTickerAPI = config["Settings"]["activeTickerAPI"]
        self.activeContractSize = config.getint("Settings","activeContractSize")
        self.passiveTicker = config["Settings"]["passiveTicker"]
        self.passiveTickerIsSpot = config.getboolean("Settings", "passiveTickerIsSpot")
        if not self.passiveTickerIsSpot:
            self.passiveTickerExpire = config["Settings"]["passiveTickerExpire"]
        else:
            self.passiveTickerExpire = "None"
        self.passiveMinAmount = config.getfloat("Settings", "passiveMinAmount")
        self.passiveTickerAPI = config["Settings"]["passiveTickerAPI"]
        self.passiveContractSize = config.getint("Settings", "passiveContractSize")

        # parameters
        self.shortWindowSize = config.getint("Settings", "shortWindowSize")
        self.longWindowSize = config.getint("Settings", "longWindowSize")

        # save ticker into list
        self.workingTickers.append(self.activeTicker)
        self.workingTickers.append(self.passiveTicker)
        self.workingTickerIsSpot.append(self.activeTickerIsSpot)
        self.workingTickerIsSpot.append(self.passiveTickerIsSpot)
        self.workingTickerExpires.append(self.activeTickerExpire)
        self.workingTickerExpires.append(self.passiveTickerExpire)
        self.workingTickerAPIs.append(self.activeTickerAPI)
        self.workingTickerAPIs.append(self.passiveTickerAPI)

        # init moving windows
        self.activeLongMovingWindow = trader.coinUtilities.MovingWindow(self.longWindowSize)
        self.activeShortMovingWindow = trader.coinUtilities.MovingWindow(self.shortWindowSize)
        self.passiveLongMovingWindow = trader.coinUtilities.MovingWindow(self.longWindowSize)
        self.passiveShortMovingWindow = trader.coinUtilities.MovingWindow(self.shortWindowSize)

        # do not forget to return True if read successfully
        return True

    #####################################################
    def onRtnDepth(self, cur_depth):
        """深度行情通知"""
        self.testStrategyThreadLock.acquire()
        cur_depth.printDepthData()
        # check ticker
        isActiveTick = ((cur_depth.symbol == self.activeTicker) and \
                          (cur_depth.expiry == self.activeTickerExpire) and \
                          (cur_depth.isSpot == self.activeTickerIsSpot) and \
                        (cur_depth.API == self.activeTickerAPI))
        isPassiveTick = ((cur_depth.symbol == self.passiveTicker) and \
                          (cur_depth.expiry == self.passiveTickerExpire) and \
                          (cur_depth.isSpot == self.passiveTickerIsSpot) and \
                         (cur_depth.API == self.passiveTickerAPI))
        status = None

        # query infos
        # print("active free: ", self.trader.getFreePosition(self.activeTicker, self.activeTickerExpire, self.activeTickerAPI))
        # print("active freezed: ", self.trader.getFreezedPosition(self.activeTicker, self.activeTickerExpire, self.activeTickerAPI))
        # print("XBt balance: ", self.trader.getCoinBalance("XBt", self.activeTickerAPI))
        # print("eth future free: ", self.trader.getFreePosition("eth", "this_week", self.passiveTickerAPI, direction="long"))
        # print("ltc future freezed: ", self.trader.getFreezedPosition("eth", "this_week", self.passiveTickerAPI, direction="short"))
        # print("ltc balance: ", self.trader.getCoinBalance("eth", self.passiveTickerAPI))

        isPassiveTick = False
        #isActiveTick = False
        # if isActiveTick:
            # cur_depth.printDepthData()

        isBuy = True
        if self.tradeMaxCount <= self.tradeCount_active:
            self.activeTraded = True
        if self.tradeMaxCount <= self.tradeCount_passive:
            self.passiveTraded = True

        if isActiveTick:
            '''
            # check if there is open order, if so, cancel order first
            openOrders = self.activePosition.getOpenOrders()
            hasOpenOrder = False
            for iOrder in openOrders:
                if iOrder.getOrderStatus() < trader.defines.orderFilled:
                    self.trader.cancelOrder(iOrder)
                    hasOpenOrder = True
            if hasOpenOrder:
                self.testStrategyThreadLock.release()
                return
            '''
            # cur_depth.printDepthData()

            # get ticker position
            # long = self.trader.getTickerLongPosition(symbol=self.activeTicker, expiry=self.activeTickerExpire, \
                                                     #strategyTag=self.strategyTag, API=self.activeTickerAPI)
            # print("active long", long)

            if self.tradeFrequencyCount % self.tradeFrequency != 0:
                self.tradeFrequencyCount += 1
                self.testStrategyThreadLock.release()
                return
            self.tradeFrequencyCount += 1
            if not self.activeTraded:
                if isBuy:
                    order = trader.coinTradeStruct.coinOrder()
                    order.iniSpotOrder(symbol=self.activeTicker, price=round(cur_depth.BidPrice * 0.99, 2), amount=round(self.activeMinAmount * 1.00, 2), orderType=trader.defines.openLong_limitPrice, apiType=self.activeTickerAPI, strategyTag=self.strategyTag)
                    # order.iniSpotOrder(symbol=self.activeTicker, price=cur_depth.BidPrice * 0.99, amount=round(self.activeMinAmount * 1.0, 2), orderType=trader.defines.openLong_limitPrice, apiType=self.activeTickerAPI, strategyTag=self.strategyTag)
                    # order.iniFutureOrder(symbol=self.activeTicker, expiry=self.activeTickerExpire, price=round(cur_depth.AskPrice * 1.0, 2), amount=self.activeMinAmount, orderType=trader.defines.openLong_limitPrice, leverage=20, apiType=self.activeTickerAPI, strategyTag=self.strategyTag)

                    # order.iniFutureOrder(symbol=self.activeTicker, expiry=self.activeTickerExpire,
                    #                      price=round(cur_depth.AskPrice * 1.0, 3), amount=self.activeMinAmount,
                    #                      orderType=trader.defines.openShort_limitPrice, leverage=20,
                    #                      apiType=self.activeTickerAPI, strategyTag=self.strategyTag)
                    self.trader.sendOrder(order)
                else:
                    order = trader.coinTradeStruct.coinOrder()
                    order.iniSpotOrder(symbol=self.activeTicker, price=round(cur_depth.BidPrice * 1.00, 2), amount=self.activeMinAmount * 1.0, orderType=trader.defines.coverLong_limitPrice, apiType=self.activeTickerAPI, strategyTag=self.strategyTag)
                    # order.iniFutureOrder(symbol=self.activeTicker, expiry=self.activeTickerExpire,
                    #                      price=cur_depth.AskPrice + 0.5 * 5, amount=self.activeMinAmount,
                    #                      orderType=trader.defines.coverShort_limitPrice, leverage=20,
                    #                      apiType=self.activeTickerAPI, strategyTag=self.strategyTag)
                    # order.iniFutureOrder(symbol=self.activeTicker, expiry=self.activeTickerExpire,
                    #                      price=round(cur_depth.BidPrice * 1.0, 3), amount=self.activeMinAmount,
                    #                      orderType=trader.defines.coverShort_limitPrice, leverage=20,
                    #                      apiType=self.activeTickerAPI, strategyTag=self.strategyTag)
                    self.trader.sendOrder(order)
                self.tradeCount_active += 1

            '''
            # do caclualtion for active ticker
            if cur_depth.BidPrice and cur_depth.AskPrice:
                self.activeShortMovingWindow.update((cur_depth.BidPrice + cur_depth.AskPrice) * 0.5)
                self.activeLongMovingWindow.update((cur_depth.BidPrice + cur_depth.AskPrice) * 0.5)
            elif cur_depth.BidPrice:
                self.activeShortMovingWindow.update(cur_depth.BidPrice)
                self.activeLongMovingWindow.update(cur_depth.BidPrice)
            elif cur_depth.AskPrice:
                self.activeShortMovingWindow.update(cur_depth.AskPrice)
                self.activeLongMovingWindow.update(cur_depth.AskPrice)
            else:
                return

            # check status
            if self.activeLongMovingWindow.full():
                if self.activeShortMovingWindow.averageValue() - self.activeLongMovingWindow.averageValue() > \
                        cur_depth.AskPrice3 - cur_depth.BidPrice:
                    status = 1
                elif self.activeLongMovingWindow.averageValue() - self.activeShortMovingWindow.averageValue() > \
                    cur_depth.AskPrice - cur_depth.BidPrice3:
                    if self.activeTickerIsSpot:
                        status = 0
                    else:
                        status = -1
                else:
                    if np.abs(self.activeShortMovingWindow.averageValue() - self.activeLongMovingWindow.averageValue()) < \
                            cur_depth.AskPrice - cur_depth.BidPrice:
                        status = 0
                    else:
                        status = None
            if not status is None:
                # check if there is open order, if so, cancel order first
                openOrders = self.activePosition.getOpenOrders()
                hasOpenOrder = False
                for iOrder in openOrders:
                    if iOrder.getOrderStatus() < trader.defines.orderFilled:
                        self.trader.cancelOrder(iOrder)
                        hasOpenOrder = True
                if hasOpenOrder:
                    return

                if self.activeTickerIsSpot:
                    share = self.activePosition.getLongShare()
                    if status == 1 and share > 0:
                        return
                    elif status == 1 and share <= 0.0000000000001:
                        # generate order
                        order = trader.coinTradeStruct.coinOrder()
                        order.iniSpotOrder(symbol = self.activeTicker, price = cur_depth.AskPrice, \
                                           amount=self.activeMinAmount, orderType = trader.defines.openLong_limitPrice, \
                                           apiType = self.activeTickerAPI, strategyTag = self.strategyTag)
                        self.trader.sendOrder(order, self.activePosition)
                    else:
                        if share == 0:
                            return
                        else:
                            order = trader.coinTradeStruct.coinOrder()
                            order.iniSpotOrder(symbol=self.activeTicker, price=cur_depth.AskPrice, \
                                               amount=self.activeMinAmount,
                                               orderType=trader.defines.coverLong_limitPrice, \
                                               apiType=self.activeTickerAPI, strategyTag=self.strategyTag)
                            self.trader.sendOrder(order, self.activePosition)
                else:
                    # for future
                    longShare = self.activePosition.getLongShare()
                    shortShare = self.activePosition.getLongShare()
                    if status == 1:
                        if shortShare > 0.00000000001:
                            # clear short first
                            order = trader.coinTradeStruct.coinOrder()
                            order.iniSpotOrder(symbol=self.activeTicker, price=cur_depth.AskPrice, \
                                               amount=shortShare,
                                               orderType=trader.defines.coverShort_limitPrice, \
                                               apiType=self.activeTickerAPI, strategyTag=self.strategyTag)
                            self.trader.sendOrder(order, self.activePosition)

                        if longShare < 0.0000000001:
                            # open long
                            order = trader.coinTradeStruct.coinOrder()
                            order.iniSpotOrder(symbol=self.activeTicker, price=cur_depth.AskPrice, \
                                                amount=self.activeMinAmount,
                                                orderType=trader.defines.openLong_limitPrice, \
                                                apiType=self.activeTickerAPI, strategyTag=self.strategyTag)
                            self.trader.sendOrder(order, self.activePosition)
                        else:
                            return
                    elif status == -1:
                        if longShare > 0.0000000001:
                            # clear long first
                            order = trader.coinTradeStruct.coinOrder()
                            order.iniSpotOrder(symbol=self.activeTicker, price=cur_depth.BidPrice, \
                                               amount=shortShare,
                                               orderType=trader.defines.coverLong_limitPrice, \
                                               apiType=self.activeTickerAPI, strategyTag=self.strategyTag)
                            self.trader.sendOrder(order, self.activePosition)

                        if shortShare < 0.0000000001:
                            # open short
                            order = trader.coinTradeStruct.coinOrder()
                            order.iniSpotOrder(symbol=self.activeTicker, price=cur_depth.BidPrice, \
                                               amount=self.activeMinAmount,
                                               orderType=trader.defines.openShort_limitPrice, \
                                               apiType=self.activeTickerAPI, strategyTag=self.strategyTag)
                            self.trader.sendOrder(order, self.activePosition)
                        else:
                            return

                    else:
                        if longShare > 0.0000000001:
                            # clear long
                            order = trader.coinTradeStruct.coinOrder()
                            order.iniSpotOrder(symbol=self.activeTicker, price=cur_depth.BidPrice, \
                                               amount=shortShare,
                                               orderType=trader.defines.coverLong_limitPrice, \
                                               apiType=self.activeTickerAPI, strategyTag=self.strategyTag)
                            self.trader.sendOrder(order, self.activePosition)
                        if shortShare > 0.00000000001:
                            # clear short
                            order = trader.coinTradeStruct.coinOrder()
                            order.iniSpotOrder(symbol=self.activeTicker, price=cur_depth.AskPrice, \
                                               amount=shortShare,
                                               orderType=trader.defines.coverShort_limitPrice, \
                                               apiType=self.activeTickerAPI, strategyTag=self.strategyTag)
                            self.trader.sendOrder(order, self.activePosition)
            '''

        elif isPassiveTick:
            # do cacluation for passive ticker
            # cur_depth.printDepthData()
            '''
            if not self.passiveTraded:
                if isBuy:
                    order = trader.coinTradeStruct.coinOrder()
                    order.iniSpotOrder(symbol=self.passiveTicker, price=cur_depth.AskPrice, \
                                       amount=self.passiveMinAmount * 1.004,
                                       orderType=trader.defines.openLong_limitPrice, \
                                       apiType=self.passiveTickerAPI, strategyTag=self.strategyTag)
                    self.trader.sendOrder(order, self.passivePosition)
                    self.passiveTraded = True
                else:
                    order = trader.coinTradeStruct.coinOrder()
                    order.iniSpotOrder(symbol=self.passiveTicker, price=cur_depth.BidPrice, \
                                       amount=0.001002, orderType=trader.defines.coverLong_limitPrice, \
                                       apiType=self.passiveTickerAPI, strategyTag=self.strategyTag)
                    self.trader.sendOrder(order, self.passivePosition)
                    self.passiveTraded = True

            self.passivePosition.updatePNLOnTick(cur_depth.LastPrice)
            '''
            # get ticker position
            #long = self.trader.getTickerLongPosition(symbol=self.passiveTicker, expiry=self.passiveTickerExpire, \
                                                     #strategyTag=self.strategyTag, API=self.passiveTickerAPI)
            #short = self.trader.getTickerShortPosition(symbol=self.passiveTicker, expiry=self.passiveTickerExpire, \
                                                     #strategyTag=self.strategyTag, API=self.passiveTickerAPI)
            #print("passive long", long, "passive short", short)


            if not self.passiveTraded:
                if isBuy:
                    order = trader.coinTradeStruct.coinOrder()
                    '''
                    order.iniFutureOrder(symbol=self.passiveTicker, expiry=self.passiveTickerExpire, \
                                         price=cur_depth.AskPrice, amount=self.passiveMinAmount, \
                                        orderType=trader.defines.openLong_limitPrice, leverage = self.passiveContractSize, \
                                       apiType=self.passiveTickerAPI, strategyTag=self.strategyTag)
                    '''
                    order.iniFutureOrder(symbol=self.passiveTicker, expiry=self.passiveTickerExpire, \
                                         price=round(cur_depth.BidPrice * 0.99, 2), amount=self.passiveMinAmount, \
                                         orderType=trader.defines.openLong_limitPrice,
                                         leverage=self.passiveContractSize, \
                                         apiType=self.passiveTickerAPI, strategyTag=self.strategyTag)
                    self.trader.sendOrder(order, isSecondaryAPI = False)
                else:
                    order = trader.coinTradeStruct.coinOrder()
                    order.iniFutureOrder(symbol=self.passiveTicker, expiry=self.passiveTickerExpire, \
                                         price=cur_depth.BidPrice, amount=self.passiveMinAmount * 0.999, \
                                         orderType=trader.defines.coverLong_limitPrice, leverage=self.passiveContractSize, \
                                         apiType=self.passiveTickerAPI, strategyTag=self.strategyTag)
                    self.trader.sendOrder(order, isSecondaryAPI = False)

                self.tradeCount_passive += 1


        else:
            # do nothing
            pass

        self.testStrategyThreadLock.release()

    def onRtnTrade(self, cur_trade):
        """成交行情通知"""
        self.testStrategyThreadLock.acquire()
        ####################################
        # write process code inside of here
        # for all latest market order fill info
        ####################################
        #if cur_trade.API == self.activeTickerAPI:
           #print(cur_trade.printMarketOrderInfo())
        self.testStrategyThreadLock.release()

    def onOrderFeedback(self, orderBack):
        self.testStrategyThreadLock.acquire()
        # process orderback here
        print(orderBack.printOrderBackInfo())
        self.testStrategyThreadLock.release()

    def onTradeFeedback(self, tradeBack):
        self.testStrategyThreadLock.acquire()
        ####################################
        # write process code inside of here
        ####################################
        # print("strategy trade back", tradeBack.printTradeBackInfo())
        self.testStrategyThreadLock.release()

    def onSendOrderFailed(self, order):
        '''
        strategy need to handle the handshake problem here
        DO NOT AND LOCK IN THIS METHOD !!!
        :param order:
        :return:
        '''
        pass

    def exitStrategy(self):
        '''
        exit, save infos to file
        :return:
        '''

        # cancel all open orders
        activeOpenOrders = self.trader.getTickerOpenOrders(self.activeTicker, self.activeTickerExpire, \
                                                      self.getStrategyTag(), self.activeTickerAPI)
        for iOrder in activeOpenOrders:
            if iOrder.getOrderStatus() < trader.defines.orderFilled:
                self.trader.cancelOrder(iOrder)
        passiveOpenOrders = self.trader.getTickerOpenOrders(self.passiveTicker, self.passiveTickerExpire, \
                                                      self.getStrategyTag(), self.passiveTickerAPI)
        for iOrder in passiveOpenOrders:
            if iOrder.getOrderStatus() < trader.defines.orderFilled:
                self.trader.cancelOrder(iOrder)
        return True

