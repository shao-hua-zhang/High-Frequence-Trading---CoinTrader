import sys
from abc import ABCMeta, abstractmethod
sys.path.append("../")
import configparser
import json
import threading
import trader.coinUtilities
import trader.coinTradeStruct
from trader.positionManager import openPosition, to_market, get_precision, get_volume_base, get_price_miniMove, get_volume_miniMove
import trader.defines
import numpy as np
import os, time, re, math


# base
from collections import defaultdict

class SimpleStrategy(metaclass=ABCMeta):
    # 为了方便调用对baseStrategy的二次封装
    # 1. 策略独自维护自己的balance, risk, positionManager
    # 2. 增加logging模块，方便log
    def __init__(self, path2Configure, cache):
        self.path2Configure = path2Configure
        # each strategy should have a unique strategy tag
        self.strategyTag = ""
        # trader to trade interface
        self.trader = None
        # need to fill up these info in initStrategy method
        self._workingTickers = []
        self._workingTickerIsSpot = []
        self._workingTickerExpires = []
        self._workingTickerAPIs = []

        self.locker = threading.Lock()
        self._balance = {}
        self._orderDetails = {}
        self._info = {}

        self.setState(cache)
        return False

    # auto cache
    def setState(self, session):
        balance = session.get("balance", {})
        for market, coins in balance.items():
            for coin, value in coins.items():
                self._balance[(market, coin)] = value

    def getState(self):
        session = {}

        balance = {}
        for (market, coin), value in self._balance.items():
            if market not in balance: balance[market] = {}
            balance[market][coin] = value
        session["balance"] = balance
        return session

    def getPositionManager(self, symbol, expire, api):
        return self.trader.getPositionManager(symbol, expire, self.getStrategyTag(), api)

    #####################################################
    # methods need to be overwritten by user
    ######################################################
    @abstractmethod
    def initStrategy(self):
        pass

    @abstractmethod
    def on_depth(self, cur_depth):
        pass

    @abstractmethod
    def on_trade(self, cur_trade):
        pass

    @abstractmethod
    def on_rtn_order(self, order):
        pass

    @abstractmethod
    def on_rtn_trade(self, order):
        pass

    @abstractmethod
    def exitStrategy(self):
        pass

    @abstractmethod
    def onSendOrderFailed(self, order):
        '''
        strategy need to handle the handshake problem here
        DO NOT AND LOCK IN THIS METHOD !!!
        :param order:
        :return:
        '''
        pass

    ################################################################################
    ################################### useful functions ###########################
    ################################################################################
    def _gen_info(self, symbol, expire, api):
        _info = {}
        _info["volume_miniMove"] = get_volume_miniMove(symbol, expire, api)
        _info["price_miniMove"] = get_price_miniMove(symbol, expire, api)
        _info["volume_precision"] = max(0, get_precision(get_volume_miniMove(symbol, expire, api)))
        _info["price_precision"] = max(0, get_precision(get_price_miniMove(symbol, expire, api)))
        _info["volume_base"] = get_volume_base(symbol, expire, api)
        self._info[(symbol, expire, api)] = _info

    def round_price(self, symbol, expire, api, price):
        if (symbol, expire, api) not in self._info:
            self._gen_info(symbol, expire, api)

        precision = self._info[(symbol, expire, api)]["price_precision"]
        return round(price, precision)
    def round_volume(self, symbol, expire, api, volume):
        if (symbol, expire, api) not in self._info:
            self._gen_info(symbol, expire, api)
        precision = self._info[(symbol, expire, api)]["volume_precision"]
        baseAmt = self._info[(symbol, expire, api)]["volume_base"]

        # use floor not round
        # volume = round(volume, precision)
        volume = math.floor(volume * 10**precision) / (10**precision)
        if abs(volume ) < baseAmt:
            return 0
        else:
            return volume

    def get_volume_base(self, symbol, expire, api):
        if (symbol, expire, api) not in self._info:
            self._gen_info(symbol, expire, api)
        return self._info[(symbol, expire, api)]["volume_base"]
    def get_price_miniMove(self, symbol, expire, api):
        if (symbol, expire, api) not in self._info:
            self._gen_info(symbol, expire, api)
        return self._info[(symbol, expire, api)]["price_miniMove"]
    def get_volume_miniMove(self, symbol, expire, api):
        if (symbol, expire, api) not in self._info:
            self._gen_info(symbol, expire, api)
        return self._info[(symbol, expire, api)]["volume_miniMove"]

    def log_info(self, msg):
        print("[INFO] {} {} - {}".format(trader.coinUtilities.getCurrentTimeString(), self.getStrategyTag(), msg), file=sys.stdout)
    def log_error(self, msg):
        print("[ERR] {} {} - {}".format(trader.coinUtilities.getCurrentTimeString(), self.getStrategyTag(), msg), file=sys.stderr)
        sys.stderr.flush()
    def log_warn(self, msg):
        print("[WARN] {} {} - {}".format(trader.coinUtilities.getCurrentTimeString(), self.getStrategyTag(), msg), file=sys.stdout)

    def get_balance(self, coin, expire, api):
        '''
        :param symbol: usdt, btc, eth, eth@future, btc@future
        '''
        market = to_market(api)
        if expire != "None":
            coin = coin + "@future"
        return self._balance.get((market, coin), 0)

    def get_freezed_balance(self, coin, expire, api):
        '''
        只有限价单会占用资金，而且期货的限价单会以杠杆占用保证金，现货只会在filling状态时会占用balance
        '''
        total_balance = 0
        for symbol, expire_, api_ in zip(self._workingTickers, self._workingTickerExpires, self._workingTickerAPIs):
            if (expire_ != expire) or (api_ != api): continue
            if expire == "None":
                coinA, coinB = symbol.split("_")
                if coin == coinB:
                    position = self.getPositionManager(symbol, expire, api)
                    for order in position.getOpenOrders():
                        otype = self._orderDetails[(order.orderID, order.API)]["otype"]
                        if order.getOrderStatus() < trader.defines.orderFilled and otype.startswith("open"):
                            amount_occupy = order.amount - order.getFilledAmount()
                            if order.price != 0:
                                total_balance += amount_occupy * order.price
                elif coin == coinA:
                    position = self.getPositionManager(symbol, expire, api)
                    for order in position.getOpenOrders():
                        otype = self._orderDetails[(order.orderID, order.API)]["otype"]
                        if order.getOrderStatus() < trader.defines.orderFilled and otype.startswith("cover"):
                            amount_occupy = order.amount - order.getFilledAmount()
                            if order.price != 0:
                                total_balance += amount_occupy
            else:
                if coin == symbol:
                    position = self.getPositionManager(symbol, expire, api)
                    total_balance = ((position.getLongShare() + position.getShortShare()) * position.futureFaceValue /
                                     (position.getLastPrice() * position.contractSize))
                    for order in position.getOpenOrders():
                        if order.getOrderStatus() < trader.defines.orderFilled:
                            amount_occupy = order.amount
                            if order.price != 0:
                                total_balance += (amount_occupy * position.futureFaceValue) / (order.price * order.leverage)

        return total_balance

    def get_free_balance(self, coin, expire, api):
        return self.get_balance(coin, expire, api) - self.get_freezed_balance(coin, expire, api)

    def get_total_fees(self):
        total_fee = 0
        for symbol, expire, api in zip(self._workingTickers, self._workingTickerExpires, self._workingTickerAPIs):
            position = self.getPositionManager(symbol, expire, api)
            if expire == "None":
                total_fee += position.getFees() * position.baseCashUnit
            else:
                total_fee += position.getFees() * position.baseCashUnit * position.getLastPrice()
        return total_fee

    def get_total_gross(self):
        total_gross = 0
        for symbol, expire, api in zip(self._workingTickers, self._workingTickerExpires, self._workingTickerAPIs):
            position = self.getPositionManager(symbol, expire, api)
            if expire == "None":
                total_gross += position.getGrossPNL() * position.baseCashUnit
            else:
                total_gross += position.getGrossPNL() * position.baseCashUnit * position.getLastPrice()
        return total_gross

    def get_total_pnl(self):
        " return pnl in RMB value"
        total_pnl = 0
        for symbol, expire, api in zip(self._workingTickers, self._workingTickerExpires, self._workingTickerAPIs):
            position = self.getPositionManager(symbol, expire, api)
            if expire == "None":
                total_pnl += (position.getGrossPNL() - position.getFees()) * position.baseCashUnit
            else:
                total_pnl += (position.getGrossPNL() - position.getFees()) * position.baseCashUnit * position.getLastPrice()

        return total_pnl

    def get_total_value(self):
        # conver all coin to RMB value? which consider all price change in self._balance
        # difficult to implemented inside strategy
        pass

    def get_balance_pnl(self):
        # 出了策略持仓的pnl，这还会计算balance中的所有货币随着价格的波动
        return self.get_total_value() - self.initial_value

    def getOpenOrders(self, tag=None):
        '''
        :param tag: tag should is callable or string(support regex) or None (match all tag) or list of any above format
        :return: orders satisfied tag
        '''
        if not isinstance(tag, list):
            tag = [tag]

        out = []
        for tagpattern in tag:
            if not (callable(tagpattern) or (tagpattern is None) or isinstance(tagpattern, str)):
                self.log_error("tagpattern format not recongnized: {}".format(tagpattern))
                return out

            for symbol, expire, api in zip(self._workingTickers, self._workingTickerExpires, self._workingTickerAPIs):
                position = self.getPositionManager(symbol, expire, api)
                for order in position.getOpenOrders():
                    if (tagpattern is None) or \
                    (isinstance(tagpattern, str) and re.match(tagpattern, order.tag)) or \
                    (callable(tagpattern) and tagpattern(order.tag)):
                        out.append(order)
        return out

    def open(self, symbol, expire, api, volume, direction=None, price=0, leverage=None, tag="" , isMaker = False):
        '''
        simplified version of sendOrder
        '''
        order = trader.coinTradeStruct.coinOrder()
        if expire == "None":
            if direction is not None:
                self.log_error("only for future direction")
                return order
            else:
                otype = "openLong"
        else:
            if direction is None:
                self.log_error("for future, direction must be assigned")
                return order
            else:
                if not direction in {"long", "short"}:
                    self.log_error("direction must be one of long or short")
                    return order
                else:
                    otype = "open" + direction.capitalize()

        price = self.round_price(symbol, expire, api, price)
        if price == 0:
            mtype = "market"
        else:
            mtype = "limit"
        if isMaker:
            mtype = "maker"

        # print("otype, mtype", otype, mtype)

        if (expire == "None") and (leverage is not None):
            self.log_warn("only for future leverage has real meaning")

        table = {
            ("openLong", "limit"): trader.defines.openLong_limitPrice,
            ("openShort", "limit"): trader.defines.openShort_limitPrice,
            ("openLong", "market"): trader.defines.openLong_marketPrice,
            ("openShort", "market"): trader.defines.openShort_marketPrice,
            ("openLong", "maker"): trader.defines.openLong_buy_limit_maker
        }

        if expire == "None":
            order.iniSpotOrder(
                symbol = symbol, price = price, amount = volume, apiType = api,
                orderType = table[(otype, mtype)], strategyTag = self.getStrategyTag())
        else:
            order.iniFutureOrder(
                symbol = symbol, expiry = expire, apiType = api, price = price, amount = volume,
                orderType = table[(otype, mtype)], leverage = leverage, strategyTag = self.getStrategyTag())

        setattr(order, "tag", tag)
        # print("sending order")
        # self.trader.sendOrder(order)
        # print("order sent",order.orderID)
        # print(isinstance((order.orderID, str)))
        if not self.trader.sendOrder(order):
            # debug
            # self.log_info("Successfully send order: #{} {}/{} >< {} @ {} >< {} {} {}".format(
                # order.orderID, order.symbol, order.expiry, order.price, order.amount, otype, mtype, api))
            self._orderDetails[(order.orderID, api)] = {"last_filledAmount": 0, "canceled": 0, "lastCanceled": time.time(),
                                                 "otype": otype, "mtype": mtype, "order": order}
        else:
            self.log_error("failed to send order: #{} {}/{} >< {} @ {} >< {} {} {}".format(
                order.orderID, order.symbol, order.expiry, order.price, order.amount, otype, mtype, api))
            return
        # print("I am returning")
        # print(order)
        return order


    def cover(self, symbol, expire, api, volume, direction=None, price=0, leverage=None, tag="", isMaker = False):
        '''
        simplified version of sendOrder
        '''
        order = trader.coinTradeStruct.coinOrder()

        if expire == "None":
            if direction is not None:
                self.log_error("only for future direction")
                return order
            else:
                otype = "coverLong"
        else:
            if direction is None:
                self.log_error("for future, direction must be assigned")
                return order
            else:
                if not direction in {"long", "short"}:
                    self.log_error("direction must be one of long or short")
                    return order
                else:
                    otype = "cover" + direction.capitalize()

        price = self.round_price(symbol, expire, api, price)
        if price == 0:
            mtype = "market"
        else:
            mtype = "limit"
        if isMaker:
            mtype = "maker"

        if (expire == "None") and (leverage is not None):
            self.log_warn("only for future leverage has real meaning")

        table = {
            ("coverLong", "limit"): trader.defines.coverLong_limitPrice,
            ("coverShort", "limit"): trader.defines.coverShort_limitPrice,
            ("coverLong", "market"): trader.defines.coverLong_marketPrice,
            ("coverShort", "market"): trader.defines.coverShort_marketPrice,
            ("coverLong", "maker"): trader.defines.coverLong_sell_limit_maker,
        }

        if expire == "None":
            order.iniSpotOrder(
                symbol = symbol, price = price, amount = volume, apiType = api,
                orderType = table[(otype, mtype)], strategyTag = self.strategyTag)
        else:
            order.iniFutureOrder(
                symbol = symbol, expiry = expire, apiType = api, price = price, amount = volume,
                orderType = table[(otype, mtype)], leverage = leverage, strategyTag = self.strategyTag)

        setattr(order, "tag", tag)
        # self.trader.sendOrder(order)
        # print(isinstance((order.orderID, str)))
        if not self.trader.sendOrder(order):
            # debug
            # self.log_info("Successfully send order: #{} {}/{} >< {} @ {} >< {} {} {}".format(
            #     order.orderID, order.symbol, order.expiry, order.price, order.amount, otype, mtype, api))
            self._orderDetails[(order.orderID, api)] = {"last_filledAmount": 0, "canceled": 0, "lastCanceled": time.time(),
                                                 "otype": otype, "mtype": mtype, "order": order}
            return order
        else:
            self.log_error("failed to send order: #{} {}/{} >< {} @ {} >< {} {} {}".format(
                order.orderID, order.symbol, order.expiry, order.price, order.amount, otype, mtype, api))
            return

    ################################################
    # methods defined in base strategy, user do NOT need to rewrite
    #################################################
    def setConfigureFile(self, path2Configure):
        self.path2Configure = path2Configure

    def setStrategyTag(self, tag):
        self.strategyTag = tag

    def getStrategyTag(self):
        return self.strategyTag

    def getWorkingTickers(self):
        return self._workingTickers, self._workingTickerIsSpot, self._workingTickerExpires, self._workingTickerAPIs

    def getAPIs(self):
        return set(self._workingTickerAPIs)

    def setTrader(self, trader):
        self.trader = trader

    def onRtnDepth(self, cur_depth):
        self.locker.acquire()
        self.on_depth(cur_depth)
        self.locker.release()

    def onRtnTrade(self, cur_trade):
        self.locker.acquire()
        self.on_trade(cur_trade)
        self.locker.release()

    def onOrderFeedback(self, orderBack):
        # print("orderBack", orderBack.printOrderBackInfo())
        self.locker.acquire()
        if (orderBack.orderID, orderBack.API) in self._orderDetails:
            position = self.getPositionManager(orderBack.symbol, orderBack.expiry, orderBack.API)
            orderDetail = self._orderDetails[(orderBack.orderID, orderBack.API)]
            if orderBack.filledAmount > orderDetail["last_filledAmount"]:
                if orderBack.expiry == "None":
                    coinA, coinB = orderBack.symbol.split("_")

                    if (to_market(orderBack.API), coinA) not in self._balance:
                        self._balance[(to_market(orderBack.API), coinA)] = 0
                    if (to_market(orderBack.API), coinB) not in self._balance:
                        self._balance[(to_market(orderBack.API), coinB)] = 0

                    last_filledAmount = orderDetail["last_filledAmount"]
                    if  orderDetail["otype"].startswith("open"):
                        feeRate = position.openPositionFeeRate if position.subTractFee else 0
                        self._balance[(to_market(orderBack.API), coinA)] += (orderBack.filledAmount - last_filledAmount) * (1 - feeRate)
                        self._balance[(to_market(orderBack.API), coinB)] -= (orderBack.filledAmount - last_filledAmount) * orderBack.fillPrice
                    else:
                        feeRate = position.coverPositionFeeRate if position.subTractFee else 0
                        self._balance[(to_market(orderBack.API), coinA)] -= (orderBack.filledAmount - last_filledAmount)
                        self._balance[(to_market(orderBack.API), coinB)] += (orderBack.filledAmount - last_filledAmount) * orderBack.fillPrice * (1 - feeRate)
                else:
                    coin = orderBack.symbol + "@future"
                    last_filledAmount = orderDetail["last_filledAmount"]
                    if (to_market(orderBack.API), coin) not in self._balance:
                        self._balance[(to_market(orderBack.API), coin)] = 0

                    if  orderDetail["otype"].startswith("open"):
                        feeRate = position.openPositionFeeRate
                    else:
                        feeRate = position.coverPositionFeeRate
                    self._balance[(to_market(orderBack.API), coin)] -= (
                        (orderBack.filledAmount - last_filledAmount) * position.futureFaceValue / orderBack.fillPrice) * feeRate

                self._orderDetails[(orderBack.orderID, orderBack.API)]["last_filledAmount"] = orderBack.filledAmount

            self.on_rtn_order(self._orderDetails[(orderBack.orderID, orderBack.API)]["order"])
            if orderBack.getOrderStatus() >= trader.defines.orderFilled:
                del self._orderDetails[(orderBack.orderID, orderBack.API)]
        else:
            self.log_error("get order back #{} do not belong to this strategy {}: {}".format(
                self.strategyTag, orderBack.orderID, orderBack.printOrderBackInfo()))
        self.locker.release()

    def onTradeFeedback(self, tradeBack):
        self.locker.acquire()
        self.on_rtn_trade(tradeBack)
        self.locker.release()

    def subscribe(self, symbol, expire, api):
        isSpot = expire is "None"
        self._workingTickers.append(symbol)
        self._workingTickerIsSpot.append(isSpot)
        self._workingTickerExpires.append(expire)
        self._workingTickerAPIs.append(api)

        market = to_market(api)
        if expire != "None":
            coin = symbol + "@future"
            if (market, coin) not in self._balance:
                self._balance[(market, coin)] = 0
        else:
            coinA, coinB = symbol.split("_")
            if (market, coinA) not in self._balance:
                self._balance[(market, coinA)] = 0
            if (market, coinB) not in self._balance:
                self._balance[(market, coinB)] = 0

    # temporary patch
    def report(self):
        pass
