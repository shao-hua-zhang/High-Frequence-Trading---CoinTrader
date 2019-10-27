# encoding: UTF-8
import threading
import sys

sys.path.append("../")
import trader.coinTradeStruct
import trader.coinUtilities
import trader.defines
import trader.locker
import math
import numbers
import pandas

info_csv_file = "../config/info.csv"

class openPosition(object):
    '''
    open position for a ticker
    '''
    def __init__(self, strategyTag):
        self.strategyTag = strategyTag
        self.symbol = ""
        self.expire = "None"
        self.isSpot = True
        self.API = ""
        self.longShare = 0.0
        self.shortShare = 0.0
        self.positionThreadLock = trader.locker.RWLock()
        self.PNLThreadLock = trader.locker.RWLock()
        self.openPositionMapLock = trader.locker.RWLock()
        self.orderID_OrderMap = {}
        self.AllOrder_ID_OrderMap = {} # this is used for backup, in case order is removed before received order/trade back
        self.baseCashUnit = 1.0
        self.grossPNL = 0.0
        self.Fee = 0.0
        self.openPositionFeeRate = 0.002
        self.coverPositionFeeRate = 0.0
        self.contractSize = 1 # margin
        self.lastPrice = 0.0
        self.secondaryAPIOrder = None
        self.futureFaceValue = 10.0

        self.volumeBase = 1
        self.priceMiniMove = 0.001
        self.volumeMiniMove = 0.001

        self.subTractFee = False

    # check is this ticker or not
    def isMyTradeTicker(self, symbol, expire, API):
        return (symbol == self.symbol) and \
               (expire == self.expire) and (API == self.API)

    # copy init
    def copyPositionInfo(self, position):
        self.symbol = position.symbol
        self.expire = position.expire
        self.isSpot = position.isSpot
        self.API = position.API
        self.longShare = position.getLongShare()
        self.shortShare = position.getShortShare()
        self.grossPNL = position.getGrossPNL()
        self.Fee = position.getFees()
        self.baseCashUnit = position.baseCashUnit
        self.contractSize = position.contractSize
        self.futureFaceValue = position.futureFaceValue
        self.subTractFee = position.subTractFee
        self.volumeBase = position.volumeBase
        self.volumeMiniMove = position.volumeMiniMove
        self.priceMiniMove = position.priceMiniMove

    # init spot position
    def initSpotPosition(self, symbol, API, share, subTractFee):
        if not isinstance(share, numbers.Number):
            print("[ERR]:", trader.coinUtilities.getCurrentTimeString(),
                  " error input dataType for share in function initSpotPosition", file=sys.stderr)
            return -1
        self.symbol = symbol
        self.API = API
        self.longShare = share
        self.subTractFee = subTractFee
        self.volumeBase = get_volume_base(symbol, "None", API)
        self.priceMiniMove = get_price_miniMove(symbol, "None", API)
        self.volumeMiniMove = get_volume_miniMove(symbol, "None", API)
        return 0
    # init future position
    def initFuturePosition(self, symbol, expire, API, longShare, shortShare):
        if not isinstance(longShare, numbers.Number):
            print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                  " error input dataType for longShare in function initFuturePosition", file=sys.stderr)
            return -1
        if not isinstance(shortShare, numbers.Number):
            print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                  " error input dataType for shortShare in function initFuturePosition", file=sys.stderr)
            return -1
        self.symbol = symbol
        self.expire = expire
        self.API = API
        self.longShare = longShare
        self.shortShare = shortShare
        self.isSpot = False
        if API == trader.defines.okexAPI_web_future:
            self.futureFaceValue = 100.0 if symbol == "btc" else 10.0
        elif API == trader.defines.HuobiSpi_future_web:
            self.futureFaceValue = 100.0 if symbol == "BTC" else 10.0
        elif API == trader.defines.BitmexSpi_future_web:
            self.futureFaceValue = 1.0 if symbol == "BTC" else 1.0
        else:
            print("[ERR]: future API should be okex or huobi future API!")
        self.subTractFee = False
        self.volumeBase = get_volume_base(symbol, expire, API)
        self.priceMiniMove = get_price_miniMove(symbol, expire, API)
        self.volumeMiniMove = get_volume_miniMove(symbol, expire, API)

        return 0
    # update position
    def setSpotPosition(self, symbol, share):
        if symbol == self.symbol:
            self.positionThreadLock.write_acquire()
            self.longShare = share
            self.positionThreadLock.write_release()
        else:
            print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                  " symbol mismatch for setSpotPosition, local: ", \
                  self.symbol, " update: ", symbol, file=sys.stderr)
    def setFuturePosition(self, symbol, expire, longShare, shortShare):
        if symbol == self.symbol and expire == self.expire:
            self.positionThreadLock.write_acquire()
            self.longShare = longShare
            self.shortShare = shortShare
            self.positionThreadLock.write_release()
        else:
            print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                  " symbol mismatch for setSpotPosition, local: ", \
                  self.symbol, self.expire, " update: ", symbol, expire, file=sys.stderr)

    def setContractSize(self, contractSize):
        if not isinstance(contractSize, numbers.Number):
            print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                  " error input dataType for contractSize in function setContractSize", file=sys.stderr)
            return -1
        self.contractSize = contractSize
        return 0

    def setGrossAndFee(self, gross, fee, lastPrice):
        if not isinstance(gross, numbers.Number):
            print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                  " error input dataType for gross in function setGrossAndFee", file=sys.stderr)
            return -1
        if not isinstance(fee, numbers.Number):
            print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                  " error input dataType for fee in function setGrossAndFee", file=sys.stderr)
            return -1
        if not isinstance(lastPrice, numbers.Number):
            print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                  " error input dataType for lastPrice in function setGrossAndFee", file=sys.stderr)
            return -1
        self.grossPNL = gross
        self.Fee = fee
        self.lastPrice = lastPrice
        return 0
    # set PNL
    def setPNLInfo(self, gross, fee, contractSize):
        if not isinstance(gross, numbers.Number):
            print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                  " error input dataType for gross in function setPNLInfo", file=sys.stderr)
            return -1
        if not isinstance(fee, numbers.Number):
            print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                  " error input dataType for fee in function setPNLInfo", file=sys.stderr)
            return -1
        if not isinstance(contractSize, numbers.Number):
            print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                  " error input dataType for contractSize in function setPNLInfo", file=sys.stderr)
            return -1
        self.grossPNL = gross
        self.Fee = fee
        self.contractSize = contractSize
        return 0
    def setFeeRate(self, openRate, coverRate):
        if not isinstance(openRate, numbers.Number):
            print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                  " error input dataType for openRate in function setFeeRate", file=sys.stderr)
            return -1
        if not isinstance(coverRate, numbers.Number):
            print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                  " error input dataType for coverRate in function setFeeRate", file=sys.stderr)
            return -1
        self.openPositionFeeRate = openRate
        self.coverPositionFeeRate = coverRate
        return 0
    def getGrossPNL(self):
        self.PNLThreadLock.read_acquire()
        gross = self.grossPNL
        self.PNLThreadLock.read_release()
        return gross
    def getFees(self):
        self.PNLThreadLock.read_acquire()
        fee = self.Fee
        self.PNLThreadLock.read_release()
        return fee
    def getLastPrice(self):
        self.PNLThreadLock.read_acquire()
        lastPrice = self.lastPrice
        self.PNLThreadLock.read_release()
        return lastPrice

    # update PNL
    def updatePNLOnTick(self, newPrice):
        netShare = self.getNetShare()
        if netShare == 0:
            return
        if not newPrice is None and newPrice > 0.0:
            if self.isSpot or self.expire == trader.defines.bitmexFuture:
                if self.lastPrice > 0.01:
                    self.PNLThreadLock.write_acquire()
                    self.grossPNL += ((newPrice - self.lastPrice) * netShare)
                    self.PNLThreadLock.write_release()
            else:
                if self.lastPrice > 0.01:
                    self.PNLThreadLock.write_acquire()
                    self.grossPNL += ((1.0 / self.lastPrice - 1.0 / newPrice) * netShare * self.futureFaceValue)
                    self.PNLThreadLock.write_release()
            self.lastPrice = newPrice

    def updatePNLOnOrderBack(self, orderBack, order):
        if not orderBack.isOrderBackOnly:
            return
        if (order.orderType == trader.defines.openLong_limitPrice) or \
                (order.orderType == trader.defines.openShort_marketPrice) or \
                (order.orderType == trader.defines.openLong_marketPrice) or \
                (order.orderType == trader.defines.openShort_limitPrice) :
            # open new position
            self.PNLThreadLock.write_acquire()
            self.Fee += (math.fabs(orderBack.filledAmount - order.filledAmount) * \
                         orderBack.fillPrice * self.openPositionFeeRate)
            self.PNLThreadLock.write_release()
            self.updatePNLOnTick(orderBack.fillPrice)

        else:
            # close existing position
            if self.coverPositionFeeRate > 0.0:
                self.PNLThreadLock.write_acquire()
                self.Fee += (math.fabs(orderBack.filledAmount - order.filledAmount) * \
                             orderBack.fillPrice * self.coverPositionFeeRate)
                self.PNLThreadLock.write_release()
            self.updatePNLOnTick(orderBack.fillPrice)

        self.lastPrice = orderBack.fillPrice

    def updatePNLOnTradeBack(self, tradeBack, order):
        if (order.orderType == trader.defines.openLong_limitPrice) or \
                (order.orderType == trader.defines.openShort_marketPrice) or \
                (order.orderType == trader.defines.openLong_marketPrice) or \
                (order.orderType == trader.defines.openShort_limitPrice) :
            # open new position
            self.PNLThreadLock.write_acquire()
            if not tradeBack.tradeFee is None:
                # use tradeFee directly
                self.Fee += (math.fabs(tradeBack.tradeFee) * \
                             tradeBack.tradePrice)
            else:
                self.Fee += (math.fabs(tradeBack.tradeAmount) * \
                             tradeBack.tradePrice * self.openPositionFeeRate)
            self.PNLThreadLock.write_release()
            self.updatePNLOnTick(tradeBack.tradePrice)

        else:
            # close existing position
            self.PNLThreadLock.write_acquire()
            if not tradeBack.tradeFee is None:
                # use tradeFee directly
                self.Fee += (math.fabs(tradeBack.tradeFee) * \
                             tradeBack.tradePrice)
            else:
                self.Fee += (math.fabs(tradeBack.tradeAmount) * \
                             tradeBack.tradePrice * \
                             self.coverPositionFeeRate)
            self.PNLThreadLock.write_release()
            self.updatePNLOnTick(tradeBack.tradePrice)
        self.lastPrice = tradeBack.tradePrice

    # add open order
    def addOpenOrder(self, order):
        # check orderID is string or not
        if isinstance(order.orderID, int):
            if order.orderID < 0:
                if order.orderID == -19860922:
                    # order send by secondary API
                    if self.secondaryAPIOrder:
                        # have unprocessed secondary order
                        print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                              "order send out while having unprocessed secondary API order", file=sys.stderr)
                    else:
                        self.openPositionMapLock.write_acquire()
                        self.secondaryAPIOrder = order
                        self.openPositionMapLock.write_release()
                else:
                    print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                      " orderID is not set before adding open order to system", file=sys.stderr)
            else:
                self.openPositionMapLock.write_acquire()
                self.orderID_OrderMap[order.orderID] = order
                self.AllOrder_ID_OrderMap[order.orderID] = order
                self.openPositionMapLock.write_release()
        elif isinstance(order.orderID, str):
            if order.orderID == "secondaryOrder":
                # order send by secondary API
                if self.secondaryAPIOrder:
                    # have unprocessed secondary order
                    print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                          "order send out while having unprocessed secondary API order", file=sys.stderr)
                else:
                    self.openPositionMapLock.write_acquire()
                    self.secondaryAPIOrder = order
                    self.openPositionMapLock.write_release()
            else:
                self.openPositionMapLock.write_acquire()
                self.orderID_OrderMap[order.orderID] = order
                self.AllOrder_ID_OrderMap[order.orderID] = order
                self.openPositionMapLock.write_release()
        else:
            print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                  "orderID type is not defined", file=sys.stderr)

    # update order status from order feedback
    def updateOrderStatusOnOrderBack(self, orderBack):
        # find the order from map
        self.openPositionMapLock.read_acquire()
        if orderBack.orderID in self.orderID_OrderMap:
            openOrder = self.orderID_OrderMap[orderBack.orderID]
            self.openPositionMapLock.read_release()
            if orderBack.isOrderBackOnly:
                if openOrder.filledAmount < orderBack.filledAmount:
                    # update pnl before update position
                    self.updatePNLOnOrderBack(orderBack, openOrder)
                    # need to update position here
                    # need to be called before update order filled amount
                    self.updatePositionOnOrderBack(orderBack.filledAmount - openOrder.filledAmount, openOrder.orderType)
                    openOrder.filledAmount = orderBack.filledAmount
                    openOrder.fillPrice = orderBack.fillPrice
                if openOrder.orderStatus < orderBack.orderStatus:
                    openOrder.orderStatus = orderBack.orderStatus
                    # check if order is finished
                    if openOrder.orderStatus >= trader.defines.orderFilled:
                        self.openPositionMapLock.write_acquire()
                        del self.orderID_OrderMap[orderBack.orderID]
                        self.openPositionMapLock.write_release()
            else:
                if openOrder.orderStatus < orderBack.orderStatus:
                    openOrder.orderStatus = orderBack.orderStatus
                    # check if order is finished
                    if openOrder.orderStatus >= trader.defines.orderFilled:
                        self.openPositionMapLock.write_acquire()
                        del self.orderID_OrderMap[orderBack.orderID]
                        self.openPositionMapLock.write_release()

        else:
            # try to find the original order from backup map
            if orderBack.orderID in self.AllOrder_ID_OrderMap:
                openOrder = self.AllOrder_ID_OrderMap[orderBack.orderID]
                self.openPositionMapLock.read_release()
                if orderBack.isOrderBackOnly:
                    if openOrder.filledAmount < orderBack.filledAmount:
                        # update pnl before update position
                        self.updatePNLOnOrderBack(orderBack, openOrder)
                        # need to update position here
                        # need to be called before update order filled amount
                        self.updatePositionOnOrderBack(orderBack.filledAmount - openOrder.filledAmount, openOrder.orderType)
                        openOrder.filledAmount = orderBack.filledAmount
                        openOrder.fillPrice = orderBack.fillPrice
                    if openOrder.orderStatus < orderBack.orderStatus:
                        openOrder.orderStatus = orderBack.orderStatus
                else:
                    if openOrder.orderStatus < orderBack.orderStatus:
                        openOrder.orderStatus = orderBack.orderStatus
            else:
                self.openPositionMapLock.read_release()
                # check if there is order sent by secondary API
                if self.secondaryAPIOrder:
                    # process the order
                    openOrder = self.secondaryAPIOrder
                    # update orderID
                    openOrder.setOrderID(orderBack.orderID)
                    # update order status and positions
                    if orderBack.isOrderBackOnly:
                        if openOrder.filledAmount < orderBack.filledAmount:
                            # update pnl before update position
                            self.updatePNLOnOrderBack(orderBack, openOrder)
                            # need to update position here
                            # need to be called before update order filled amount
                            self.updatePositionOnOrderBack(orderBack.filledAmount - openOrder.filledAmount, openOrder.orderType)
                            openOrder.filledAmount = orderBack.filledAmount
                            openOrder.fillPrice = orderBack.fillPrice
                        if openOrder.orderStatus < orderBack.orderStatus:
                            openOrder.orderStatus = orderBack.orderStatus
                        # check if order is finished
                        if openOrder.orderStatus >= trader.defines.orderFilled:
                            # do nothing
                            pass
                        else:
                            # add the order into orderID_OrderMap
                            self.openPositionMapLock.write_acquire()
                            self.orderID_OrderMap[orderBack.orderID] = openOrder
                            self.openPositionMapLock.write_release()
                            # reset secondaryAPIOrder to None
                            self.secondaryAPIOrder = None
                    else:
                        if openOrder.orderStatus < orderBack.orderStatus:
                            openOrder.orderStatus = orderBack.orderStatus
                        # check if order is finished
                        if openOrder.orderStatus >= trader.defines.orderFilled:
                            # do nothing
                            pass
                        else:
                            # add the order into orderID_OrderMap
                            self.openPositionMapLock.write_acquire()
                            self.orderID_OrderMap[orderBack.orderID] = openOrder
                            self.openPositionMapLock.write_release()
                            # reset secondaryAPIOrder to None
                            self.secondaryAPIOrder = None

                else:
                    print("[ERR]:",  trader.coinUtilities.getCurrentTimeString(), \
                          "order feedback orderID is missing in open order map, ID ", \
                          orderBack.orderID, file=sys.stderr)

    # update positions from order feedback
    def updatePositionOnOrderBack(self, fillAmount, orderType):
        if orderType == trader.defines.openLong_limitPrice or orderType == trader.defines.openLong_marketPrice:
            if self.subTractFee:
                self.positionThreadLock.write_acquire()
                self.longShare += (fillAmount * (1.0 - self.openPositionFeeRate))
                self.positionThreadLock.write_release()
            else:
                self.positionThreadLock.write_acquire()
                self.longShare += fillAmount
                self.positionThreadLock.write_release()
        elif orderType == trader.defines.openShort_limitPrice or orderType == trader.defines.openShort_marketPrice:
            if self.subTractFee:
                self.positionThreadLock.write_acquire()
                self.shortShare += (fillAmount * (1.0 - self.openPositionFeeRate))
                self.positionThreadLock.write_release()
            else:
                self.positionThreadLock.write_acquire()
                self.shortShare += fillAmount
                self.positionThreadLock.write_release()
        elif orderType == trader.defines.coverLong_limitPrice or orderType == trader.defines.coverLong_marketPrice:
            self.positionThreadLock.write_acquire()
            self.longShare -= fillAmount
            self.positionThreadLock.write_release()

        elif orderType == trader.defines.coverShort_limitPrice or orderType == trader.defines.coverShort_marketPrice:
            self.positionThreadLock.write_acquire()
            self.shortShare -= fillAmount
            self.positionThreadLock.write_release()
        else:
            print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                  " get undefined orderType : ", orderType, file=sys.stderr)

    # update positions from trade feedback
    def updatePositionOnTradeBack(self, tradeBack):
        # find the order from map
        self.openPositionMapLock.read_acquire()
        if tradeBack.orderID in self.orderID_OrderMap:
            openOrder = self.orderID_OrderMap[tradeBack.orderID]
            self.openPositionMapLock.read_release()
            # update PNL on trade
            self.updatePNLOnTradeBack(tradeBack, openOrder)
            # update position after update PNL
            self.updatePositionForTradeBack(tradeBack.tradeAmount, openOrder.orderType, tradeBack.tradeFee, True)
            # update open order status
            openOrder.filledAmount += tradeBack.tradeAmount
            openOrder.fillPrice = tradeBack.tradePrice
            if abs(abs(openOrder.filledAmount) - abs(openOrder.amount)) / abs(openOrder.amount) < 0.000001:
                openOrder.orderStatus = trader.defines.orderFilled
            # check if order is finished
            if openOrder.orderStatus >= trader.defines.orderFilled:
                self.openPositionMapLock.write_acquire()
                del self.orderID_OrderMap[tradeBack.orderID]
                self.openPositionMapLock.write_release()
            return openOrder
        else:
            # try to find the ID in backup map
            if tradeBack.orderID in self.AllOrder_ID_OrderMap:
                openOrder = self.AllOrder_ID_OrderMap[tradeBack.orderID]
                self.openPositionMapLock.read_release()
                # update PNL on trade
                self.updatePNLOnTradeBack(tradeBack, openOrder)
                # update position after update PNL
                self.updatePositionForTradeBack(tradeBack.tradeAmount, openOrder.orderType, tradeBack.tradeFee, True)
                # update open order status
                openOrder.filledAmount += tradeBack.tradeAmount
                openOrder.fillPrice = tradeBack.tradePrice
                if abs(abs(openOrder.filledAmount) - abs(openOrder.amount)) / abs(openOrder.amount) < 0.000001:
                    openOrder.orderStatus = trader.defines.orderFilled
                return openOrder
            else:
                self.openPositionMapLock.read_release()
                print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                    "trade feedback orderID is missing in open order map, ID ", \
                      tradeBack.orderID, file=sys.stderr)
                return None

    # update positions from order feedback
    def updatePositionForTradeBack(self, fillAmount, orderType, tradeFee = None, subTractFee = False):
        if orderType == trader.defines.openLong_limitPrice or orderType == trader.defines.openLong_marketPrice:
            if subTractFee:
                self.positionThreadLock.write_acquire()
                if tradeFee is None:
                    self.longShare += (fillAmount - abs(tradeFee))
                else:
                    self.longShare += (fillAmount * (1.0 - self.openPositionFeeRate))
                self.positionThreadLock.write_release()
            else:
                self.positionThreadLock.write_acquire()
                self.longShare += fillAmount
                self.positionThreadLock.write_release()
        elif orderType == trader.defines.openShort_limitPrice or orderType == trader.defines.openShort_marketPrice:
            if subTractFee:
                self.positionThreadLock.write_acquire()
                if tradeFee is None:
                    self.shortShare += (fillAmount * (1.0 - self.openPositionFeeRate))
                else:
                    self.shortShare += (fillAmount - abs(tradeFee))
                self.positionThreadLock.write_release()
            else:
                self.positionThreadLock.write_acquire()
                self.shortShare += fillAmount
                self.positionThreadLock.write_release()
        elif orderType == trader.defines.coverLong_limitPrice or orderType == trader.defines.coverLong_marketPrice:
            self.positionThreadLock.write_acquire()
            self.longShare -= fillAmount
            self.positionThreadLock.write_release()

        elif orderType == trader.defines.coverShort_limitPrice or orderType == trader.defines.coverShort_marketPrice:
            self.positionThreadLock.write_acquire()
            self.shortShare -= fillAmount
            self.positionThreadLock.write_release()
        else:
            print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                  " get undefined orderType : ", orderType, file=sys.stderr)

    # query current positions
    def getLongShare(self):
        self.positionThreadLock.read_acquire()
        longShares = self.longShare
        self.positionThreadLock.read_release()
        return longShares
    def getShortShare(self):
        self.positionThreadLock.read_acquire()
        shortShares = self.shortShare
        self.positionThreadLock.read_release()
        return shortShares
    def getNetShare(self):
        self.positionThreadLock.read_acquire()
        netShares = self.longShare - self.shortShare
        self.positionThreadLock.read_release()
        return netShares
    def getTradeAPI(self):
        return self.API
    def getOpenOrders(self):
        openOrderList = []
        self.openPositionMapLock.read_acquire()
        for iOrder in self.orderID_OrderMap.values():
            if iOrder.getOrderStatus() < trader.defines.orderFilled:
                openOrderList.append(iOrder)
        self.openPositionMapLock.read_release()
        if self.secondaryAPIOrder:
            openOrderList.append(self.secondaryAPIOrder)
        return openOrderList

    # used to save position info to file
    def getPositonInfoString(self):
        return self.symbol + "," + self.expire  + "," + self.API + "," + str(self.getLongShare()) + \
               "," + str(self.getShortShare()) + "," + self.strategyTag  + "," + str(self.getGrossPNL()) + "," + \
               str(self.getFees()) + "," + str(self.lastPrice) + "," + str(self.contractSize) + "," + str(self.subTractFee) + "\n"

    # used to print current position to scream
    def printPositionContent(self):
        long = self.getLongShare()
        short = self.getShortShare()
        gross = self.getGrossPNL()
        Fee = self.getFees()
        lastPrice = self.getLastPrice()
        # print("gross", gross, "self.baseCashUnit", self.baseCashUnit, "last", lastPrice)
        if self.isSpot:
            outString = "{0: <8s} {1: <10s} {2: <20s} {3:10.6f} {4: 10.6f} {5: <10s} {6:10.6f} {7:8.6f}".format( \
                   self.symbol, self.expire, self.API, long, short, \
                self.strategyTag, gross * self.baseCashUnit, Fee * self.baseCashUnit)
            print(outString, file=sys.stdout)
        else:
            outString = "{0: <8s} {1: <10s} {2: <20s} {3:10.6f} {4: 10.6f} {5: <10s} {6:10.6f} {7:8.6f}".format( \
                self.symbol, self.expire, self.API, long, short, \
                self.strategyTag, gross * self.baseCashUnit, Fee * self.baseCashUnit)
            print(outString, file=sys.stdout)

    # used to merge positions
    def mergePosition(self, position):
        if self.isMyTradeTicker(position.symbol, position.expire, position.API):
            # merge infos
            self.longShare += position.getLongShare()
            self.shortShare += position.getShortShare()
            self.grossPNL += position.getGrossPNL()
            self.Fee += position.getFees()
            return True
        else:
            return False

    # used to set base cash value
    def setBaseCashValue(self, baseCashMap):
        if self.isSpot:
            # get the base of the spot
            self.baseCashUnit = baseCashMap[self.symbol.split("_")[1]]
        else:
            # all the future are using dollar
            self.baseCashUnit = baseCashMap["dollar"]

    def setVolumeMiniMove(self, volumeMiniMove):
        if not isinstance(volumeMiniMove, numbers.Number):
            print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                  " error input dataType for volumeMiniMove in function setVolumeMiniMove", file=sys.stderr)
            return -1
        if (not self.isSpot) and (volumeMiniMove != 0):
            print("[WARN]:", trader.coinUtilities.getCurrentTimeString(), \
                  "only for spot, volumeMiniMove can be adjusted", file=sys.stdout)
            self.volumeMiniMove = 1
        else:
            self.volumeMiniMove = volumeMiniMove

    def setPriceMiniMove(self, priceMiniMove):
        if not isinstance(priceMiniMove, numbers.Number):
            print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                  " error input dataType for priceMiniMove in function setPriceMiniMove", file=sys.stderr)
            return -1
        self.priceMiniMove = priceMiniMove

    def setVolumeBase(self, volumeBase):
        if not isinstance(volumeBase, numbers.Number):
            print("[ERR]:", trader.coinUtilities.getCurrentTimeString(), \
                  " error input dataType for priceMiniMove in function setPriceMiniMove", file=sys.stderr)
            return -1
        self.volumeBase = volumeBase

##########################################################################################
info = pandas.read_csv(info_csv_file).set_index(["source", "symbol"])

def get_volume_miniMove(symbol, expire, api):
    try:
        market = to_market(api)
        ticker = to_ticker(symbol, expire)
        return info.loc[(market, ticker), "volumeMiniMove"]
    except:
        print("[ERR]", trader.coinUtilities.getCurrentTimeString(), \
              "can not find info for ({}, {})".format(market, ticker), file=sys.stderr)
        return 0.01
def get_price_miniMove(symbol, expire, api):
    try:
        market = to_market(api)
        ticker = to_ticker(symbol, expire)
        return info.loc[(market, ticker), "priceMiniMove"]
    except:
        print("[ERR]", trader.coinUtilities.getCurrentTimeString(), \
              "can not find info for ({}, {})".format(market, ticker), file=sys.stderr)
        return 0.01
def get_volume_base(symbol, expire, api):
    try:
        market = to_market(api)
        ticker = to_ticker(symbol, expire)
        return info.loc[(market, ticker), "baseVolume"]
    except:
        print("[ERR]", trader.coinUtilities.getCurrentTimeString(), \
              "can not find info for ({}, {})".format(market, ticker), file=sys.stderr)
        return 1
def to_ticker(symbol, expire):
    if expire != "None":
        return symbol + "@"+expire[0]
    return symbol
def to_market(api):
    api = api.lower()
    i = api.find("api") if api.find("api") >= 0 else api.find("spi")
    if i < 0:
        print("[ERR]", trader.coinUtilities.getCurrentTimeString(), \
              "can not get market name for {}".format(api), file=sys.stderr)
        raise ValueError("Failed to find market for api {}".format(api))
    else:
        return api[:i]
def get_precision(num):
    return int(-math.log10(num))
