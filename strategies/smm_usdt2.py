import sys
import configparser
import threading
import trader.coinUtilities
import trader.coinTradeStruct
import trader.defines
import numpy as np
import os, datetime, math, json, time

from strategies.simpleStrategy import SimpleStrategy
import pandas as pd


HIGH = 1
LOW = 0
UPWARDS = 1
DOWNWARDS = -1

col_ap = ['AP' + str(i) for i in range(1, 21)]
col_av = ['AV' + str(i) for i in range(1, 21)]
col_bp = ['BP' + str(i) for i in range(1, 21)]
col_bv = ['BV' + str(i) for i in range(1, 21)]
col = ['time'] + ['symbol'] + ['last price'] + ['volume'] + col_ap + col_av + col_bp  + col_bv + ['buy_count'] + ['sell_count']

aux_col = ['symbol'] + ["BP"] + ["AP"] + ["BidTime"] + ["AskTime"] + ["PnL"] + ["qty"]


class SMMStrategy(SimpleStrategy):
    def __init__(self, path2Config, cache):
        super(SMMStrategy, self).__init__(path2Config, cache)
        self.buy_count, self.sell_count = 0, 0
        self.last_buy_order_qty, self.last_sell_order_qty = 0, 0
        self.last_buy_order, self.last_sell_order = None, None
        self.fee_ratio = 0.00005  # 0.0002
        self.first_start = True
        self.pricerange = 1e-5
        self.initCoinAQty = 0
        self.initCoinBQty = 0

        self.isMaker = False

        """
            Ben Liu's params
        """



    def initStrategy(self):
        config = configparser.ConfigParser()
        config.read(self.path2Configure)

        # ------------------------------------------ strategy config ---------------------------------------------------------------------------
        self.ticker = "pax_usdt"
        self.coinA, self.coinB = self.ticker.split("_")
        self.api = "BinanceSpi_spot_web"  # config["Settings"]["api"]
        self.subscribe(self.ticker, "None", self.api)

        # ------------------------------------------ strategy management & strategy stats params ------------------------------------------------
        self.tradeVol = 1000 # Param
        self.iszero_buy_count = 0
        self.iszero_sell_count = 0

        self.smm_pnl = 0
        self.profitability = 0
        self.total_amount = 0
        self.all_pnl = []
        self.all_win, self.all_loss, self.all_tie = [0], [0], [0]
        self.total_win, self.total_loss = 0, 0
        self.pnl_stoploss = False
        self.pnl_stopprofit = False
        self.exposure_stopselling = False
        self.exposure_stopbuying = False
        self.stopTradingDeltaQty = min(3*self.tradeVol, 1500) # Param

        self.num_flat = 5 # Param
        self.catch_flat = 3 # Param

        self.bufferVol1 = 500 # Param
        self.bufferVol2 = 800 # Param
        self.initUsdtBalance = 30000 # Param
        self.usdtBalanceHi = 45000 # Param
        self.usdtBalanceLo = 15000 # Param

        self.deltaTime = 10 # Param
        self.stopTradingLoss = -30 # Param

        self.strpre = "usdt2 " # Param

        self.aux_buy_queue, self.aux_sell_queue = [], []
        self.ignoreOrderIDLs = []
        self.ignoreOrderStatusLs = [trader.defines.orderFilling, trader.defines.orderPending, trader.defines.orderInit]


        # --------------------------------------------- user inputs (to be arranged) ------------------------------------------------------------
        self.stop_trading = False

        # --------------------------------------------- helper params (to be deleted) -------------------------------------------------------------
        self.risk = LOW
        self.volatility = -1
        self.bb_down_level = 0
        self.bb_up_level = 0
        self.spread_level = 0
        self.midPriceMove_level = 0
        self.flat_level = 0
        self.flat2_level = 0


        self.unit_pct = 5e-4
        self.ask_bid_one_tol = self.get_price_miniMove(self.ticker, "None", self.api)

        self.index = 0
        self.period = 10
        self.num_vol = 10000
        self.vol_hist_count = 0
        self.count = 0

        self.period_bb = 20
        self.count_bb = 0

        self.depth_stack = []
        self.flat_stack = []

        self.isFirstUSDTBalance = True

        # self.cancel_thread = threading.Thread(target=self.cancel_lt_order)
        # self.cancel_thread.setDaemon(True)
        # self.cancel_thread.start()

        self.lastTime = time.time()


        # ---------------------------------------------- temp params ------------------------------------------------------------------------

        return True


    ###########################################################################
    ####                            STRATEGY BODY
    ###########################################################################
    def on_depth(self, cur_depth):
        sys.stdout.flush()
        expire = "None" if cur_depth.isSpot else cur_depth.expiry
        api = cur_depth.API

        if (cur_depth.symbol == self.ticker) and (expire == "None") and (api == self.api):
            self.last_depth = cur_depth
        else:
            return


        depth = self.last_depth
        # asks = depth.Asks[:20]
        # askvolumes = depth.AskVolumes[:20]
        # bids = depth.Bids[:20]
        # bidvolumes = depth.BidVolumes[:20]

        # lastPrice = depth.LastPrice
        # Volume = depth.Volume
        # data = [depth.localTimeStamp] + [depth.symbol] + [lastPrice] + [Volume] + asks + askvolumes + bids + bidvolumes + [self.buy_count] + [self.sell_count]
        # df = pd.DataFrame(dict(zip(col, data)), columns=col, index=[self.index])
        # self.index += 1


        if self.first_start:
            import re
            path_parent=re.sub(r"(trader)$", "logs", os.getcwd())
            current_time = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            self.path2csv = os.path.join(path_parent, "trade_"+current_time+".csv")
            self.aux_path2csv = os.path.join(path_parent, "aux_"+current_time+".csv")

            askprice = self.adjust_AskPrice()
            bidprice = self.adjust_BidPrice()
            # ----------------------------------- helper params (cont) -----------------------------------------------------------
            self.midPrice = (askprice + bidprice) / 2
            self.value = self.get_balance(self.coinA, "None", self.api) * self.midPrice
            self.value += self.get_balance(self.coinB, "None", self.api)
            self.initCoinAQty = self.get_balance(self.coinA, "None", self.api)
            self.initCoinBQty = self.get_balance(self.coinB, "None", self.api)
            self.adjusted_ask = self.adjust_AskPrice()
            self.adjusted_bid = self.adjust_BidPrice()
            self.lp = [self.last_depth.LastPrice] * self.period
            self.ma = [self.last_depth.LastPrice] * self.period
            self.md = [0] * self.period
            self.askprices = [self.adjusted_ask] * self.period
            self.bidprices = [self.adjusted_bid] * self.period
            self.vol_hist = [0] * self.num_vol

            self.lp_bb = [self.last_depth.LastPrice] * self.period_bb
            self.ma_bb = [self.last_depth.LastPrice] * self.period_bb
            self.md_bb = [0] * self.period_bb
            self.ssub = [self.last_depth.LastPrice] * self.period_bb
            self.ub = [self.last_depth.LastPrice] * self.period_bb
            self.umid = [self.last_depth.LastPrice] * self.period_bb
            self.lmid = [self.last_depth.LastPrice] * self.period_bb
            self.lb = [self.last_depth.LastPrice] * self.period_bb
            self.sslb = [self.last_depth.LastPrice] * self.period_bb

            self.period_pm = 3000
            self.count_pm = 0
            self.midprice_pm = [self.last_depth.LastPrice] * self.period_pm
            self.midpricemove_pm = [0]* self.period_pm


            self.log_info("account value at the first is {} usdt".format(self.value))
            self.first_start = False

            # df.to_csv(self.path2csv, index=False, sep=',')
        else:
            pass
            # df.to_csv(self.path2csv, mode='a', header=False, sep=',', index=False)

            # while len(self.aux_buy_queue) > 0 and len(self.aux_sell_queue) > 0:
            # # if self.aux_buy_sell_pair[0] != None and self.aux_buy_sell_pair[1] != None and self.aux_buy_sell_pair[2] != 0:
            #     bid_order, ask_order = self.aux_buy_queue.pop(0), self.aux_sell_queue.pop(0)
            #     bid_price = bid_order.price
            #     ask_price = ask_order.price
            #     bid_time = bid_order.orderUpdateTime
            #     ask_time = ask_order.orderUpdateTime
            #     qty = min(bid_order.filledAmount, ask_order.filledAmount)
            #     pnl = (ask_price - bid_price) * qty
            #     data = [depth.symbol] + [bid_price] + [ask_price] + [bid_time] + [ask_time] + [pnl] + [qty]
            #     df = pd.DataFrame(dict(zip(aux_col, data)), columns=aux_col, index=[self.aux_index])
            #     self.aux_index += 1

            #     # simple store on depth
            #     if bid_order.filledAmount > qty:
            #         new_bid_order = bid_order
            #         new_bid_order.filledAmount -= qty
            #         self.aux_buy_queue.insert(0, new_bid_order)
            #     if ask_order.filledAmount > qty:
            #         new_ask_order = ask_order
            #         new_ask_order.filledAmount -= qty
            #         self.aux_sell_queue.insert(0, new_ask_order)
            #     # if qty == bid_order.filledAmount:
            #     #     self.aux_buy_sell_pair[0] = None
            #     #     if len(self.aux_buy_wl) > 0:
            #     #         self.aux_buy_sell_pair[0] = self.aux_buy_wl.pop(0)
            #     # elif qty < bid_order.filledAmount:
            #     #     self.aux_buy_sell_pair[0].filledAmount -= qty
            #     # if qty == ask_order.filledAmount:
            #     #     self.aux_buy_sell_pair[1] = None
            #     #     if len(self.aux_sell_wl) > 0:
            #     #         self.aux_buy_sell_pair[1] = self.aux_sell_wl.pop(0)
            #     # elif qty < ask_order.filledAmount:
            #     #     self.aux_buy_sell_pair[1].filledAmount -= qty
            #     # self.aux_buy_sell_pair[2] = 0

            #     if self.aux_is_first:
            #         # df.to_csv(self.aux_path2csv, index=False, sep=',')
            #         self.aux_is_first = False
            #     else:
            #         # df.to_csv(self.aux_path2csv, mode='a', header=False, sep=',', index=False)
            #         pass

        # self.compute_midPriceMove_level()
        self.get_volatility()
        self.get_bb_level()
        self.get_spread_level()
        self.compute_flat_level()
        self.eval_risk()
        self.do_signal()

    # ----------------------------------------------------------------------- 监听模块 --------------------------------------------------
    def do_signal(self):
        net_buy = round((self.buy_count - self.sell_count)/self.tradeVol, 0)
        net_sell = round((self.sell_count - self.buy_count)/self.tradeVol, 0)
        net_buy1 = round((self.buy_count - self.sell_count)/self.tradeVol, 2)
        net_sell1 = round((self.sell_count - self.buy_count)/self.tradeVol, 2)
        askPrice = self.last_depth.AskPrice
        bidPrice = self.last_depth.BidPrice
        strpre = self.strpre

        usdtBalance = self.trader.getCoinBalance("usdt", self.api)
        if usdtBalance:
            if self.isFirstUSDTBalance:
                self.initUsdtBalance = usdtBalance
                self.isFirstUSDTBalance = False
            if usdtBalance > self.usdtBalanceHi:
                self.exposure_stopselling = True
                # print("bstopselling since usdt is ", usdtBalance)
            elif usdtBalance < self.usdtBalanceLo:
                self.exposure_stopbuying = True
                # print("bstopbuying since usdt is ", usdtBalance)
            else:
                pass
                # print("usdt is ", usdtBalance)
            netbuyUsdt = round( (usdtBalance - self.initUsdtBalance)/self.tradeVol, 0)
            netsellUsdt = round( (self.initUsdtBalance - usdtBalance)/self.tradeVol, 0)
        else:
            netsellUsdt, netbuyUsdt = 0, 0

        # net_buy = max(net_buy, netsellUsdt)
        # net_sell = max(net_sell, netbuyUsdt)
        # net_buy1 = max(net_buy1, netsellUsdt)
        # net_sell1 = max(net_sell1, netbuyUsdt)

        # self.exposure_stopselling = True

        if net_buy * self.tradeVol >= self.stopTradingDeltaQty:
            self.exposure_stopbuying = True
        if net_sell * self.tradeVol >= self.stopTradingDeltaQty:
            self.exposure_stopselling = True

        # if self.pnl_stoploss:
        #     print(self.pnl_stoploss, self.pnl_stopprofit, self.stop_trading)
        #     if self.buy_count < self.sell_count:
        #         ret1 = self.balanceBuyOne()
        #         ret2 = self.testSellOne(net_sell)
        #         print("stoploss", ret1, " ", ret2)
        #     elif self.buy_count > self.sell_count:
        #         ret1 = self.balanceSellOne()
        #         ret2 = self.testBuyOne(net_buy)
        #         print("stoploss", ret1, " ", ret2)
        #     elif self.buy_count == self.sell_count:
        #         self.pnl_stoploss = False
        #         self.pnl_stopprofit = False
        #     return

        curTime = time.time()
        if curTime - self.lastTime <= self.deltaTime:
            return
        else:
            self.lastTime = curTime

        open_orders = self.getOpenOrders()
        open_orders_ls = [order.orderID for order in open_orders if order.orderStatus not in self.ignoreOrderStatusLs]
        # open_orders_ls = [order.orderID for order in open_orders if order.orderID not in self.ignoreOrderIDLs]
        if len(open_orders_ls) >= 3:
            cnt = 0
            print(strpre+"bigger than 3 orders")
            for order in open_orders:
                if order.orderID not in open_orders_ls:
                    print(strpre+"and orderstatus is ", order.orderStatus)
                    continue
                if cnt > 0:
                    break
                if self.last_buy_order and order.orderID == self.last_buy_order.orderID:
                    continue
                elif self.last_sell_order and order.orderID == self.last_sell_order.orderID:
                    continue
                elif order.orderStatus == trader.defines.orderFilling or order.orderStatus==trader.defines.orderPending or order.orderStatus == trader.defines.orderInit:
                    self.ignoreOrderIDLs.append(order.orderID)
                    print(strpre+"and orderstatus is ", order.orderStatus)
                    continue
                else:
                    print(strpre+"Cancel Long Time Order", order.orderID)
                    self.trader.cancelOrder(order)
                    cnt += 1
            return

        if self.exposure_stopbuying:
            if self.spread_level == 2:
                ret1 = self.aheadSellOne(net_sell)
            else:
                ret1 = self.testSellOne(net_sell)
            if self.last_buy_order:
                self.trader.cancelOrder(self.last_buy_order)
            print(strpre+"stopbuying ", ret1)
            return
        elif self.exposure_stopselling:
            if self.spread_level == 2:
                ret1 = self.aheadBuyOne(net_buy)
            else:
                ret1 = self.testBuyOne(net_buy)
            if self.last_sell_order:
                self.trader.cancelOrder(self.last_sell_order)
            print(strpre+"stopselling ", ret1)
            return
        elif self.spread_level == 2:
            ret1 = self.aheadBuyOne(net_buy)
            ret2 = self.aheadSellOne(net_sell)
            print(strpre+"spread2 ", ret1, " ", ret2)
            return
        elif self.flat2_level == 1:
            ret1 = self.safeBuyOne(net_buy)
            ret2 = self.safeSellOne(net_sell)
            print(strpre+"too volatile and activate skipping ", ret1, " ", ret2)
            return
        elif self.flat_level == 1:
            ret1 = self.testSellOne(net_sell)
            # ret1 = self.protectedSellOne(net_sell)
            print(strpre+"flatselling ", ret1)
            return
        elif self.flat_level == 2:
            ret1 = self.testBuyOne(net_buy)
            # ret1 = self.protectedBuyOne(net_buy)
            print(strpre+"flatbuying ", ret1)
            return
        elif self.flat_level == 3:
            print(strpre+"temp skipping")
            return
        else:
            ret1 = self.testBuyOne(net_buy)
            ret2 = self.testSellOne(net_sell)
            # ret1 = self.protectedBuyOne(net_buy)
            # ret2 = self.protectedSellOne(net_sell)
            print(strpre+"testbs ", ret1, " ", ret2)
            return


        print(strpre+"do nothing !!!!!!!!")

    # 0 is narrow spread, 1 is wide spread
    def get_spread_level(self):
        midPrice = (self.last_depth.AskPrice + self.last_depth.BidPrice) * 0.5
        self.spread = self.last_depth.AskPrice - self.last_depth.BidPrice
        spread = self.spread
        if round(spread, 8) >= 0.0003:
            # self.log_info("spread is big {}".format(self.spread))
            self.spread_level = 2
            return self.spread_level
        elif round(spread, 8) >= 0.0002:
            self.spread_level = 1
            return self.spread_level
        else:
            self.spread_level = 0
            return self.spread_level

    # 0 is flat; 1 is bid not flat, 2 is ask not flat, 3 is bid ask not flat
    def compute_flat_level(self):
        self.depth_stack.append(self.last_depth)
        if len(self.depth_stack) >= 2:
            last_depth = self.depth_stack[-1]
            lastSec_depth = self.depth_stack[-2]
            if round(last_depth.BidPrice, 8) == round(lastSec_depth.BidPrice, 8) and round(last_depth.AskPrice, 8) == round(lastSec_depth.AskPrice, 8):
                self.flat_level = 0
            elif last_depth.BidPrice > lastSec_depth.BidPrice and round(last_depth.AskPrice, 8) == round(lastSec_depth.AskPrice, 8):
                self.flat_level = 1
            elif round(last_depth.BidPrice, 8) == round(lastSec_depth.BidPrice, 8) and last_depth.AskPrice < lastSec_depth.AskPrice:
                self.flat_level = 2
            elif last_depth.BidPrice > lastSec_depth.BidPrice and last_depth.AskPrice < lastSec_depth.AskPrice:
                self.flat_level = 3
        else:
            self.flat_level = 0
        self.flat_stack.append(self.flat_level)
        self.compute_flat2_level()
        return self.flat_level

    def compute_flat2_level(self):
        num_flat = self.num_flat
        catch_flat = self.catch_flat
        if len(self.flat_stack) >= num_flat:
            last_flat_list = self.flat_stack[-num_flat:-1]
            count = 0
            is_bid, is_ask = False, False
            for flat in last_flat_list:
                if flat != 0:
                    count += 1
                if flat == 1:
                    is_bid = True
                if flat == 2:
                    is_ask = True
                if flat == 3:
                    is_bid = True
                    is_ask = True
            if count >= catch_flat and is_bid and is_ask:
                self.flat2_level = 1
            else:
                self.flat2_level = 0
        else:
            self.flat2_level = 0
        return self.flat2_level


    def compute_midPriceMove_level(self):
        """
        rets:
        midPriceMove_level      - 1 is midpricemove up
                                - 0 is midpricemove stable
                                - -1 is midpricemove down
        """
        midPrice = (self.last_depth.AskPrice + self.last_depth.BidPrice) * 0.5

        self.midprice_pm.pop(0)
        self.midprice_pm.append(midPrice)

        self.midpricemove_pm.pop(0)
        self.midpricemove_pm.append(midPrice / self.midprice_pm[-2] - 1)

        self.count_pm += 1

        if self.count_pm >= self.period_pm:
            # TODO
            thred_pm_level = [0, 0]
            thred_pm_level[0] = min(0, np.percentile(self.midpricemove_pm, 10))
            thred_pm_level[1] = max(0, np.percentile(self.midpricemove_pm, 90))

            if self.midpricemove_pm[-1] >= thred_pm_level[1]:
                self.midPriceMove_level = 1
                return self.midPriceMove_level
            elif self.midpricemove_pm[-1] >= thred_pm_level[0]:
                self.midPriceMove_level = 0
                return self.midPriceMove_level
            elif self.midpricemove_pm[-1] < thred_pm_level[0]:
                self.midPriceMove_level = -1
                return self.midPriceMove_level
            else:
                assert False
        else:
            self.midPriceMove_level = 0
            return self.midPriceMove_level

    def get_volatility(self):
        self.lp.pop(0)
        self.lp.append(self.last_depth.LastPrice)  # update lp list
        self.ma = np.average(self.lp)

        self.md.pop(0)
        self.md.append(np.sqrt(np.sum((self.lp - self.ma) ** 2) / self.period))
        self.count = self.count + 1

        self.vol_hist.pop(0)
        self.vol_hist.append(self.md[-1]*100)
        self.vol_hist_count += 1

        vol_thred = [0.05, 0.15, 0.20, 0.60]
        if self.vol_hist_count >= self.num_vol:
            vol_thred[0] = max(0.05, np.percentile(self.vol_hist, 30))
            vol_thred[1] = max(0.15, np.percentile(self.vol_hist, 70))
            vol_thred[2] = max(0.20, np.percentile(self.vol_hist, 90))
            vol_thred[3] = max(0.60, np.percentile(self.vol_hist, 95))


        if self.count >= self.period:
            # self.log_info("standard deviation max:{}, min:{}".format(max(self.md), min(self.md)))
            if self.md[-1]*100 >= vol_thred[3]:
                self.log_info("the volatility is too big {}".format(self.md[-1]))
                self.volatility = 3
                return self.volatility
            elif self.md[-1]*100 >=  vol_thred[2]: # 0.75
                self.log_info("the volatility is too big {}".format(self.md[-1]))
                self.volatility = 2
                return self.volatility
            elif self.md[-1]*100 >= vol_thred[1]:
                self.log_info("the volatility is big {}".format(self.md[-1]))
                self.volatility = 1
                return self.volatility
            elif self.md[-1]*100 >= vol_thred[0]: # 0.45
                self.volatility = 0
                return self.volatility
            elif self.md[-1]*100 < vol_thred[0]:
                self.volatility = -1
                return self.volatility
            else:
                return 0
        else:
            self.volatility = 0
            return self.volatility

    def get_bb_level(self):

        self.lp_bb.pop(0)
        self.lp_bb.append(self.last_depth.LastPrice)  # update lp list

        self.ma_bb.pop(0)
        self.ma_bb.append(np.mean(self.lp_bb))


        self.md_bb.pop(0)
        self.md_bb.append(np.std(self.lp_bb))

        self.ssub.pop(0)
        self.ssub.append(self.ma_bb[-1] + 3 * self.md_bb[-1])

        self.ub.pop(0)
        self.ub.append(self.ma_bb[-1] + 2 * self.md_bb[-1])

        self.umid.pop(0)
        self.umid.append(self.ma_bb[-1] + 1 * self.md_bb[-1])

        self.lmid.pop(0)
        self.lmid.append(self.ma_bb[-1] - 1 * self.md_bb[-1])

        self.lb.pop(0)
        self.lb.append(self.ma_bb[-1] - 2 * self.md_bb[-1])

        self.sslb.pop(0)
        self.sslb.append(self.ma_bb[-1] - 3 * self.md_bb[-1])

        self.count_bb += 1

        if self.count_bb >= self.period_bb:
            # if self.lp[-1] >= self.ssub[-1]:
            #     self.log_info("the volatility is too big {}".format(self.md_bb[-1]))
            #     self.bb_level = 3
            #     return self.bb_level
            # elif self.lp[-1] >= self.ub[-1]:
            #     self.log_info("the volatility is big {}".format(self.md[-1]))
            #     self.bb_level = 2
            #     return self.bb_level
            # elif self.lp[-1] >= self.umid[-1]:
            #     self.bb_level = 1
            #     return self.bb_level
            # elif self.lp[-1] >= self.lmid[-1]:
            #     self.bb_level = 0
            #     return self.bb_level
            # elif self.lp[-1] >= self.lb[-1]:
            #     self.bb_level = 1
            #     return self.bb_level
            # elif self.lp[-1] >= self.sslb[-1]:
            #     self.log_info("the volatility is big {}".format(self.md[-1]))
            #     self.bb_level = 2
            #     return self.bb_level
            # else:
            #     self.log_info("the volatility is too big {}".format(self.md[-1]))
            #     self.bb_level = 3
            #     return self.bb_level
            if self.last_depth.AskPrice >= self.ub[-1] and self.md_bb[-1] >= 3e-4:
                self.bb_up_level = 1
            else:
                self.bb_up_level = 0
            if self.last_depth.BidPrice <= self.lb[-1] and self.md_bb[-1] >= 3e-4:
                self.bb_down_level = 1
            else:
                self.bb_down_level = 0
            return self.bb_up_level, self.bb_down_level
        else:
            self.bb_up_level = 0; self.bb_down_level = 0;
            return self.bb_up_level, self.bb_down_level


    # ----------------------------------------------------------- 定价模块 ------------------------------------------------------------------
    def testBuyOne(self, net_buy):
        spread = self.last_depth.AskPrice - self.last_depth.BidPrice
        pos = 0 if self.volatility + 1 <= 1 else 1
        buyPrice = self.last_depth.BidPrice - max(2 * net_buy, pos) * self.get_price_miniMove(self.ticker, "None", self.api) * (2 + self.volatility) #* 2
        ret = self.adjust_buy_order(buyPrice, "testBuyOne", 1)
        return ret

    def testSellOne(self, net_sell):
        spread = self.last_depth.AskPrice - self.last_depth.BidPrice
        pos = 0 if self.volatility + 1 <= 1 else 1
        sellPrice = self.last_depth.AskPrice + max(2 * net_sell, pos) * self.get_price_miniMove(self.ticker, "None", self.api) * (2 + self.volatility) #* 2
        ret = self.adjust_sell_order(sellPrice, "testSellOne", 1)
        return ret

    def aheadBuyOne(self, net_buy):
        if net_buy <= 0:
            buyPrice = self.last_depth.BidPrice + self.get_price_miniMove(self.ticker, "None", self.api)
            if self.last_buy_order and round(self.last_depth.BidPrice, 8) == round(self.last_buy_order.price, 8):
                buyPrice = self.last_depth.BidPrice
            if self.last_depth.BidVolumes[0] < 10:
                buyPrice = self.last_depth.BidPrice

            ret = self.adjust_buy_order(buyPrice, "aheadBuyOne", 1)
            return ret
        else:
            return self.protectedBuyOne(net_buy)

    def aheadSellOne(self, net_sell):
        if net_sell <= 0:
            sellPrice = self.last_depth.AskPrice - self.get_price_miniMove(self.ticker, "None", self.api)
            if self.last_sell_order and round(self.last_depth.AskPrice, 8) == round(self.last_sell_order.price, 8):
                sellPrice = self.last_depth.AskPrice
            if self.last_depth.BidVolumes[0] < 10:
                sellPrice = self.last_depth.AskPrice

            ret = self.adjust_sell_order(sellPrice, "aheadSellOne", 1)
            return ret
        else:
            return self.protectedSellOne(net_sell)


    def safeBuyOne(self, net_buy):
        spread = self.last_depth.AskPrice - self.last_depth.BidPrice
        pos = 0 if self.volatility + 1 == 0 else 1
        buyPrice = self.last_depth.BidPrice - max(net_buy, 1) * self.get_price_miniMove(self.ticker, "None", self.api) * (6 + self.volatility) #* 2
        ret = self.adjust_buy_order(buyPrice, "safeBuyOne", 1)
        return ret

    def safeSellOne(self, net_sell):
        spread = self.last_depth.AskPrice - self.last_depth.BidPrice
        pos = 0 if self.volatility + 1 == 0 else 1
        sellPrice = self.last_depth.AskPrice + max(net_sell, 1) * self.get_price_miniMove(self.ticker, "None", self.api) * (6 + self.volatility) #* 2
        ret = self.adjust_sell_order(sellPrice, "safeSellOne", 1)
        return ret

    def protectedBuyOne(self, net_buy):
        baseVol = []
        bufferVol = []
        bufferVolRemain, bufferVolEnter = self.bufferVol1, self.bufferVol2
        for i in range(5):
            if self.last_buy_order and round(self.last_buy_order.price, 8) == round(self.last_depth.Bids[i], 8):
                baseVol.append(self.last_buy_order.amount - self.last_buy_order.filledAmount)
                bufferVol.append(bufferVolRemain)
            else:
                baseVol.append(0)
                bufferVol.append(bufferVolEnter)
        if self.last_depth.BidVolumes[0] > baseVol[0] + bufferVol[0]:
            buyPrice = self.last_depth.BidPrice
        elif self.last_depth.BidVolumes[1] > baseVol[1] + bufferVol[1]:
            buyPrice = self.last_depth.Bids[1]
        elif self.last_depth.BidVolumes[2] > baseVol[2] + bufferVol[2]:
            buyPrice = self.last_depth.Bids[2]
        elif self.last_depth.BidVolumes[3] > baseVol[3] + bufferVol[3]:
            buyPrice = self.last_depth.Bids[3]
        else:
            buyPrice = self.last_depth.Bids[4]

        mult = 2 + self.volatility
        pos = 0 if self.volatility + 1 <= 2 else 1
        buyPrice = buyPrice - max(net_buy, pos) * self.get_price_miniMove(self.ticker, "None", self.api) * mult

        ret = self.adjust_buy_order(buyPrice, "protectedBuyOne", 1)
        return ret


    def protectedSellOne(self, net_sell):
        baseVol = []
        bufferVol = []
        bufferVolRemain, bufferVolEnter = self.bufferVol1, self.bufferVol2
        for i in range(5):
            if self.last_sell_order and round(self.last_sell_order.price, 8) == round(self.last_depth.Asks[i], 8):
                baseVol.append(self.last_sell_order.amount - self.last_sell_order.filledAmount)
                bufferVol.append(bufferVolRemain)
            else:
                baseVol.append(0)
                bufferVol.append(bufferVolEnter)
        if self.last_depth.AskVolumes[0] > baseVol[0] + bufferVol[0]:
            sellPrice = self.last_depth.AskPrice
        elif self.last_depth.AskVolumes[1] > baseVol[1] + bufferVol[1]:
            sellPrice = self.last_depth.Asks[1]
        elif self.last_depth.AskVolumes[2] > baseVol[2] + bufferVol[2]:
            sellPrice = self.last_depth.Asks[2]
        elif self.last_depth.AskVolumes[3] > baseVol[3] + bufferVol[3]:
            sellPrice = self.last_depth.Asks[3]
        else:
            sellPrice = self.last_depth.Asks[4]

        mult = 2 + self.volatility
        pos = 0 if self.volatility + 1 <= 2 else 1
        sellPrice = sellPrice + max(net_sell, pos) * self.get_price_miniMove(self.ticker, "None", self.api) * mult

        ret = self.adjust_sell_order(sellPrice, "protectedSellOne", 1)
        return ret

    def clearBuyAll(self):
        buyPrice = self.last_depth.BidPrice + 3 * self.get_price_miniMove(self.ticker, "None", self.api)
        qty_mult = math.floor((self.sell_count - self.buy_count)/self.tradeVol)
        ret = self.adjust_buy_order(buyPrice, "clearBuyAll", qty_mult)
        return ret

    def clearSellAll(self):
        sellPrice = self.last_depth.AskPrice - 3 * self.get_price_miniMove(self.ticker, "None", self.api)
        qty_mult = math.floor((self.buy_count - self.sell_count)/self.tradeVol)
        ret = self.adjust_sell_order(sellPrice, "clearSellAll", qty_mult)
        return ret

    def balanceBuyOne(self):
        buyPrice = self.last_depth.BidPrice + self.get_price_miniMove(self.ticker, "None", self.api)
        ret = self.adjust_buy_order(buyPrice, "balanceBuyOne", 1)
        return ret

    def balanceSellOne(self):
        sellPrice = self.last_depth.AskPrice - self.get_price_miniMove(self.ticker, "None", self.api)
        ret = self.adjust_sell_order(sellPrice, "balanceSellOne", 1)
        return ret

    def seizeBuyOne(self, net_buy):
        spread = self.last_depth.AskPrice - self.last_depth.BidPrice
        if net_buy <= 0:
            buyPrice = self.last_depth.BidPrice + spread * 0.2
        else:
            buyPrice = self.last_depth.BidPrice - 2 * net_buy * self.get_price_miniMove(self.ticker, "None", self.api)
        ret = self.adjust_buy_order(buyPrice, "seizeBuyOne", 1)
        return ret

    def seizeSellOne(self, net_sell):
        spread = self.last_depth.AskPrice - self.last_depth.BidPrice
        if net_sell <= 0:
            sellPrice = self.last_depth.AskPrice - spread * 0.2
        else:
            sellPrice = self.last_depth.AskPrice + 2 * net_sell * self.get_price_miniMove(self.ticker, "None", self.api)
        ret = self.adjust_sell_order(sellPrice, "seizeSellOne", 1)
        return ret


    def price_engine(self):
        pass

    # --------------------------------------------------------------- 管理模块 --------------------------------------------------------------------
    def eval_risk(self):
        buyOrSellTooMuch = abs(self.buy_count - self.sell_count) >= min(80, 5*self.tradeVol)
        if buyOrSellTooMuch:
            self.risk = HIGH
        return self.risk

    def simple_management_on_rtn_order(self, order, type="filled"):
        if not order:
            return

        curMidPirce = (self.last_depth.AskPrice + self.last_depth.BidPrice) / 2
        curBidPrice = self.last_depth.BidPrice
        curAskPrice = self.last_depth.AskPrice

        curCoinAQty = self.get_balance(self.coinA, "None", self.api)
        curCoinBQty =  self.get_balance(self.coinB, "None", self.api)

        # smm_pnl = curCoinBQty -self.initCoinBQty + (curCoinAQty - self.initCoinAQty) * curMidPirce
        if curCoinAQty - self.initCoinAQty > 0:
            smm_pnl = curCoinBQty -self.initCoinBQty + (curCoinAQty - self.initCoinAQty) * curBidPrice
        else:
            smm_pnl = curCoinBQty -self.initCoinBQty + (curCoinAQty - self.initCoinAQty) * curAskPrice
        delta_pnl = smm_pnl - self.smm_pnl

        qty = order.filledAmount

        if curCoinAQty - self.initCoinAQty >= self.stopTradingDeltaQty:
            self.exposure_stopbuying = True
            self.log_warn("stop buying due to too much {}".format(self.coinA))
        else:
            self.exposure_stopbuying = False
        if curCoinBQty - self.initCoinBQty >= self.stopTradingDeltaQty:
            self.exposure_stopselling = True
            self.log_warn("stop selling due to too much {}".format(self.coinB))
        else:
            self.exposure_stopselling = False



        if delta_pnl <= -0.0004 * self.tradeVol:
            self.pnl_stoploss = False
        else:
            self.pnl_stoploss = False
        if delta_pnl >= 0.0008 * self.tradeVol:
            self.pnl_stopprofit = True
        else:
            self.pnl_stopprofit = False

        self.total_amount += qty * self.round_price(self.ticker, "None", self.api, order.price)

        if type=="filled":
            # self.smm_pnl = curCoinBQty -self.initCoinBQty + (curCoinAQty - self.initCoinAQty) * curMidPirce
            if curCoinAQty - self.initCoinAQty > 0:
                self.smm_pnl = curCoinBQty -self.initCoinBQty + (curCoinAQty - self.initCoinAQty) * curBidPrice
            else:
                self.smm_pnl = curCoinBQty -self.initCoinBQty + (curCoinAQty - self.initCoinAQty) * curAskPrice
            self.all_pnl.append(delta_pnl)
            if delta_pnl > 0:
                self.all_win.append(delta_pnl)
            elif delta_pnl < 0:
                self.all_loss.append(delta_pnl)
            else:
                self.all_tie.append(delta_pnl)

            self.profitability = self.smm_pnl / self.total_amount
            num_trades = len(self.all_win) + len(self.all_loss) + len(self.all_tie)
            win_rate = len(self.all_win) / num_trades
            loss_rate = len(self.all_loss) / num_trades
            mean_win = np.mean(self.all_win)
            mean_loss = np.mean(self.all_loss)
            win2loss = mean_win / abs(mean_loss)
            mean_pnl = np.mean(self.all_pnl)
            pnl25 = np.percentile(self.all_pnl, 25)
            pnl75 = np.percentile(self.all_pnl, 75)
            median_pnl = np.percentile(self.all_pnl, 50)
            max_pnl = np.max(self.all_pnl)
            min_pnl = np.min(self.all_pnl)

            out = "+"*46 + self.ticker + "+"*46 +"\n"
            out = out + "OrderFinished:{tag}, {qty}@{price}\ncumpnl (in .00001): {cumpnl}, deltapnl: {deltapnl}, total amount: {total_amount}, profitability: {profitability}, netbuy: {net_buy}\nwin rate: {win_rate}, loss rate: {loss_rate}, mean win: {mean_win}, mean loss: {mean_loss}, win to loss: {win2loss},\nmean pnl: {mean_pnl}, median pnl: {median_pnl}, 25pnl: {pnl25}, 75pnl: {pnl75}, max pnl: {max_pnl}, min_pnl: {min_pnl}".format(
                    cumpnl = round(self.smm_pnl * 10000, 4),
                    deltapnl = round(delta_pnl * 10000, 4),
                    total_amount= round(self.total_amount, 2),
                    profitability= round(self.profitability * 10000, 4),
                    net_buy=round(self.buy_count - self.sell_count, 2),
                    win_rate= round(win_rate * 100, 2),
                    loss_rate= round(loss_rate * 100, 2),
                    mean_win= round(mean_win * 10000, 4),
                    mean_loss= round(mean_loss * 10000, 4),
                    win2loss= round(win2loss, 4),
                    mean_pnl= round(mean_pnl * 10000, 4),
                    median_pnl= round(median_pnl * 10000, 4),
                    pnl25= round(pnl25 * 10000, 4),
                    pnl75= round(pnl75 * 10000, 4),
                    max_pnl= round(max_pnl * 10000, 4),
                    min_pnl= round(min_pnl * 10000, 4),
                    tag=order.tag,
                    qty=qty,
                    price=order.price
                )
            out = out + "\n" + "+"*100

        else:
            out = "+"*46 + self.ticker + "+"*46 +"\n"
            out = out + "random log\ncumpnl (in .00001): {cumpnl}, deltapnl: {deltapnl}, total amount: {total_amount}, profitability: {profitability}, netbuy: {net_buy}".format(
                    cumpnl=round(self.smm_pnl * 10000, 4),
                    deltapnl=round(delta_pnl * 10000, 4),
                    total_amount=round(self.total_amount, 2),
                    profitability=round(self.profitability * 10000, 4),
                    net_buy=round(self.buy_count - self.sell_count, 2)
                )
            out = out + "\n" + "+"*100

        print(out)
        # self.log_info("@onRtnOrder: {}:{} {}:{}".format(
        #     self.coinA,
        #     self.get_balance(self.coinA, "None", self.api),
        #     self.coinB,
        #     self.get_balance(self.coinB, "None", self.api)
        # ))

        if self.smm_pnl < self.stopTradingLoss: #np.clip(-2* math.ceil(self.total_amount / 10000), a_max=-10, a_min=-30):
            self.log_warn("stop trading due to cum loss > -0.3 usdt")
            self.exitStrategy()

        # if datetime.datetime.now().hour >= 23:
        #     self.exitStrategy()

        return out

    def simple_store_on_rtn_order(self, order, type="filled"):
        # bid_order, ask_order, qty = self.aux_buy_sell_pair
        if order.orderType == trader.defines.openLong_limitPrice:
            # if bid_order == None and ask_order == None:
            #     self.aux_buy_sell_pair[0] = order
            # elif bid_order == None and ask_order != None:
            #     self.aux_buy_sell_pair[0] = order
            #     self.aux_buy_sell_pair[2] = min(order.filledAmount, ask_order.filledAmount)
            # elif bid_order != None:
            #     self.aux_buy_wl.append(order)
            # order.orderUpdateTime = time.time()
            self.aux_buy_queue.append(order)
        elif order.orderType == trader.defines.coverLong_limitPrice:
            # if ask_order == None and bid_order == None:
            #     self.aux_buy_sell_pair[1] = order
            # elif ask_order == None and bid_order != None:
            #     self.aux_buy_sell_pair[1] = order
            #     self.aux_buy_sell_pair[2] = min(order.filledAmount, bid_order.filledAmount)
            # elif ask_order != None:
            #     self.aux_sell_wl.append(order)
            # order.orderUpdateTime = time.time()
            self.aux_sell_queue.append(order)


    # ---------------------------------------------------------------- 交易模块 ------------------------------------------------------------------------

    def adjust_buy_order(self, bp, type, qty_mult = 1):
        # 1. 需要考虑挂出单子是否是对手价，如果是对手，将价格调整为对手价
        # 2. 如果部分成交，需要调整价格，但不补充订单量
        # 3. We need to consider whether the remaining order is valid.
        if self.stop_trading:
            return
        obp = bp
        if round(self.last_depth.BidPrice - bp, 8) < round(self.ask_bid_one_tol, 8) and bp < self.last_depth.BidPrice:
            bp = self.last_depth.BidPrice - self.get_price_miniMove(self.ticker, "None", self.api)

        # if self.last_buy_order:
        #     pass
        # if self.last_sell_order and self.last_sell_order.price <= bp:
        #     ret = type + " own bp: {} >= ap: {}".format(bp, self.last_sell_order.price)
        #     ret = ret + " obp: {}".format(obp)
        #     return ret

        if self.last_depth.AskPrice <= bp:
            ret = type + " market bp: {} >= ap: {}".format(bp, self.last_depth.AskPrice)
            ret = ret + " obp: {}".format(obp)
            return ret

        if self.last_buy_order == None:
            ret = type + " order@{}".format(bp)
            qty = self.round_volume(self.ticker, "None", self.api, self.tradeVol- self.last_buy_order_qty)
            if qty >= self.get_volume_base(self.ticker, "None", self.api):
                self.last_buy_order = self.open(
                    self.last_depth.symbol, self.last_depth.expiry, self.last_depth.API, qty*qty_mult, price=bp,
                    tag="buy@{:0d}L".format(max(math.floor((self.buy_count - self.sell_count)/self.tradeVol), 0)), isMaker = self.isMaker)
                # if self.last_buy_order:
                #     pass
                # self.log_info("Open Order {} {}@{}".format(self.last_buy_order.tag, qty, self.last_buy_order.price))

            else:
                # self.buy_count += 1
                self.last_buy_order_qty = 0
        elif self.last_buy_order and round(self.last_buy_order.price, 8) == round(bp, 8):
            ret = type + " pass@{}".format(bp)

        else:
            ret = type + " cancel order@{}".format(self.last_buy_order.price)
            # if self.last_buy_order.isCancel == True:
            #     print("[WARN:] cancel order with isCancel True")
            #     self.last_buy_order = None
            # else:
            iszero = self.trader.cancelOrder(self.last_buy_order)
            if iszero == None:
                self.iszero_buy_count += 1
                print("b"+"!"*20 + str(self.iszero_buy_count))
                if self.iszero_buy_count >= 10:
                    self.iszero_buy_count = 0
                    self.last_buy_order = None
        return ret

    def adjust_sell_order(self, ap, type, qty_mult = 1):
        if self.stop_trading:
            return
        oap = ap
        if round(ap - self.last_depth.AskPrice, 8) < round(self.ask_bid_one_tol, 8) and ap > self.last_depth.AskPrice:
            ap = self.last_depth.AskPrice + self.get_price_miniMove(self.ticker, "None", self.api)

        # if self.last_sell_order:
        #     pass
        # if self.last_buy_order and self.last_buy_order.price >= ap:
        #     ret = type + " own bp: {} >= ap: {}".format(self.last_buy_order.price, ap)
        #     ret = ret + " oap: {}".format(oap)
        #     return ret

        if self.last_depth.BidPrice >= ap:
            ret = type + " market bp: {} >= ap: {}".format(self.last_depth.BidPrice, ap)
            ret = ret + " oap: {}".format(oap)
            return ret

        if self.last_sell_order == None:
            ret = type + " order@{}".format(ap)
            qty = self.round_volume(self.ticker, "None", self.api, self.tradeVol - self.last_sell_order_qty)
            if qty >= self.get_volume_base(self.ticker, "None", self.api):
                self.last_sell_order = self.cover(
                    self.last_depth.symbol, self.last_depth.expiry, self.last_depth.API, qty*qty_mult, price=ap,
                    tag="sell@{:0d}L".format(max(math.floor((self.sell_count - self.buy_count)/self.tradeVol), 0)), isMaker=self.isMaker)
                # if self.last_sell_order:
                #     pass
                # self.log_info("Cover Order {} {}@{}".format(self.last_sell_order.tag, qty, self.last_sell_order.price))
            else:
                # self.sell_count += 1
                self.last_sell_order_qty = 0
        elif self.last_sell_order and round(self.last_sell_order.price, 8) == round(ap, 8):
            ret = type + " pass@{}".format(ap)
        else:
            ret = type + " cancel order@{}".format(self.last_sell_order.price)
            # if self.last_sell_order.isCancel == True:
            #     print("[WARN:] cancel order with isCancel True")
            #     self.last_sell_order = None
            # else:
            iszero = self.trader.cancelOrder(self.last_sell_order)
            if iszero == None:
                self.iszero_sell_count += 1
                print("b"+"!"*20 + str(self.iszero_sell_count))
                if self.iszero_sell_count >= 10:
                    self.iszero_sell_count = 0
                    self.last_sell_order = None
        return ret

    ###########################################################################
    ####                            HELPER && NECESSARY FUNCTIONS
    ###########################################################################

    def adjust_AskPrice(self):
        if self.last_depth.AskVolumes[0] >= self.tradeVol:
            return self.last_depth.Asks[0]
        elif sum(self.last_depth.AskVolumes[0:2]) >= self.tradeVol:
            ratio = self.last_depth.AskVolumes[0]/self.tradeVol
            return self.last_depth.Asks[0]*ratio + self.last_depth.Asks[1]*(1-ratio)
        else:
            ratio1 = self.last_depth.AskVolumes[0]/self.tradeVol
            ratio2 = self.last_depth.AskVolumes[1] / self.tradeVol
            return self.last_depth.Asks[0]*ratio1 + self.last_depth.Asks[1]*ratio2 + self.last_depth.Asks[2]*(1-ratio2-ratio1)

    def adjust_BidPrice(self):
        if self.last_depth.BidVolumes[0] >= self.tradeVol:
            return self.last_depth.Bids[0]
        elif sum(self.last_depth.BidVolumes[0:2]) >= self.tradeVol:
            ratio = self.last_depth.BidVolumes[0]/self.tradeVol
            return self.last_depth.Bids[0]*ratio + self.last_depth.Bids[1]*(1-ratio)
        else:
            ratio1 = self.last_depth.BidVolumes[0]/self.tradeVol
            ratio2 = self.last_depth.BidVolumes[1] / self.tradeVol
            return self.last_depth.Bids[0]*ratio1 + self.last_depth.Bids[1]*ratio2 + self.last_depth.Bids[2]*(1-ratio2-ratio1)

    def on_rtn_order(self, order):
        if order.orderStatus == trader.defines.orderCancelled:
            # self.log_info("order cancel: {}".format(order.tag))
            self.canceledFlag = True
            # this line is important since it ensures our hedge strategy works...
            if (order.tag.find("sell") >= 0):
                self.last_sell_order = None
                self.sell_count += order.filledAmount
                self.last_sell_order_qty += order.filledAmount

            elif (order.tag.find("buy") >= 0):
                self.last_buy_order = None
                self.buy_count += order.filledAmount
                self.last_buy_order_qty += order.filledAmount
            if order.filledAmount != 0:
                self.simple_management_on_rtn_order(order, "filled")
                self.simple_store_on_rtn_order(order, "filledpartial")
            else:
                if not self.pnl_stoploss:
                    if np.random.randint(100) > 80:
                        self.simple_management_on_rtn_order(order, "canceled")
                elif self.pnl_stoploss:
                    self.simple_management_on_rtn_order(order, "canceled")

        elif order.orderStatus == trader.defines.orderFilled:
            self.canceledFlag = False
            if order.orderType == trader.defines.openLong_limitPrice or order.orderType == trader.defines.openLong_buy_limit_maker:
                self.last_buy_order = None
                self.buy_count += order.filledAmount
                self.last_buy_order_qty += order.filledAmount

            elif order.orderType == trader.defines.coverLong_limitPrice or order.orderType == trader.defines.coverLong_buy_limit_maker:
                self.last_sell_order = None
                self.sell_count += order.filledAmount
                self.last_sell_order_qty += order.filledAmount

            self.simple_management_on_rtn_order(order, "filled")
            self.simple_store_on_rtn_order(order, "filled")

    def cancelAllOrders(self):
        if self.last_buy_order:
            self.trader.cancelOrder(self.last_buy_order)
        if self.last_sell_order:
            self.trader.cancelOrder(self.last_sell_order)

    def checkIfCanceled(self, order):
        if order is None:
            return
        if order.orderStatus != trader.defines.orderCancelled:
            self.trader.cancelOrder(order)
            timer = threading.Timer(1, self.checkIfCanceled, (order,))
            timer.start()

    def exitStrategy(self):
        for order in self.getOpenOrders():
            # self.log_info("cancelling order #{} {}".format(order.orderID, order.tag))
            self.trader.cancelOrder(order)

        self.stop_trading = True
        return True

    def onSendOrderFailed(self, order):
        if order.orderID and order.orderID > 0:
            self.log_info("Failed to send order {}".format(order.__dict__))

    def on_trade(self, trade):
        pass

    def on_rtn_trade(self, tradeBack):
        self.log_error("should not happen got TradeBack: ", tradeBack.__dict__)
