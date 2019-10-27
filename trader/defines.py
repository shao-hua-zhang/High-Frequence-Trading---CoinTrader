# encoding: UTF-8

# do NOT use 0 in this define

traderConfigureFile = "../config/config.ini"

# defines api tags
okexAPI_web_future = "okexAPI_web_future"
okexAPI_web_spot = "okexAPI_web_spot"
HuobiSpi_spot_web = "HuobiSpi_spot_web"
BinanceSpi_spot_web = "BinanceSpi_spot_web"
BitfinexSpi_spot_web = "BitfinexSpi_spot_web"
HuobiNewSpi_spot_web = "HuobiNewSpi_spot_web"
Digifinex_spot_web = "DgfSpi_spot_web"
HuobiSpi_future_web = "HuobiSpi_future_web"
BitmexSpi_future_web = "BitmexSpi_future_web"
All_API_List = [okexAPI_web_future, okexAPI_web_spot, HuobiSpi_spot_web, BinanceSpi_spot_web, \
                BitfinexSpi_spot_web, HuobiNewSpi_spot_web, Digifinex_spot_web, HuobiSpi_future_web, \
                BitmexSpi_future_web]

# define order types
openLong_limitPrice = 21
openShort_limitPrice = 22
coverLong_limitPrice = 23
coverShort_limitPrice = 24
openLong_marketPrice = 31
openShort_marketPrice = 32
coverLong_marketPrice = 33
coverShort_marketPrice = 34
'''this part for ctp future only'''
coverTodayLong_limitPrice = 25
coverTodayShort_limitPrice = 26
coverYestodayLong_limitPrice = 27
coverYestodayShort_limitPrice = 28
openLong_FAK = 31
openShort_FAK = 32
coverLong_FAK = 33
coverShort_FAK = 34
openLong_FOK = 31
openShort_FOK = 32
coverLong_FOK = 33
coverShort_FOK = 34
coverTodayLong_FAK = 35
coverTodayShort_FAK = 36
coverYestodayLong_FAK = 37
coverYestodayShort_FAK = 38
coverTodayLong_FOK = 35
coverTodayShort_FOK = 36
coverYestodayLong_FOK = 37
coverYestodayShort_FOK = 38

# IOC order, supported in Huobi API
openLong_buy_ioc = 35
coverLong_sell_ioc = 36
# maker order, supported in Huobi and Dgf API
openLong_buy_limit_maker = 37
coverLong_sell_limit_maker = 38

# define trade types
tradeBid = 51
tradeAsk = 52

# define order status
orderInit = 1
orderPending = 2
orderFilling = 3
orderFilled = 4
orderCancelled = 5
orderRejected = 6
orderUnknown = -1
orderCancelFailed = -2

# define flag for bitmex api
bitmexForever = "forever"
bitmexFuture = "month"