# -*- coding: utf-8 -*-
import datetime
import bitmex
import json
import warnings
warnings.filterwarnings('ignore')

if __name__ == "__main__":

    # apikey = "YYKrem7cHwh7omuA57VWKlZz"
    # apisecret = "PnCqijDk4VvX32UsdWilncmsWsjOSTMbXIOEqgX-20I7H7s_"

    apikey = "_RtZ1y2iH9dCR9BBfyUTuK_k"
    apisecret = "29SG_2k3cQjeBgxo5NX4TCEhzBrToNWfb2RFVyGK9VOrJJlo"
    client = bitmex.bitmex(test=True, api_key=apikey, api_secret=apisecret)
    # mk
    # result = client.Instrument.Instrument_get(filter=json.dumps({'symbol': 'XRPU18'})).result()
    # result = client.Instrument.Instrument_get(filter=json.dumps({'symbol': 'XRPU18'})).result()
    # result = client.Instrument.Instrument_get(filter=json.dumps({'symbol': 'XRPU18'})).result()
    # result = client.Trade.Trade_get(filter=json.dumps({'symbol': 'XRPU18'}), count=10, reverse=True).result()
    # print(result)
    # result = len(client.OrderBook.OrderBook_getL2(symbol='XRPU18', depth=0).result()[0])
    # print(result)


    #
    symbol = "XRPU18"
    price = 0.000001
    quantity = 100000

    try:
        result = client.Order.Order_new(symbol=symbol, orderQty=quantity, price=price).result()
    except Exception as e:
        print(e)
    print(result)

    # print(client.Order.Order_cancel(orderID='94ed4e64-64ea-672c-80d8-d45e60e8003e').result())
    #     # print(client.Order.Order_cancelAll().result())
    # print(client.Order.Order_cancel(orderID='94ed4e64-64ea-672c-80d8-d45e60e8003e').result())
    # print(client.Position.Position_get(filter=json.dumps({'symbol': 'TRXU18'})).result())
    # print(client.Position.Position_isolateMargin(symbol='TRXU18', enabled=True).result())





    # result = client.OrderBook.OrderBook_getL2(symbol='XRPU18', depth=0).result()
    # print(result)

    # order
