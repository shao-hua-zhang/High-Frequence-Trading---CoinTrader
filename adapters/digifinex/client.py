# -*- coding: utf-8 -*-
import time
import hashlib
import requests
from copy import deepcopy
from trader.coinUtilities import getCurrentTimeString

class dgf_client(object):

    def __init__(self, url, apikey, secretkey):
        self._url = url
        self._apikey = apikey
        self._secretkey = secretkey

    def build_sign(self, param):
        params = deepcopy(param)
        params["apiSecret"] = self._secretkey
        data = ''
        for key in sorted(params.keys()):
            data += str(params[key])
        return hashlib.md5(data.encode("utf8")).hexdigest()

    def http_request(self, path, params, method="GET"):

        url =  self._url + path

        headers = {
            "Connection": 'close',
            "user-agent": 'python-requests/2.18.4'
        }

        if method == "POST":
            payload = "&".join(["{}={}".format(key, params[key]) for key in params.keys()])
            headers["content-type"] = "application/x-www-form-urlencoded"
        else:
            payload = ""

        try:
            if method == "POST":
                response = requests.request(method, url, data=payload, headers=headers, timeout=5)
            else:
                response = requests.get(url, timeout=2, headers=headers)
        except Exception as e:
            print("[WARN] {} Digifinex request timeout".format(getCurrentTimeString()))
            return None
        try:
            ret_data = response.json()
        except Exception as e:
            print("[WARN] {} Digifinex inside error".format(getCurrentTimeString()))
            return None

        response.close()

        try:
            if ret_data["code"] == 0:
                return ret_data
            else:
                if ret_data["code"] == 20011:
                    print("[ERR] {} Digifinex {} balance not enough".format(getCurrentTimeString(), params["symbol"]))
                else:
                    print("[ERR] {} Digifinex request error, msg={}".format(getCurrentTimeString(), ret_data["code"]))
                return None
        except Exception as e:
            print("[WARN] {} Digifinex ret_data parse error, {}".format(getCurrentTimeString(), ret_data))
            return None

    def depth(self, symbol):
        depth_resource = "/v2/depth?"
        param = {}
        param["symbol"] = symbol
        param["apiKey"] = self._apikey
        param["timestamp"] = int(time.time())
        param["sign"] = self.build_sign(param)
        querys = "&".join(["{}={}".format(key, param[key]) for key in param.keys()])
        depth_resource += querys
        return self.http_request(depth_resource, {})

    def deals(self, symbol):
        deals_resource = "/v2/trade_detail?"
        param = {}
        param["symbol"] = symbol
        param["apiKey"] = self._apikey
        param["timestamp"] = int(time.time())
        param["sign"] = self.build_sign(param)
        querys = "&".join(["{}={}".format(key, param[key]) for key in param.keys()])
        deals_resource += querys
        return self.http_request(deals_resource, {})

    def balance(self):
        balance_resource = "/v2/myposition?"
        param = {}
        param["apiKey"] = self._apikey
        param["timestamp"] = int(time.time())
        param["sign"] = self.build_sign(param)
        querys = "&".join(["{}={}".format(key, param[key]) for key in param.keys()])
        balance_resource += querys
        return self.http_request(balance_resource, {})

    def input_order(self, iparam):
        trade_resource = "/v2/trade"
        param = deepcopy(iparam)
        param["apiKey"] = self._apikey
        param["timestamp"] = int(time.time())
        param["sign"] = self.build_sign(param)
        return self.http_request(trade_resource, param, "POST")

    def cancel_order(self, iparam):
        cancel_resource = "/v2/cancel_order"

        param = deepcopy(iparam)
        param["apiKey"] = self._apikey
        param["timestamp"] = int(time.time())
        param["sign"] = self.build_sign(param)
        return self.http_request(cancel_resource, param, "POST")

    def open_orders(self, symbol):
        open_orders_resource = "/v2/open_orders"
        param = {}
        param["symbol"] = symbol
        param["apiKey"] = self._apikey
        param["timestamp"] = int(time.time())
        param["sign"] = self.build_sign(param)
        querys = "&".join(["{}={}".format(key, param[key]) for key in param.keys()])
        open_orders_resource += querys
        return self.http_request(open_orders_resource, {})

    def batch_orders(self, ordernos):
        batch_orders_resource = "/v2/order_info?"

        if len(ordernos) > 20:
            print("[WARN] {} Digifinex query orders cross 20 limit".format(getCurrentTimeString()))

        param = {}
        param["order_id"] = ",".join(ordernos)
        param["apiKey"] = self._apikey
        param["timestamp"] = int(time.time())
        param["sign"] = self.build_sign(param)
        querys = "&".join(["{}={}".format(key, param[key]) for key in param.keys()])
        batch_orders_resource += querys
        return self.http_request(batch_orders_resource, {})

