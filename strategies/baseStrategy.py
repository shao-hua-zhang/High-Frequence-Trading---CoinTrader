# encoding: UTF-8
from abc import ABCMeta, abstractmethod
import sys

# add import path
sys.path.append("../")


class baseStrategy(metaclass=ABCMeta):
    '''
    base strategy calss, pure virtual, all the strategies should be inherented from this class
    '''

    def __init__(self, path2Configure, cache):
        self.path2Configure = path2Configure
        # each strategy should have a unique strategy tag
        self.strategyTag = ""
        # trader to trade interface
        self.trader = None
        # need to fill up these info in initStrategy method
        self.workingTickers = []
        self.workingTickerIsSpot = []
        self.workingTickerExpires = []
        self.workingTickerAPIs = []

    #####################################################
    # methods need to be overwritten by user
    ######################################################
    @abstractmethod
    def initStrategy(self):
        '''
        pure virtual method need to be overwriten by user for each strategy
        used to init each strategy, will be called after loading the configure file
        :return: True for init successful, False for failed
        '''

    @abstractmethod
    def onRtnDepth(self, cur_depth):
        '''
        pure virtual method need to be overwriten by user for each strategy
        '''
        pass

    @abstractmethod
    def onRtnTrade(self, cur_trade):
        '''
        pure virtual method need to be overwriten by user for each strategy
        used to do calculations when market trades arrives
        :param cur_trade: dataframe for lastest trade info
        :return:
        '''
        pass

    @abstractmethod
    def onOrderFeedback(self, orderBack):
        '''
        pure virtual method need to be overwriten by user for each strategy
        used to do calculations when order feedback arrives
        :return:
        '''
        pass

    @abstractmethod
    def onTradeFeedback(self, tradeBack):
        '''
        pure virtual method need to be overwriten by user for each strategy
        used to do calculations when trade feedback arrives
        :return:
        '''
        pass

    @abstractmethod
    def exitStrategy(self):
        pass

    @abstractmethod
    def onSendOrderFailed(self, order):
        '''
        strategy need to handle the handshake problem here
        :param order:
        :return:
        '''
        pass

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
        return self.workingTickers, self.workingTickerIsSpot, self.workingTickerExpires, self.workingTickerAPIs

    def getAPIs(self):
        return set(self.workingTickerAPIs)

    def setTrader(self, trader):
        self.trader = trader

    def getState(self):
        return{}