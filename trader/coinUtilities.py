# encoding: UTF-8
import datetime

# this file will define utility functions

######################################
# Moving window on time calculation
######################################
class MovingWindow (object):
    def __init__(self, windowSize):
        self.windowSize = float(windowSize)
        self.SpreadWindow = []
        self.SpreadCount = 0
        self.Average = 0.

    def update(self, spread):
        if self.windowSize == 0:
            return
        self.Average += (spread / self.windowSize)
        self.SpreadWindow.append(spread)
        self.SpreadCount += 1
        if self.SpreadCount > self.windowSize:
            expired_spread = self.SpreadWindow.pop(0)
            self.SpreadCount -= 1
            self.Average -= (expired_spread / self.windowSize)
    def size(self):
        return self.SpreadCount
    def full(self):
        return self.SpreadCount >= self.windowSize
    def averageValue(self):
        return self.Average

############################################
# time operations
############################################
def getCurrentTimeString():
    return datetime.datetime.now().strftime("%H:%M:%S")
