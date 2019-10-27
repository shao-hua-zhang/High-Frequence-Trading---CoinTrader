# encoding: UTF-8

import sys
sys.path.append("../trader")
sys.path.append("../strategies")
sys.path.append("../adapters")
sys.path.append("../adapters/rest")
sys.path.append("../adapters/websocket")
import signal
import time
import traderInterface


trader = traderInterface.traderInterface()

class GracefulKiller:
  kill_now = False
  def __init__(self):
    signal.signal(signal.SIGINT, self.exit_gracefully)
    signal.signal(signal.SIGTERM, self.exit_gracefully)

  def exit_gracefully(self, signum, frame):
      print("exit system",file=sys.stdout)
      self.kill_now = True
      if trader:
          trader.exitSystem()
          sys.stdout.flush()
          sys.stderr.flush()
          time.sleep(3)


killer = GracefulKiller()

if __name__ == "__main__":
    timeCounter = 0
    if trader.initTraders():
        if trader.initStrategies():
            if trader.linkStrategies():
                if trader.linkTrader(trader):
                    # begin each trade adatpters
                    time.sleep(1)
                    trader.startAdapters()
                    # begin timer work
                    while True:
                        if killer.kill_now:
                            # exit system
                            break
                        # other works will be added in the following part
                        if timeCounter % 30 == 0:
                            # query positions
                            try:
                                trader.queryPositions()
                            except Exception as e:
                                print("exception", str(e))

                            # print infos
                            trader.printPositionInfo()
                        if timeCounter % 30 == 0:
                            # flush stderr
                            sys.stdout.flush()
                            sys.stderr.flush()

                        # increase time counter
                        timeCounter += 1
                        time.sleep(1)

                    time.sleep(30)