import os
import pandas as pd
from core_ms.bot_event import CoinOrderEvent, PairOrderEvent

class MelanionReport:
    def __init__(self, working_directory):
        self.working_directory = working_directory

        self.pnl_file = 'pnl.csv'
        self.pnl_file_name = os.path.join(self.working_directory, self.pnl_file)
        if os.path.exists(self.pnl_file_name):
            self.pnl = pd.read_csv(self.pnl_file_name)
        else:
            self.pnl = pd.Series(dtype='float32')

    def append_pnl(self, date, value):
        self.pnl[date] = value
        self.pnl.to_csv(self.pnl_file_name)

    def update_pose(self, date, coin, value):
        self.pnl[date] = value
        self.pnl.to_csv(self.pnl_file_name)



if __name__ == '__main__':
    main()
