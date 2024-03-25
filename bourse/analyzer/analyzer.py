import pandas as pd
import numpy as np
import sklearn

import timescaledb_model as tsdb

db = tsdb.TimescaleStockMarketModel('bourse', 'ricou', 'db', 'monmdp')        # inside docker
#db = tsdb.TimescaleStockMarketModel('bourse', 'ricou', 'localhost', 'monmdp') # outside docker

def store_file(name, website):
    if db.is_file_done(name):
        return
    if website.lower() == "boursorama":
        print("Yes bourse")
        try:
            df = pd.read_pickle("/home/bourse/data/" + name)  # is this dir ok for you ?
        except:
            year = name.split()[1].split("-")[0]
            df = pd.read_pickle("/home/bourse/data/" + year + "/" + name)
            print(df)
        # to be finished


if __name__ == '__main__':

    store_file("amsterdam 2020-01-01 09:02:02.532411.bz2", "boursorama")
    print("Done")
