import pandas as pd
import numpy as np
import sklearn
import os
import datetime
import dateutil
import tarfile
import pandas as pd
import pandas.io.sql as sqlio
import glob
import sys
import timescaledb_model as tsdb

db = tsdb.TimescaleStockMarketModel('bourse', 'ricou', 'db', 'monmdp')        # inside docker
#db = tsdb.TimescaleStockMarketModel('bourse', 'ricou', 'localhost', 'monmdp') # outside docker


def collecte_df(directory_path: str = "../boursorama/2023/", market_name: str = "compA") -> pd.DataFrame:
    dfglob = glob.glob1(directory_path, market_name + "*")
    files_path = [directory_path +"/"+ file for file in dfglob]
    df = pd.concat(
        {dateutil.parser.parse(f.split(market_name)[1].split(".bz2")[0]): pd.read_pickle(f) for f in files_path})
    return df


def convertion(x):
    try:

        return float(x)
    except:
        return float(x.replace(' ', ''))


def firstFiltering(df, year: str = "2023"):
    df["last"] = [str(x).split("(c)")[0] for x in df["last"]]
    df["last"] = [str(x).split("(s)")[0] for x in df["last"]]
    df['last'] = df['last'].apply(convertion)
    # on retire l'index qui commence par les noms d'entreprises pour finir par leur symbole
    df = df.reset_index(level=1, drop=True)
    df = df.dropna()
    # on nomme l'index des dates puis on passe la colonne des symboles en index
    df = df.rename_axis('date', axis=0)
    # on retire les jours de congés dont les données ne devraient pas exister
    df.sort_index(inplace=True)
    day_off = [year + '-01-01', year + '-03-30', year + '-04-02', year + '-05-01', year + '-12-25', year + '-12-26']
    for d in day_off:
        if d in df.index:
            df.drop(df.loc[d].index, inplace=True)
    # on passe la colonne des symboles en index et on la met en premier
    # df = df.set_index('symbol', append=True)
    # df = df.swaplevel(0,1).sort_index()
    return df



def store_market(website, market_name, year):
    if website.lower() == "boursorama":
        market_id = db.raw_query('SELECT (id) FROM markets WHERE LOWER(name) LIKE LOWER(%s)',
                                 ('%' + market_name + '%',))[0][0]

        print("START FILTERING: ")
        df = collecte_df(directory_path="/home/bourse/data/" + year, market_name=market_name)
        df = firstFiltering(df, year)

        df['last'] = df['last'].astype(int)
        df['volume'] = df['volume'].astype(int)

        print("END filter: columns: ", df.columns)

        df.rename(columns={'last': 'value', 'volume': 'volume', 'symbol': 'symbol', 'name': 'name'}, inplace=True)

        # Remplir la table companies avec les colonnes name, mid, symbol
        df_BY = df.groupby(by=["name", "symbol"]).mean()
        df_BY.reset_index(inplace=True)
        df_companies = df_BY[["name", "symbol"]]
        df_companies['mid'] = [market_id] * len(df_companies)
        print("companies groupby: ", df_companies)
        db.df_write(df_companies, table='companies', if_exists='append', index=False)

        # Remplir la table stocks avec date, cid, value, volume
        df_stocks = df.loc[:, ["value", "volume"]]

        # Récupérer un mapping entre les noms des entreprises et leurs ID
        company_names = df['name'].unique()
        company_id_mapping = {name: db.search_company_id(name) for name in company_names}
        # Assigner les ID d'entreprise dans le dataframe en utilisant le mapping
        df_stocks['cid'] = df['name'].apply(lambda x: company_id_mapping[x])

        print("Df rename for stocks: ", df_stocks)

        # Écrire le dataframe dans la table "stocks"
        db.df_write(df_stocks, table='stocks', if_exists='append', index=True, index_label='date')


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
            #db.df_write(df, )
            print(df)
        # to be finished


if __name__ == '__main__':
    store_market("boursorama", "compA", year="2023")
    #store_file("amsterdam 2020-01-01 09:02:02.532411.bz2", "boursorama")
    print("Done")
