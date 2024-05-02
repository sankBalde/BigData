import logging
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
import time

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

db = tsdb.TimescaleStockMarketModel('bourse', 'ricou', 'db', 'monmdp')  # inside docker


# db = tsdb.TimescaleStockMarketModel('bourse', 'ricou', 'localhost', 'monmdp') # outside docker


def collecte_df(directory_path: str = "../boursorama/2023/", market_name: str = "compA") -> pd.DataFrame:
    dfglob = glob.glob1(directory_path, market_name + "*")
    files_path = [directory_path + "/" + file for file in dfglob]
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
        market_id = db.raw_query('SELECT (id) FROM markets WHERE LOWER(alias) LIKE LOWER(%s)',
                                 (market_name ,))[0][0]
        logger.info("market name id: %s", str(market_id))

        logger.info("START FILTERING: ")
        start_time = time.time()
        df = collecte_df(directory_path="/home/bourse/data/" + year, market_name=market_name)

        df = firstFiltering(df, year)
        logger.info("---filtering and collecting  %s min ---", str((time.time() - start_time) / 60))

        df['last'] = df['last'].astype(int)
        df['volume'] = df['volume'].astype(int)

        logger.info("END filter: columns: %s", df.columns)

        df.rename(columns={'last': 'value', 'volume': 'volume', 'symbol': 'symbol', 'name': 'name'}, inplace=True)

        # Remplir la table companies avec les colonnes name, mid, symbol
        df_BY = df.groupby(by=["name", "symbol"]).mean()
        df_BY.reset_index(inplace=True)
        df_companies = df_BY[["name", "symbol"]]
        # df_companies['mid'] = [market_id] * len(df_companies)
        df_companies.loc[:, 'mid'] = market_id
        #logger.info("companies groupby: %s", df_companies)
        start_time = time.time()
        db.df_write(df_companies, table='companies', if_exists='fail', index=False)
        logger.info("---companies writting in db  %s min ---", str((time.time() - start_time)/ 60))
        # Remplir la table stocks avec date, cid, value, volume
        df_stocks = df.loc[:, ["value", "volume"]]

        # Récupérer un mapping entre les noms des entreprises et leurs ID
        company_names = df['name'].unique()
        company_id_mapping = {name: db.search_company_id(name) for name in company_names}
        # Assigner les ID d'entreprise dans le dataframe en utilisant le mapping
        df_stocks['cid'] = df['name'].apply(lambda x: company_id_mapping[x])

        #logger.info("Df rename for stocks: %s", df_stocks)
        start_time = time.time()
        # Écrire le dataframe dans la table "stocks"
        db.df_write(df_stocks, table='stocks', if_exists='fail', index=True, index_label='date')
        logger.info("---stocks writting in db %s min ---", str((time.time() - start_time)/60))


def store_file(name, website):
    if db.is_file_done(name):
        return
    if website.lower() == "boursorama":
        logger.info("Yes bourse")
        try:
            df = pd.read_pickle("/home/bourse/data/" + name)  # is this dir ok for you ?
        except:
            year = name.split()[1].split("-")[0]
            df = pd.read_pickle("/home/bourse/data/" + year + "/" + name)
            # db.df_write(df, )
            logger.info(df)
        # to be finished


if __name__ == '__main__':
    years = ["2019", "2020", "2021", "2022", "2023"]
    for year in years:
        list_files = os.listdir("/home/bourse/data/" + year)
        market_list = []
        for file_name in list_files:
            market_name = file_name.split()[0]
            if market_name not in market_list:
                market_list.append(market_name)
        logger.info("Year : %s", year)
        logger.info("liste markets : %s", str(market_list))
        for market in market_list:
            if market != "amsterdam":
                logger.info("LOADING market name: %s", market)
                try:
                    store_market("boursorama", market_name=market, year=year)
                except ValueError :
                    print()
                except IndexError as e:
                    logger.info(e)
    # store_market("boursorama", "compA", year="2023")
    # store_file("amsterdam 2020-01-01 09:02:02.532411.bz2", "boursorama")
    logger.info("Done")

