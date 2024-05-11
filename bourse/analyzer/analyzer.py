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


def drop_par_chunks(df, chunk_size=10000):
    def process_chunk(chunk):
        return chunk.dropna()

    chunks = []
    for i in range(0, len(df), chunk_size):
        chunks.append(process_chunk(df[i:i + chunk_size]))
    df = pd.concat(chunks)
    return df


def filter_amsterdam(df, year: str = "2020"):
    # amsterdam['last'] = amsterdam['last'].str.replace('(c)', '')
    df["last"] = df["last"].str.replace('(c)', '')
    df["last"] = df["last"].str.replace('(s)', '')
    df["last"] = df["last"].str.replace(' ', '').astype(float)

    df = drop_par_chunks(df)
    df = df.reset_index(level=1, drop=True)
    df = df.rename_axis('date', axis=0)

    return df

def store_market(website, market_name, year, last_companies_lenght):
    if website.lower() == "boursorama":

        if market_name == "peapme" and year == "2021":
            market_id = 11
            #insert dans la markets peapme
            data = {
                'id': [11],
                'name': ['peapme'],
                'alias': ['peapme']
            }
            df_peapme = pd.DataFrame(data)

            db.df_write(df_peapme, table='markets', if_exists='append', index=False)
            
      
        else:
            market_id = db.raw_query('SELECT (id) FROM markets WHERE LOWER(alias) LIKE LOWER(%s)',
                                     (market_name,))[0][0]

        logger.info("market name: %s id: %s", market_name, str(market_id))

        logger.info("START FILTERING: %s", year)
        start_time = time.time()
        df = collecte_df(directory_path="/home/bourse/data/" + year, market_name=market_name)
        if market_name == "amsterdam":
            df = filter_amsterdam(df, year)
        else:
            df = firstFiltering(df, year)
        logger.info("---filtering and collecting  %s min ---", str((time.time() - start_time) / 60))

        df['last'] = df['last'].astype(int)
        df['volume'] = df['volume'].astype(int)

        #logger.info("END filter: columns: %s", df.columns)

        df.rename(columns={'last': 'value', 'volume': 'volume', 'symbol': 'symbol', 'name': 'name'}, inplace=True)

        # Remplir la table companies avec les colonnes name, mid, symbol
        df_BY = df.groupby(by=["name", "symbol"]).mean()
        df_BY.reset_index(inplace=True)
        if (year == "2019" and (market_name == "amsterdam" or market_name == "compA" or market_name == "compB")) or (year =="2021" and market_name =="peapme"):
            df_companies = df_BY[["name", "symbol"]]
            ids = list(range(last_companies_lenght, len(df_companies) + last_companies_lenght))
            last_companies_lenght += len(df_companies)
            logger.info("Last len companies table: %s", str(last_companies_lenght))
            # df_companies['mid'] = [market_id] * len(df_companies)
            df_companies.loc[:, 'mid'] = market_id
            df_companies['id'] = ids
            #df_companies.reset_index(inplace=True)
            #df_companies.rename(columns={'index': 'id'}, inplace=True)

            #logger.info("companies groupby: %s", df_companies)
            start_time = time.time()
            db.df_write(df_companies, table='companies', if_exists='append', index=False)
            logger.info("---companies writting in db  %s min ---", str((time.time() - start_time) / 60))

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
       # db.df_write(df_stocks, table='stocks', if_exists='append', index=True, index_label='date')


        # Calculer la taille de chaque chunk
        chunk_size = len(df_stocks) // 10 + (len(df_stocks) % 10 > 0)

        # Diviser le dataframe en 10 chunks et les écrire dans la base de données
        for i in range(0, len(df_stocks), chunk_size):
            chunk = df_stocks.iloc[i:i+chunk_size]
            # Écrire chaque chunk dans la base de données
            db.df_write(chunk, table='stocks', if_exists='append', index=True, index_label='date')



        logger.info("---stocks writting in db %s min ---", str((time.time() - start_time)/60))

        return last_companies_lenght


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
    start_time = time.time()
    years = ["2019", "2020", "2021", "2022", "2023"]
    last_companies_lenght = 0
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
            #if market != "amsterdam":
           # if market == "peapme" and year == "2021":
            #if market == "compB" and year == "2019":
            logger.info("LOADING market name: %s", market)
            try:
                last_companies_lenght = store_market("boursorama", market_name=market, year=year, last_companies_lenght=last_companies_lenght)
            except ValueError :
                logger.info(e)
            except IndexError as e:
                logger.info(e)
            except Exception as e:
                logger.info(e)
    
                    
    # store_market("boursorama", "compA", year="2023")
    # store_file("amsterdam 2020-01-01 09:02:02.532411.bz2", "boursorama")
    logger.info("--- TIME TAKEN TO WRITE IN DB:  %s min ---", str((time.time() - start_time) / 60))
    
    logger.info("Done")
