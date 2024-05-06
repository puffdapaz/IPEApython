import os
import pandas as pd
import ipeadatapy as ipea
import rpy2.robjects as robjects
from rpy2.robjects import pandas2ri
from rpy2.robjects.conversion import localconverter
import logging
from typing import List, Tuple, Optional, Dict, Callable

logging.basicConfig(level=logging.ERROR)

def create_folders(folders: List[str]) -> None:
    for folder in folders:
        os.makedirs(folder
                    , exist_ok=True)

def fetch_and_save_data(series: str
                        , year: int
                        , folder: str
                        , filename: str
                        , r_code: Optional[str] = None) -> Optional[pd.DataFrame]:
    try:
        if filename == 'IDHM_2010.csv':
            data = robjects.r(r_code)
            with localconverter(robjects.default_converter + pandas2ri.converter) as cv:
                raw_data = cv.rpy2py(data)
            if 'date' in raw_data.columns and raw_data['date'].dtype == 'float64':
                raw_data['date'] = pd.to_datetime(raw_data['date']
                                                  , unit='D'
                                                  , origin='1970-01-01')
            raw_data = pd.DataFrame(raw_data)
        else:
            raw_data = ipea.timeseries(series=series
                                       , year=year)
            raw_data = pd.DataFrame(raw_data)
        
        path = os.path.join(folder, filename)
        raw_data.to_csv(path, index=False)
        return raw_data
    except Exception as e:
        logging.error(f"Error fetching data for {filename}: {e}")
        return None

def process_dataframe(df: pd.DataFrame
                      , folder: str
                      , filename: str) -> None:
    try:
        if 'IDHM_2010.csv' in filename:
            df = df.query('(uname == "Municipality") & (date == "2010-01-01")')
            df = df.drop(columns=['code'
                                  , 'uname'
                                  , 'date'])
            df = df.rename(columns={'tcode': 'CodMunIBGE'
                                    , 'value': 'IDHM 2010'})
        else:
            df = df.query('NIVNOME == "Municípios"')
            df = df.drop(columns=['CODE'
                                  , 'RAW DATE'
                                  , 'YEAR'
                                  , 'NIVNOME'])
            if 'PIB_2010.csv' in filename:
                df['VALUE (R$ (mil), a preços do ano 2010)'] = df['VALUE (R$ (mil), a preços do ano 2010)'].astype(float) * 1000
                df = df.rename(columns={'TERCODIGO': 'CodMunIBGE'
                                        , 'VALUE (R$ (mil), a preços do ano 2010)': 'PIB 2010(R$)'})
            elif 'Arrecadação_2010.csv' in filename:
                df = df.rename(columns={'TERCODIGO': 'CodMunIBGE'
                                        , 'VALUE (R$)': 'Arrecadação 2010(R$)'})
            elif 'População_2010.csv' in filename:
                df = df.rename(columns={'TERCODIGO': 'CodMunIBGE'
                                        , 'VALUE (Habitante)': 'Habitantes 2010'})
        
        path = os.path.join(folder, filename)
        df.to_csv(path, index=False)
    except Exception as e:
        logging.error(f"Error processing data for {filename}: {e}")

def fetch_and_process_data(series: str
                           , year: int
                           , bronze_folder: str
                           , silver_folder: str
                           , filename: str
                           , r_code: Optional[str] = None) -> None:
    bronze_df = fetch_and_save_data(series
                                    , year
                                    , bronze_folder
                                    , filename
                                    , r_code)
    if bronze_df is not None:
        process_dataframe(bronze_df
                          , silver_folder
                          , filename)

def main():
    config = {
        'bronze': os.getenv('BRONZE_FOLDER', 'bronze'),
        'silver': os.getenv('SILVER_FOLDER', 'silver'),
        'gold': os.getenv('GOLD_FOLDER', 'gold'),
    }
    create_folders([config['bronze']
                    , config['silver']
                    , config['gold']])

    series_data = [
        ('PIB_IBGE_5938_37', 2010, 'PIB_2010.csv'),
        ('RECORRM', 2010, 'Arrecadação_2010.csv'),
        ('POPTOT', 2010, 'População_2010.csv')
    ]
    for series, year, filename in series_data:
        fetch_and_process_data(series
                               , year
                               , config['bronze']
                               , config['silver']
                               , filename)

    r_code = """
    install.packages('ipeadatar', repos='http://cran.r-project.org')
    library(ipeadatar)
    data_IDHM <- ipeadatar::ipeadata(code = 'ADH_IDHM')
    data_IDHM
    """
    fetch_and_process_data(None
                           , None
                           , config['bronze']
                           , config['silver']
                           , 'IDHM_2010.csv'
                           , r_code=r_code)

if __name__ == "__main__":
    main()
