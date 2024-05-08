#%%
# Import necessary libraries
import os
import pandas as pd
import ipeadatapy as ipea
import rpy2.robjects as robjects
from rpy2.robjects import pandas2ri
from rpy2.robjects.conversion import localconverter
import logging
from typing import List, Tuple, Optional, Dict, Callable

# Set error capture logging
logging.basicConfig(level=logging.ERROR)

# Create directories (if they don't exist)
def create_folders(folders: List[str]) -> None:
    for folder in folders:
        os.makedirs(folder
                    , exist_ok=True)

# Fetch data and save it to a CSV file
def fetch_and_save_data(series: str
                        , year: int
                        , folder: str
                        , filename: str
                        , r_code: Optional[str] = None) -> Optional[pd.DataFrame]:
    try:
        # Special handling for IDHM 2010 (IPEAdataR)
        if filename == 'IDHM_2010.csv':
            data = robjects.r(r_code) # R code to fetch data
            with localconverter(robjects.default_converter + pandas2ri.converter) as cv:
                raw_data = cv.rpy2py(data) # R data conversion to pandas DataFrame
            if 'date' in raw_data.columns and raw_data['date'].dtype == 'float64':
                raw_data['date'] = pd.to_datetime(raw_data['date']
                                                  , unit='D'
                                                  , origin='1970-01-01') # Date conversion to datetime
            raw_data = pd.DataFrame(raw_data)
        elif series == 'Municípios':
            raw_data = ipea.territories()  # Cities names data fetch
            raw_data = pd.DataFrame(raw_data)
        else:
            raw_data = ipea.timeseries(series=series
                                       , year=year) # Regular fetch (IPEAdataPy)
            raw_data = pd.DataFrame(raw_data)
        
        # Save DataFrame as CSV file
        path = os.path.join(folder, filename)
        raw_data.to_csv(path, index=False, encoding='utf-8')
        return raw_data
    except Exception as e:
        logging.error(f"Error fetching data for {filename}: {e}") # Log any errors
        return None

# Transform fetched data and save it to the next tier folder
def process_dataframe(df: pd.DataFrame
                      , folder: str
                      , filename: str) -> None:
    try:
        # Special transform for IDHM 2010 (IPEAdataR)
        if 'IDHM_2010.csv' in filename:
            df = df.query('(uname == "Municipality") & (date == "2010-01-01")') # Data filter
            df = df.drop(columns=['code'
                                  , 'uname'
                                  , 'date'])
            df = df.rename(columns={'tcode': 'CodMunIBGE'
                                    , 'value': 'IDHM 2010'})
        elif filename == 'Municípios.csv':
            df = df.query('LEVEL == "Municípios"') \
                   .drop(columns=['LEVEL'
                                , 'AREA'
                                , 'CAPITAL']) \
                   .rename(columns={'NAME': 'Município'
                                  , 'ID': 'CodMunIBGE'})
            pass
        else: # Regular transform (IPEAdataPy)
            df = df.query('NIVNOME == "Municípios"') # Data filter
            df = df.drop(columns=['CODE'
                                  , 'RAW DATE'
                                  , 'YEAR'
                                  , 'NIVNOME'])
            if 'PIB_2010.csv' in filename: # Variable transforming based on filename
                df['VALUE (R$ (mil), a preços do ano 2010)'] = df['VALUE (R$ (mil), a preços do ano 2010)'].astype(float) \
                                                                                                           .round(2) * 1000
                df = df.rename(columns={'TERCODIGO': 'CodMunIBGE'
                                        , 'VALUE (R$ (mil), a preços do ano 2010)': 'PIB 2010 (R$)'})
            elif 'Arrecadação_2010.csv' in filename:
                df = df.rename(columns={'TERCODIGO': 'CodMunIBGE'
                                        , 'VALUE (R$)': 'Receitas Correntes 2010(R$)'})
            elif 'População_2010.csv' in filename:
                df = df.rename(columns={'TERCODIGO': 'CodMunIBGE'
                                        , 'VALUE (Habitante)': 'Habitantes 2010'})
        
        # Save DataFrame as CSV file
        path = os.path.join(folder, filename)
        df.to_csv(path, index=False, encoding='utf-8')
    except Exception as e:
        logging.error(f"Error transforming data for {filename}: {e}") # Log any errors

# Fetch, Transform and save Data the next tier folder
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
                                    , r_code) # Fetch and Save Data
    if bronze_df is not None:
        process_dataframe(bronze_df
                        , silver_folder
                        , filename) # Transform and Save data

# Orchestrate Data Fetching and Transforming
def main():
    # Directories parameters config
    config = {
        'bronze': os.getenv('BRONZE_FOLDER', 'bronze')
      , 'silver': os.getenv('SILVER_FOLDER', 'silver')
      , 'gold': os.getenv('GOLD_FOLDER', 'gold')
    }
    create_folders([config['bronze']
                  , config['silver']
                  , config['gold']])

    # Series parameters config
    series_data = [
        ('PIB_IBGE_5938_37', 2010, 'PIB_2010.csv')
      , ('RECORRM', 2010, 'Arrecadação_2010.csv')
      , ('POPTOT', 2010, 'População_2010.csv')
      , ('Municípios', None, 'Municípios.csv')
    ]
    for series, year, filename in series_data:
        fetch_and_process_data(series
                               , year
                               , config['bronze']
                               , config['silver']
                               , filename)

    # R Script (IPEAdataR)
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

# Execute
if __name__ == "__main__":
    main()
# %%
