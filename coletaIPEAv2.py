#%%
import os
import pandas as pd
import ipeadatapy as ipea
import rpy2.robjects as robjects
from rpy2.robjects import pandas2ri
from rpy2.robjects.conversion import localconverter
import logging
from typing import List, Tuple, Optional

logging.basicConfig(level=logging.ERROR)


def create_folders(folders: List[str]) -> None:
    """Create folders if they don't exist."""
    for folder in folders:
        os.makedirs(folder, exist_ok=True)


def fetch_and_save_data(series: str, year: int, folder: str, filename: str) -> Optional[pd.DataFrame]:
    """Fetch data from IPEAdatapy and save to CSV file (BRONZE)."""
    try:
        data = ipea.timeseries(series=series, year=year)
        df = pd.DataFrame(data)
        path = os.path.join(folder, filename)
        df.to_csv(path, index=False)
        return df
    except Exception as e:
        logging.error(f"Error fetching data for {series} in {year}: {e}")
        return None


def process_dataframe(df: pd.DataFrame, folder: str, filename: str, query_condition: str, drop_columns: List[str], rename_columns: dict, dtype_conversion: type, column_to_convert: str) -> None:
    """Process DataFrame and save it to the silver folder."""
    try:
        processed_df = df.query(query_condition)
        processed_df = processed_df.drop(columns=drop_columns)
        if 'PIB_2010.csv' in filename:
            processed_df['VALUE (R$ (mil), a preços do ano 2010)'] = processed_df['VALUE (R$ (mil), a preços do ano 2010)'].astype(float) * 1000
        else:
            processed_df = processed_df.astype({column_to_convert: dtype_conversion}, errors='ignore')
        processed_df = processed_df.rename(columns=rename_columns)
        processed_df = processed_df.drop_duplicates(subset=['CodMunIBGE'])
        path = os.path.join(folder, filename)
        processed_df.to_csv(path, index=False)
    except Exception as e:
        logging.error(f"Error processing data for {filename}: {e}")


def fetch_and_process_ipea_data(series: str, year: int, bronze_folder: str, silver_folder: str, filename: str) -> Optional[pd.DataFrame]:
    """Fetch and process data from IPEAdatapy (SILVER)."""
    try:
        bronze_df = fetch_and_save_data(series, year, bronze_folder, filename)
        if bronze_df is not None:
            column_to_convert = {
                'Arrecadação_2010.csv': 'VALUE (R$)',
                'População_2010.csv': 'VALUE (Habitante)',
                'PIB_2010.csv': 'VALUE (R$ (mil), a preços do ano 2010)'
            }.get(filename, 'VALUE (R$ (mil), a preços do ano 2010)')

            process_dataframe(bronze_df, silver_folder, filename, 'NIVNOME == "Municípios"',
                              ['CODE', 'RAW DATE', 'YEAR', 'NIVNOME'],
                              {'TERCODIGO': 'CodMunIBGE', 'VALUE (R$)': 'Receitas Correntes 2010(R$)',
                               'VALUE (Habitante)': 'Habitantes 2010', 'VALUE (R$ (mil), a preços do ano 2010)': 'PIB 2010(R$)'},
                              int, column_to_convert)
        return bronze_df
    except Exception as e:
        logging.error(f"Error fetching and processing IPEA data for {filename}: {e}")
        return None


def fetch_and_process_r_data(r_code: str, bronze_folder: str, filename: str, silver_folder: str) -> Optional[pd.DataFrame]:
    """Fetch and process data from IPEAdataR (BRONZE and SILVER)."""
    try:
        data = robjects.r(r_code)
        with localconverter(robjects.default_converter + pandas2ri.converter) as cv:
            raw_data = cv.rpy2py(data)
        if 'date' in raw_data.columns and raw_data['date'].dtype == 'float64':
            raw_data['date'] = pd.to_datetime(raw_data['date'], unit='D', origin='1970-01-01')
        raw_data = pd.DataFrame(raw_data)
        path = os.path.join(bronze_folder, filename)
        raw_data.to_csv(path, index=False)
        process_dataframe(raw_data, silver_folder, filename, '(uname == "Municipality") & (date == "2010-01-01")',
                          ['code', 'uname', 'date'],
                          {'tcode': 'CodMunIBGE', 'value': 'IDHM 2010'},
                          float, 'value')
        return raw_data
    except Exception as e:
        logging.error(f"Error fetching and processing data from R for {filename}: {e}")
        return None


def main():
    """Main function to orchestrate data fetching and processing."""
    # Configure folder paths
    config = {
        'bronze': os.getenv('BRONZE_FOLDER', 'bronze'),
        'silver': os.getenv('SILVER_FOLDER', 'silver'),
        'gold': os.getenv('GOLD_FOLDER', 'gold'),
    }
    # Create required folders if they don't exist
    create_folders([config['bronze'], config['silver'], config['gold']])

    # Fetch and process IPEA data
    series_data = [
        ('PIB_IBGE_5938_37', 2010, 'PIB_2010.csv'),
        ('RECORRM', 2010, 'Arrecadação_2010.csv'),
        ('POPTOT', 2010, 'População_2010.csv')
    ]
    for series, year, filename in series_data:
        fetch_and_process_ipea_data(series, year, config['bronze'], config['silver'], filename)

    # Fetch and process R data
    r_code = """
    install.packages('ipeadatar')
    library(ipeadatar)
    data_IDHM <- ipeadatar::ipeadata(code = 'ADH_IDHM')
    data_IDHM
    """
    fetch_and_process_r_data(r_code, config['bronze'], 'IDHM_2010.csv', config['silver'])


if __name__ == "__main__":
    main()
# %%
