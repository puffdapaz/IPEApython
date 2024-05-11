#%%
import os
import pandas as pd
import ipeadatapy as ipea
import rpy2.robjects as robjects
from rpy2.robjects import pandas2ri
from rpy2.robjects.conversion import localconverter
import logging
from typing import List, Optional

# Set error capture logging
logging.basicConfig(level=logging.ERROR)

class DataProcessor:
    def __init__(self, bronze_folder: str, silver_folder: str, gold_folder: str):
        self.bronze_folder = bronze_folder
        self.silver_folder = silver_folder
        self.gold_folder = gold_folder
        self.join_list = []

    def create_folders(self) -> None:
        folders = [self.bronze_folder, self.silver_folder, self.gold_folder]
        for folder in folders:
            os.makedirs(folder, exist_ok=True)

    def bronze_fetch(self, series: str, year: int, filename: str, r_code: Optional[str] = None) -> Optional[pd.DataFrame]:
        try:
            if filename == 'IDHM_2010.csv':
                data = robjects.r(r_code)
                with localconverter(robjects.default_converter + pandas2ri.converter) as cv:
                    raw_data = cv.rpy2py(data)
                if 'date' in raw_data.columns and raw_data['date'].dtype == 'float64':
                    raw_data['date'] = pd.to_datetime(raw_data['date'], unit='D', origin='1970-01-01')
                raw_data = pd.DataFrame(raw_data)
            elif series == 'Municípios':
                raw_data = ipea.territories()
                raw_data = pd.DataFrame(raw_data)
            else:
                raw_data = ipea.timeseries(series=series, year=year)
                raw_data = pd.DataFrame(raw_data)

            self.saving_step(raw_data, self.bronze_folder, filename)
            return raw_data
        except Exception as e:
            logging.error(f"Error fetching data for {filename}: {e}")
            return None

    def saving_step(self, data: pd.DataFrame, folder: str, filename: str) -> None:
        path = os.path.join(folder, filename)
        data.to_csv(path, index=False, encoding='utf-8')

    def silver_transform(self, df: pd.DataFrame, filename: str) -> Optional[pd.DataFrame]:
        try:
            if 'IDHM_2010.csv' in filename:
                df = df.query('(uname == "Municipality") & (date == "2010-01-01")').drop(columns=['code', 'uname', 'date'])
                df = df.rename(columns={'tcode': 'CodMunIBGE', 'value': 'IDHM 2010'})
            elif filename == 'Municípios.csv':
                df = df.query('LEVEL == "Municípios"').drop(columns=['LEVEL', 'AREA', 'CAPITAL']).rename(columns={'NAME': 'Município', 'ID': 'CodMunIBGE'})
            else:
                df = df.query('NIVNOME == "Municípios"').drop(columns=['CODE', 'RAW DATE', 'YEAR', 'NIVNOME'])
                if 'PIB_2010.csv' in filename:
                    df['VALUE (R$ (mil), a preços do ano 2010)'] = df['VALUE (R$ (mil), a preços do ano 2010)'].astype(float).round(3) * 1000
                    df = df.rename(columns={'TERCODIGO': 'CodMunIBGE', 'VALUE (R$ (mil), a preços do ano 2010)': 'PIB 2010 (R$)'})
                elif 'Arrecadação_2010.csv' in filename:
                    df = df.rename(columns={'TERCODIGO': 'CodMunIBGE', 'VALUE (R$)': 'Receitas Correntes 2010 (R$)'})
                elif 'População_2010.csv' in filename:
                    df = df.rename(columns={'TERCODIGO': 'CodMunIBGE', 'VALUE (Habitante)': 'Habitantes 2010'})

            self.saving_step(df, self.silver_folder, filename)
            return df
        except Exception as e:
            logging.error(f"Error transforming data for {filename}: {e}")
            return None

    def gold_finish(self, filename: str) -> pd.DataFrame:
        merged_df = self.join_list[0]
        for df in self.join_list[1:]:
            df['CodMunIBGE'] = df['CodMunIBGE'].astype(str)
            merged_df = merged_df.merge(df, how='left', on='CodMunIBGE')

        column_order = ['CodMunIBGE', 'Município', 'Habitantes 2010', 'IDHM 2010', 'PIB 2010 (R$)', 'Receitas Correntes 2010 (R$)']
        merged_df.dropna(inplace=True)
        merged_df = merged_df.reindex(columns=column_order)
        merged_df.sort_values(by='CodMunIBGE', inplace=True)

        self.saving_step(merged_df, self.gold_folder, filename)
        return merged_df

    def process_data(self, series: str, year: int, filename: str, r_code: Optional[str] = None) -> None:
        bronze_df = self.bronze_fetch(series, year, filename, r_code)
        if bronze_df is not None:
            silver_df = self.silver_transform(bronze_df, filename)
            if silver_df is not None:
                self.join_list.append(silver_df)

def main():
    config = {
        'bronze': os.getenv('BRONZE_FOLDER', 'bronze'),
        'silver': os.getenv('SILVER_FOLDER', 'silver'),
        'gold': os.getenv('GOLD_FOLDER', 'gold')
    }
    processor = DataProcessor(config['bronze'], config['silver'], config['gold'])
    processor.create_folders()

    data_series = [
        ('PIB_IBGE_5938_37', 2010, 'PIB_2010.csv'),
        ('RECORRM', 2010, 'Arrecadação_2010.csv'),
        ('POPTOT', 2010, 'População_2010.csv'),
        ('Municípios', None, 'Municípios.csv')
    ]
    for series, year, filename in data_series:
        processor.process_data(series, year, filename)

    r_code = """
    install.packages('ipeadatar', repos='http://cran.r-project.org')
    library(ipeadatar)
    data_IDHM <- ipeadatar::ipeadata(code = 'ADH_IDHM')
    data_IDHM
    """
    processor.process_data(None, None, 'IDHM_2010.csv', r_code=r_code)

    if processor.join_list:
        clean_data = processor.gold_finish('CleanData.csv')
        print(clean_data)

if __name__ == "__main__":
    main()
# %%
