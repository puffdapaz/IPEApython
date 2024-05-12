#%%
import ipeadatapy as ipea
import rpy2.robjects as robjects
from rpy2.robjects import pandas2ri
from rpy2.robjects.conversion import localconverter
import os as os
import pandas as pd

bronze = 'bronze'
silver = 'silver'
gold = 'gold'

# Creating folders (if don't exist)
for folder in [bronze, silver, gold]:
    os.makedirs(folder, exist_ok=True)

data_PIB = ipea.timeseries(series = 'PIB_IBGE_5938_37', year = 2010)
raw_PIB = pd.DataFrame(data_PIB)
namefile_PIB = 'PIB_2010.csv'
path_PIB = os.path.join(bronze, namefile_PIB)
raw_PIB.to_csv(path_PIB, index=False, encoding='utf-8')

silver_PIB = pd.DataFrame(raw_PIB) \
    .query('NIVNOME == "Municípios"') \
    .drop(columns=['CODE'
                 , 'RAW DATE'
                 , 'YEAR'
                 , 'NIVNOME']) \
        .rename(columns={'TERCODIGO' : 'CodMunIBGE'
                   , 'VALUE (R$ (mil), a preços do ano 2010)'  : 'PIB 2010 (R$)'}) \
        .astype({'PIB 2010 (R$)': float, 'CodMunIBGE': str}, errors='ignore') \
    .drop_duplicates(subset=['CodMunIBGE'])
silver_PIB['PIB 2010 (R$)'] = silver_PIB['PIB 2010 (R$)'].round(3)
silver_PIB['PIB 2010 (R$)'] = silver_PIB['PIB 2010 (R$)']* 1000
path_PIB = os.path.join(silver, namefile_PIB)
silver_PIB.to_csv(path_PIB, index=False, encoding='utf-8')

data_Arrecadação = ipea.timeseries(series = 'RECORRM', year = 2010)
raw_Arrecadação = pd.DataFrame(data_Arrecadação)
namefile_Arrecadação = 'Arrecadação_2010.csv'
path_Arrecadação = os.path.join(bronze, namefile_Arrecadação)
raw_Arrecadação.to_csv(path_Arrecadação, index=False, encoding='utf-8')

silver_Arrecadação = pd.DataFrame(raw_Arrecadação) \
    .query('NIVNOME == "Municípios"') \
    .drop(columns=['CODE'
                 , 'RAW DATE'
                 , 'YEAR'
                 , 'NIVNOME']) \
    .rename(columns={'TERCODIGO' : 'CodMunIBGE'
                   , 'VALUE (R$)'  : 'Receitas Correntes 2010 (R$)'}) \
    .astype({'Receitas Correntes 2010 (R$)': float, 'CodMunIBGE': str}, errors='ignore') \
    .drop_duplicates(subset=['CodMunIBGE'])
path_Arrecadação = os.path.join(silver, namefile_Arrecadação)
silver_Arrecadação.to_csv(path_Arrecadação, index=False, encoding='utf-8')

data_População = ipea.timeseries(series = 'POPTOT', year = 2010)
raw_População = pd.DataFrame(data_População)
namefile_População = 'População_2010.csv'
path_População = os.path.join(bronze, namefile_População)
raw_População.to_csv(path_População, index=False, encoding='utf-8')

silver_População = pd.DataFrame(raw_População) \
    .query('NIVNOME == "Municípios"') \
    .drop(columns=['CODE'
                 , 'RAW DATE'
                 , 'YEAR'
                 , 'NIVNOME']) \
    .rename(columns={'TERCODIGO' : 'CodMunIBGE'
                   , 'VALUE (Habitante)'  : 'Habitantes 2010'}) \
    .astype({'Habitantes 2010': int, 'CodMunIBGE': str}, errors='ignore') \
    .drop_duplicates(subset=['CodMunIBGE'])
path_População = os.path.join(silver, namefile_População)
silver_População.to_csv(path_População, index=False, encoding='utf-8')

# Conversão de RObjects em pandas DF
pandas2ri.activate()

# Coleta ipeadatar
r_code = """
install.packages('ipeadatar', repos='http://cran.r-project.org')

library(ipeadatar)

# Dados IDHM
data_IDHM <- ipeadatar::ipeadata(code = 'ADH_IDHM')
data_IDHM
"""

# Executa R
data_IDHM = robjects.r(r_code)

with localconverter(robjects.default_converter + pandas2ri.converter) as cv:
    raw_IDHM = cv.rpy2py(data_IDHM)
if 'date' in raw_IDHM.columns and raw_IDHM['date'].dtype == 'float64':
    raw_IDHM['date'] = pd.to_datetime(raw_IDHM['date'], unit='D', origin='1970-01-01')
raw_IDHM = pd.DataFrame(raw_IDHM)
namefile_IDHM = 'IDHM_2010.csv'
path_IDHM = os.path.join(bronze, namefile_IDHM)
raw_IDHM.to_csv(path_IDHM, index=False, encoding='utf-8')

silver_IDHM = pd.DataFrame(raw_IDHM) \
    .query('uname == "Municipality" and date == "2010-01-01"') \
    .drop(columns=['code'
                 , 'uname'
                 , 'date']) \
    .rename(columns={'tcode' : 'CodMunIBGE'
                   , 'value'  : 'IDHM 2010'}) \
    .astype({'IDHM 2010': float, 'CodMunIBGE': str}, errors='ignore') \
    .drop_duplicates(subset=['CodMunIBGE'])
path_IDHM = os.path.join(silver, namefile_IDHM)
silver_IDHM.to_csv(path_IDHM, index=False, encoding='utf-8')


data_Municípios = ipea.territories()
raw_Municípios = pd.DataFrame(data_Municípios)
namefile_Municípios = 'Municípios.csv'
path_Municípios = os.path.join(bronze, namefile_Municípios)
raw_Municípios.to_csv(path_Municípios, index=False, encoding='utf-8')

silver_Municípios = pd.DataFrame(raw_Municípios) \
    .query('LEVEL == "Municípios"') \
    .drop(columns=['LEVEL'
               ,   'AREA'
               ,   'CAPITAL']) \
    .rename(columns={'NAME' : 'Município'
                   , 'ID'  : 'CodMunIBGE'}) \
    .drop_duplicates(subset=['CodMunIBGE'])
path_Municípios = os.path.join(silver, namefile_Municípios)
silver_Municípios.to_csv(path_Municípios, index=False, encoding='utf-8')

df_PopPIB = silver_População.merge(silver_PIB,
                   how='left',
                   left_on=['CodMunIBGE'],
                   right_on=['CodMunIBGE'],                   
                   )

df_PopPIBArrec = df_PopPIB.merge(silver_Arrecadação,
                                 how='left',
                                 left_on=['CodMunIBGE'],
                                 right_on=['CodMunIBGE'],
                                 )

df_Variables = df_PopPIBArrec.merge(silver_IDHM,
                                   how='left',
                                   left_on=['CodMunIBGE'],
                                   right_on=['CodMunIBGE'],
                                   )

df_Complete = df_Variables.merge(silver_Municípios,
                                   how='left',
                                   left_on=['CodMunIBGE'],
                                   right_on=['CodMunIBGE'],
                                   )

df_Complete.dropna(inplace=True)
df_Complete = df_Complete.reindex(columns=['CodMunIBGE', 'Município', 'Habitantes 2010', 'IDHM 2010', 'PIB 2010 (R$)', 'Receitas Correntes 2010 (R$)', 'Carga Tributária'])
df_Complete.sort_values(by='CodMunIBGE', inplace=True)
df_Complete['Carga Tributária'] = df_Complete['Receitas Correntes 2010 (R$)'] / df_Complete['PIB 2010 (R$)'].astype(float)

namefile_clean_data = 'CleanData.csv'
path_clean_data = os.path.join(gold, namefile_clean_data)
df_Complete.to_csv(path_clean_data, index=False, encoding='utf-8')
df_Complete
# %%
