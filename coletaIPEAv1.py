#%% 

# Fluxograma ilustrativo para a coleta dos dados IPEA: https://github.com/abraji/APIs/blob/f595f40a5952555e23422847b4f726bcec42cbc5/ipeadata/fluxogram.png
# Bibliotecas
import ipeadatapy as ipea
import rpy2.robjects as robjects
from rpy2.robjects import pandas2ri
import pandas as pd

ipea.list_series() # Obtenção de informações sobre as séries e IDs;
ipea.themes() # Lista de temas para argumentos/filtros;
ipea.sources() # Lista de fontes para argumentos/filtros;
ipea.territories() # Lista de níveis geográficos para argumentos/filtros;
ipea.describe() # Informações sobre os metadados da série;
ipea.metadata() # Função de Busca com base em argumentos/colunas da série;
ipea.timeseries() # Função (que também utiliza argumentos *plot()) aplicada em conjunto com o código da série para obter dia, mês e ano de cada observação, além do valor da série;

'''
Silver Steps
Filtros de Municípios em Nível Geográfico;
Remoção de Colunas sem uso;
Remoção de Duplicidades;
Conversoes de formato;
Rotulação de Colunas.
'''

import ipeadatapy as ipea
ipea.list_series('PIB Municipal - preços de mercado') # PIB_IBGE_5938_37
ipea.list_series('Receita corrente - receita bruta - municipal') # RECORRM
ipea.list_series('Índice de Desenvolvimento Humano Municipal') # ADH_IDHM * Obtido em pacote do R (Nao Ha valores municipais de IDH em 2010 na biblioteca ipeadatapy)
ipea.list_series('População residente - total') # POPTOT
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
silver_PIB['PIB 2010 (R$)'] = silver_PIB['PIB 2010 (R$)'].round(2)
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
install.packages('ipeadatar')

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

df_Complete

# %%
