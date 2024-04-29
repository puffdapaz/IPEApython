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
raw_PIB.to_csv(path_PIB, index=False)

silver_PIB = pd.DataFrame(raw_PIB) \
    .query('NIVNOME == "Municípios"') \
    .drop(columns=['CODE'
                 , 'RAW DATE'
                 , 'YEAR'
                 , 'NIVNOME']) \
    .astype({'VALUE (R$ (mil), a preços do ano 2010)': int}, errors='ignore') \
    .rename(columns={'TERCODIGO' : 'CodMunIBGE'
                   , 'VALUE (R$ (mil), a preços do ano 2010)'  : 'PIB (mil R$), a preços de 2010'}) \
    .drop_duplicates(subset=['CodMunIBGE'])
path_PIB = os.path.join(silver, namefile_PIB)
silver_PIB.to_csv(path_PIB, index=False)

data_Arrecadação = ipea.timeseries(series = 'RECORRM', year = 2010)
raw_Arrecadação = pd.DataFrame(data_Arrecadação)
namefile_Arrecadação = 'Arrecadação_2010.csv'
path_Arrecadação = os.path.join(bronze, namefile_Arrecadação)
raw_Arrecadação.to_csv(path_Arrecadação, index=False)

silver_Arrecadação = pd.DataFrame(raw_Arrecadação) \
    .query('NIVNOME == "Municípios"') \
    .drop(columns=['CODE'
                 , 'RAW DATE'
                 , 'YEAR'
                 , 'NIVNOME']) \
    .astype({'VALUE (R$)': int}, errors='ignore') \
    .rename(columns={'TERCODIGO' : 'CodMunIBGE'
                   , 'VALUE (R$ (mil), a preços do ano 2010)'  : 'Receitas Correntes 2010(R$)'}) \
    .drop_duplicates(subset=['CodMunIBGE'])
path_Arrecadação = os.path.join(silver, namefile_Arrecadação)
silver_Arrecadação.to_csv(path_Arrecadação, index=False)

data_População = ipea.timeseries(series = 'POPTOT', year = 2010)
raw_População = pd.DataFrame(data_População)
namefile_População = 'População_2010.csv'
path_População = os.path.join(bronze, namefile_População)
raw_População.to_csv(path_População, index=False)

silver_População = pd.DataFrame(raw_População) \
    .query('NIVNOME == "Municípios"') \
    .drop(columns=['CODE'
                 , 'RAW DATE'
                 , 'YEAR'
                 , 'NIVNOME']) \
    .astype({'VALUE (Habitante)': int}, errors='ignore') \
    .rename(columns={'TERCODIGO' : 'CodMunIBGE'
                   , 'VALUE (Habitante)'  : 'Habitantes 2010'}) \
    .drop_duplicates(subset=['CodMunIBGE'])
path_População = os.path.join(silver, namefile_População)
silver_População.to_csv(path_População, index=False)

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
raw_IDHM.to_csv(path_IDHM, index=False)

silver_IDHM = pd.DataFrame(raw_IDHM) \
    .query('uname == "Municipality" and date == "2010-01-01"') \
    .drop(columns=['code'
                 , 'uname']) \
    .astype({'value': float}, errors='ignore') \
    .rename(columns={'tcode' : 'CodMunIBGE'
                   , 'value'  : 'IDHM 2010'}) \
    .drop_duplicates(subset=['CodMunIBGE'])
path_IDHM = os.path.join(silver, namefile_IDHM)
silver_IDHM.to_csv(path_IDHM, index=False)
# %%

df_PopPIB = df_População.merge(df_PIB,
                   how='left',
                   left_on=['TERCODIGO'],
                   right_on=['TERCODIGO'],                   
                   )
df_PopPIB