#%% 

# Fluxograma ilustrativo para a coleta dos dados IPEA: https://github.com/abraji/APIs/blob/f595f40a5952555e23422847b4f726bcec42cbc5/ipeadata/fluxogram.png
import ipeadatapy as ipea
import pandas as pd

ipea.list_series() # Obtenção de informações sobre as séries e IDs;
ipea.themes() # Lista de temas para argumentos/filtros;
ipea.sources() # Lista de fontes para argumentos/filtros;
ipea.territories() # Lista de níveis geográficos para argumentos/filtros;
ipea.describe() # Informações sobre os metadados da série;
ipea.metadata() # Função de Busca com base em argumentos/colunas da série;
ipea.timeseries() # Função (que também utiliza argumentos *plot()) aplicada em conjunto com o código da série para obter dia, mês e ano de cada observação, além do valor da série;

# %%
import ipeadatapy as ipea
ipea.list_series('PIB Municipal - preços de mercado') # PIB_IBGE_5938_37
ipea.list_series('Receita corrente - receita bruta - municipal') # RECORRM
ipea.list_series('Índice de Desenvolvimento Humano Municipal') # ADH_IDHM * Obtido em pacote do R (Nao Ha valores municipais de IDH em 2010 na biblioteca ipeadatapy)
ipea.list_series('População residente - total') # POPTOT
# %%
import ipeadatapy as ipea
ipea.timeseries(series = 'PIB_IBGE_5938_37'
              , series = 'RECORRM'
              , series = 'ADH_IDHM'
              , series = 'POPTOT'
            , year = 2010
            )
# %%
import ipeadatapy as ipea
import pandas as pd

data_PIB = ipea.timeseries(series = 'PIB_IBGE_5938_37', year = 2010)
raw_PIB = pd.DataFrame(data_PIB)

data_Arrecadação = ipea.timeseries(series = 'RECORRM', year = 2010)
raw_Arrecadação = pd.DataFrame(data_Arrecadação)

data_População = ipea.timeseries(series = 'POPTOT', year = 2010)
raw_População = pd.DataFrame(data_População)

data_IDHM = ipea.timeseries(series = 'ADH_IDHM', year = 2010)
raw_IDHM = pd.DataFrame(data_IDHM)

df_PopPIB = df_População.merge(df_PIB,
                   how='left',
                   left_on=['TERCODIGO'],
                   right_on=['TERCODIGO'],                   
                   )
df_PopPIB

# %%
import pandas as pd
import ipeadatapy as ipea

# Obtenção da versão original/integral dos dados
data_População = ipea.timeseries(series='POPTOT', year=2010)
raw_População = pd.DataFrame(data_População)

'''
Filtrando Municípios em Nível Geográfico;
Remoção de Colunas;
Rótulação de Colunas.
'''
bronze_População = pd.DataFrame(raw_População) \
    .query('NIVNOME == "Municípios"') \
    .drop(columns=['CODE'
                 , 'RAW DATE'
                 , 'YEAR'
                 , 'NIVNOME']) \
    .rename(columns={'TERCODIGO'          : 'CodMunIBGE'
                   , 'VALUE (Habitante)'  : 'População'})
bronze_População
# %%

# %%
