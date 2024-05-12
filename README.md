# Refazendo o projeto de Artigo (TCC Graduação): Obtenção, Manipulação e Análise dos dados utilizando python
[Artigo Integral](https://github.com/puffdapaz/TCC/blob/66a3e445755dc30225056ef4bb92fabd85f85d14/Impacto%20da%20receita%20tribut%C3%A1ria%20no%20desenvolvimento%20econ%C3%B4mico%20e%20social.%20um%20estudo%20nos%20munic%C3%ADpios%20brasileiros.pdf)

## Impacto da receita tributária no desenvolvimento econômico e social: um estudo nos municípios brasileiros
- Municípios Brasileiros com mais de 100.000 habitantes em 2010 (diante do alto índice de informações faltantes em delimitação mais ampla);
- IDHM 2010;
- População 2010;
- Arrecadação Municipal 2010;
- Produto Interno Bruto Municipal 2010;
- Arrecadação 2010 / PIB 2010 = Carga Tributária 2010;
- PIB 2010 / População 2010 = PIB per capita 2010;

### Incrementos
- Expandir estudo para todos os municípios possíveis;
- Refazer estudo com valores censitarios (2022) atualizados;
- Oter dados via API;
- Incluir painel/visuais;
- Documentar e publicar.

## Métodos
### Model Summary (R, R², Adjusted R², Std error Estimate, Durbin-Watson)
- *Predictors: (Constant), Carga Tributária;*
- *Predictors: (Constant), Carga Tributária, Arrecadação 2010;*
- *Predictors: (Constant), Carga Tributária, Arrecadação 2010, PIB 2010;*
- *Dependent Variable: IDHM 2010.*
### ANOVA (Regression, Residual, Total)
- *Predictors: (Constant), Carga Tributária;*
- *Predictors: (Constant), Carga Tributária, Arrecadação 2010;*
- *Predictors: (Constant), Carga Tributária, Arrecadação 2010, PIB 2010;*
- *Dependent Variable: IDHM 2010.*

## Fontes de Arquivos
Por Município em 2010: http://www.ipeadata.gov.br/api/odata4/.
- [Código IBGE x Nome Município](https://geoftp.ibge.gov.br/organizacao_do_territorio/estrutura_territorial/divisao_territorial/2022/DTB_2022.zip);
- [IDH e População](https://basedosdados.org/dataset/cbfc7253-089b-44e2-8825-755e1419efc8?table=ec5fb3d1-fa98-4ab3-8a02-4b9950048a83);
- [PIB]();
- [Arrecadação]().

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