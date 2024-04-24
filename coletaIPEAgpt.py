#%%
# Carregando bibliotecas
import pandas as pd
from rpy2.robjects import pandas2ri
import rpy2.robjects as robjects

# Ativa a conversão de objetos R em data frames do pandas
pandas2ri.activate()

# Código R: ipeadatar
r_code = """
# Instalando pacote 'ipeadatar' - não é necessário executar dentro do R code
# install.packages('ipeadatar')

# Carregando pacote 'ipeadatar'
library(ipeadatar)

# Recuperando dados via API
data_IDHM <- ipeadatar::ipeadata(code = 'ADH_IDHM')

# Retornando os dados
data_IDHM
"""

# Executa o código R
data_IDHM = robjects.r(r_code)

# Visualizando base de dados
data_IDHM

# df_IDHM = pd.DataFrame(data_IDHM)
with (robjects.default_converter + pandas2ri.converter).context():
  df_IDHM = robjects.conversion.get_conversion().rpy2py(data_IDHM)

df_IDHM
#%%