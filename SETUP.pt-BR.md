# Guia de Configuração para Replicar o Projeto

[![en-us](https://img.shields.io/badge/lang-en--us-red.svg)](https://github.com/puffdapaz/pythonIPEA/blob/main/SETUP.en-US.md)

### Este guia fornece instruções passo a passo para configurar o ambiente do projeto e executar o script.

## Pré-requisitos
- Passo 1: **Instalar Python**<br/>
Baixe a última versão do Python no site oficial: https://www.python.org/downloads/<br/>
Execute o instalador e certifique-se de marcar a opção "**_Add Python to PATH_**". <br/>

- Passo 2: **Configurar um ambiente virtual**<br/>
Abra o prompt de comando (Windows) ou Terminal (macOS/Linux). <br/>
Navegue até a pasta do seu projeto: cd endereço/seuprojeto/diretório <br/>
Crie um ambiente virtual chamado venv: python -m venv venv <br/>
Ative o ambiente virtual: <br/>
    No Windows: .\venv\Scripts\activate <br/>
    No macOS/Linux: source venv/bin/activate <br/>

- Passo 3: **Instalar bibliotecas necessárias**<br/>
pip install:<br/>
pandas<br/>
ipeadatapy<br/>
rpy2<br/>
statsmodels<br/>
duckdb<br/>
geobr<br/>
geopandas<br/>
plotly<br/>
folium<br/>
streamlit<br/>
streamlit-folium<br/>
patsy<br/>

- Passo 4: **Instalar Software R**<br/>
Baixe a última versão do R no site oficial: https://cran.r-project.org/ <br/>
Siga as instruções de instalação para o seu sistema operacional. <br/>

- Passo 5: **Instalar pacotes necessários**<br/>
Instale IpeaDataR: install.packages("ipeadatar") <br/>

Existem arquivos [README](https://github.com/puffdapaz/pythonIPEA/blob/main/README.pt-BR.md) com suporte adicional. Não hesite em pedir ajuda. <br/>