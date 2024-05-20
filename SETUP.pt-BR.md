# Guia de Configuração para Replicar o Projeto

[![en-us](https://img.shields.io/badge/lang-en--us-red.svg)](https://github.com/puffdapaz/pythonIPEA/blob/main/SETUP.en-US.md)

### Este guia fornece instruções passo a passo para configurar o ambiente do projeto e executar o script.

## Pré-requisitos
- Passo 1: **Instalar Python** </br>
Baixe a última versão do Python no site oficial: https://www.python.org/downloads/</br>
Execute o instalador e certifique-se de marcar a opção "**_Add Python to PATH_**". </br>

- Passo 2: **Configurar um ambiente virtual** </br>
Abra o prompt de comando (Windows) ou Terminal (macOS/Linux).</br>
Navegue até a pasta do seu projeto: cd endereço/seuprojeto/diretório </br>
Crie um ambiente virtual chamado venv: python -m venv venv </br>
Ative o ambiente virtual: </br>
    No Windows: .\venv\Scripts\activate </br>
    No macOS/Linux: source venv/bin/activate </br>

- Passo 3: **Instalar bibliotecas necessárias** </br>
Instale Pandas: pip install pandas==1.4.1 </br>
Instale IpeaDataPy: pip install ipeadatapy==0.1.0 </br>
Instale Rpy2: pip install rpy2==3.4.5 </br>
Instale Matplotlib: pip install matplotlib==3.5.1 </br>
Instale Seaborn: pip install seaborn==0.11.2 </br>
Instale StatsModels: pip install statsmodels==0.13.2 </br>
Instale Patsy: pip install patsy==0.5.2 </br>

- Passo 4: **Siga as instruções do script** </br>
Entenda o script lendo-o com atenção. Execute o script digitando coletaIPEAv3.py no terminal. </br>

Existem arquivos [README](https://github.com/puffdapaz/pythonIPEA/blob/main/README.pt-br.md) com suporte adicional. Não hesite em pedir ajuda. </br>