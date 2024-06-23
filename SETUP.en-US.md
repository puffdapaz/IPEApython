# Setup Guide for Replicating the Project

[![pt-br](https://img.shields.io/badge/lang-pt--br-green.svg)](https://github.com/puffdapaz/pythonIPEA/blob/main/SETUP.pt-BR.md)

### This guide provides step-by-step instructions for setting up the project environment and running the script.

## Prerequisites
- Step 1: **Install Python**<br/>
Download Python's last version from the official website: https://www.python.org/downloads/<br/>
Run the installer and ensure to check the option "**_Add Python to PATH_**". <br/>

- Step 2: **Set Up a Virtual Environment**<br/>
Open Command Prompt (Windows) or Terminal (macOS/Linux). <br/>
Navigate to your project folder: cd path/yourproject/folder <br/>
Create a virtual environment named venv: python -m venv venv <br/>
Activate the virtual environment: <br/>
    On Windows: .\venv\Scripts\activate <br/>
    On macOS/Linux: source venv/bin/activate <br/>

- Step 3: **Install Required Libraries**<br/>
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

- Step 4: **Install R Software**<br/>
Download R from the official website: https://cran.r-project.org/ <br/>
Follow the installation instructions for your operating system. <br/>

- Step 5: **Install Required R Packages**<br/>
Install IpeaDataR: install.packages("ipeadatar") <br/>

There is [README](https://github.com/puffdapaz/pythonIPEA/blob/main/README.en-US.md) files with additional support. Don't hesitate to ask for assistance. <br/>
