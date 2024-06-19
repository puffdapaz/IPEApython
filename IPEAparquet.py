#%%
import os
import logging
import pandas as pd
import ipeadatapy as ipea
import rpy2.robjects as robjects
from rpy2.robjects import pandas2ri
from rpy2.robjects.conversion import localconverter
import statsmodels.api as sm
import statsmodels.formula.api as smf
from patsy.builtins import *
from typing import Optional
import numpy as np

logging.basicConfig(level = logging.INFO
                  , format = '%(asctime)s - %(levelname)s - %(message)s')

class DataProcessor:
    def __init__(self
               , bronze_folder : str
               , silver_folder : str
               , gold_folder : str
               , statistical_analysis_folder : str):
        self.bronze_folder = bronze_folder
        self.silver_folder = silver_folder
        self.gold_folder = gold_folder
        self.statistical_analysis_folder = statistical_analysis_folder
        self.join_list = []

    def create_folders(self) -> None:
        """Create required folders as layer directories."""
        folders = [self.bronze_folder
                 , self.silver_folder
                 , self.gold_folder
                 , self.statistical_analysis_folder]
        for folder in folders:
            os.makedirs(folder
                      , exist_ok = True)

    def saving_step(self
                  , df : pd.DataFrame
                  , folder : str
                  , filename : str) -> None:
        """
        Save data at each step, on respective layer with specified filename.

    Args:
        df (DataFrame): Fetched data at step before.
        folder (str): Directory emulating Medallion layer.
        filename (str): Filename to save the df fetched.

    Returns:
        File: Saved file at layer directory.
        """
        path = os.path.join(folder
                          , filename)
        df.to_parquet(path
                      , engine = 'pyarrow')

    def bronze_fetch(self
                   , series : str
                   , year : int
                   , filename : str
                   , r_code : Optional[str] = None) -> Optional[pd.DataFrame]:
        """
        Fetches IPEA raw data based on series and year provided. ipeadatar and ipeadatapy (ipea.territories(), ipea.timeseries()).

    Args:
        series (str): Series ID at IPEA database.
        year (int): Year filter for the data fetched.
        filename (str): Filename to save the data fetched.
        r_code (Optional[str]): R code to ipeadatar for fetching data that isn't at ipeadatapy.

    Returns:
        DataFrame: Fetched data as pandas DataFrame, then saving at Bronze layer, or None if an error occurs (with an error log).
        """
        try:
            # Special handling for IDHM 2010 (IPEAdataR)
            if filename == 'IDHM_2010.parquet':
                data = robjects.r(r_code) # R code to fetch data
                with localconverter(robjects.default_converter + pandas2ri.converter) as cv:
                    raw_data = cv.rpy2py(data) # R data conversion to pandas DataFrame
                if 'date' in raw_data.columns and raw_data['date'].dtype == 'float64':
                    raw_data['date'] = pd.to_datetime(raw_data['date']
                                                    , unit = 'D'
                                                    , origin = '1970-01-01')
                raw_data = pd.DataFrame(raw_data)
            elif series == 'Municípios':
                raw_data = ipea.territories() # Cities names data fetch
                raw_data = pd.DataFrame(raw_data)
            else:
                # Regular fetch (IPEAdataPy)
                raw_data = ipea.timeseries(series = series
                                         , year = year)
                raw_data = pd.DataFrame(raw_data)

            self.saving_step(raw_data
                           , self.bronze_folder
                           , filename)
            return raw_data
        except Exception as e:
            logging.error(f'Error fetching data for {filename}: {e}')
            return None

    def silver_transform(self
                       , transf_df : pd.DataFrame
                       , filename : str) -> Optional[pd.DataFrame]:
        """
        Process Bronze layer data to get it ready to consolidate. Removing unused data, Relabeling fields and Row filtering.

    Args:
        transf_df (DataFrame): DataFrame from Series ID fetched at IPEA.
        filename (str): Filename to save the data fetched.

    Returns:
        DataFrame: Processed data as pandas DataFrame, then saving at Silver layer, or None if an error occurs (with an error log).
        """
        try:
            if 'IDHM_2010.parquet' in filename:
                date_filter = pd.to_datetime('2010-01-01')
                transf_df = transf_df.query("(uname == 'Municipality') & (date == @date_filter)") \
                       .drop(columns = ['code'
                                      , 'uname'
                                      , 'date']) \
                       .rename(columns = {'tcode' : 'CodMunIBGE'
                                        , 'value' : 'IDHM 2010'})
            elif filename == 'Municípios.parquet':
                transf_df = transf_df.query("LEVEL == 'Municípios'") \
                       .drop(columns = ['LEVEL'
                                      , 'AREA'
                                      , 'CAPITAL']) \
                       .rename(columns = {'NAME' : 'Município'
                                        , 'ID' : 'CodMunIBGE'})
            else:
                transf_df = transf_df.query("NIVNOME == 'Municípios'") \
                       .drop(columns = ['CODE'
                                      , 'RAW DATE'
                                      , 'YEAR'
                                      , 'NIVNOME'])
                if 'PIB_2010.parquet' in filename:
                    transf_df['VALUE (R$ (mil), a preços do ano 2010)'] = transf_df['VALUE (R$ (mil), a preços do ano 2010)'].astype(float) * 1000
                    transf_df['VALUE (R$ (mil), a preços do ano 2010)'] = transf_df['VALUE (R$ (mil), a preços do ano 2010)'].round(3)
                    transf_df = transf_df.rename(columns = {'TERCODIGO' : 'CodMunIBGE'
                                            , 'VALUE (R$ (mil), a preços do ano 2010)' : 'PIB 2010 (R$)'})
                    transf_df['PIB 2010 (R$)'] = pd.to_numeric(transf_df['PIB 2010 (R$)']
                                                         , errors = 'coerce')
                elif 'Arrecadação_2010.parquet' in filename:
                    transf_df['VALUE (R$)'] = transf_df['VALUE (R$)'].astype(float).round(2)
                    transf_df = transf_df.rename(columns = {'TERCODIGO' : 'CodMunIBGE'
                                            , 'VALUE (R$)' : 'Receitas Correntes 2010 (R$)'})
                    transf_df['Receitas Correntes 2010 (R$)'] = pd.to_numeric(transf_df['Receitas Correntes 2010 (R$)']
                                                                     , errors = 'coerce')
                elif 'População_2010.parquet' in filename:
                    transf_df = transf_df.rename(columns = {'TERCODIGO' : 'CodMunIBGE'
                                            , 'VALUE (Habitante)' : 'Habitantes 2010'})
                    transf_df = transf_df.astype({'Habitantes 2010' : int
                                  , 'CodMunIBGE' : str}
                                  , errors = 'ignore')

            self.saving_step(transf_df
                           , self.silver_folder
                           , filename)
            return transf_df
        except Exception as e:
            logging.error(f'Error transforming data for {filename}: {e}')
            return None

    def gold_finish(self
                  , filename : str) -> pd.DataFrame:
        """
        Process Silver layer data to finish it. Merging variables, Reordering fields, N/A Row filtering, Sorting.

    Args:
         filename (str): Filename to save the data processed.

    Returns:
        DataFrame: Processed data as a single pandas DataFrame, then saving at Gold layer, or None if an error occurs. Also, Descriptive Summary as a csv file, then saving at Statistical Analysis folder, or None if an error occurs.
        """
        try:
            df = self.join_list[0]
            for transf_df in self.join_list[1:]:
                transf_df['CodMunIBGE'] = transf_df['CodMunIBGE'].astype(str)
                df = df.merge(transf_df
                                            , how = 'left'
                                            , on = 'CodMunIBGE')
            order_set = ['CodMunIBGE'
                       , 'Município'
                       , 'Habitantes 2010'
                       , 'IDHM 2010'
                       , 'Receitas Correntes 2010 (R$)'
                       , 'PIB 2010 (R$)'
                       , 'Carga Tributária Municipal 2010']
            df = df.reindex(columns = order_set)
            df.sort_values(by = 'CodMunIBGE'
                                 , inplace = True)
            df['Carga Tributária Municipal 2010'] = df['Receitas Correntes 2010 (R$)'].div(df['PIB 2010 (R$)']
                                                                                                         , fill_value = 0).astype(float)
            df['data_status'] = np.where((pd.notnull(df['IDHM 2010']) & (df['IDHM 2010'] != 0)) & 
                                                     (pd.notnull(df['PIB 2010 (R$)']) & (df['PIB 2010 (R$)'] != 0)) &
                                                     (pd.notnull(df['Receitas Correntes 2010 (R$)']) & (df['Receitas Correntes 2010 (R$)'] != 0))
                                                   , 'complete'
                                                   , 'incomplete')

            summary = df.describe()
            print('Descriptive Statistics:\n'
                , summary)
            summary.to_parquet(os.path.join(self.statistical_analysis_folder
                                          , 'Descriptive Statistics Initial Analysis.parquet'))

            self.saving_step(df
                           , self.gold_folder
                           , filename)

            return df
        except Exception as e:
            logging.error(f'Error finalizing data for {filename}: {e}')
            return None

    def process_data(self
                   , series : str
                   , year : int
                   , filename : str
                   , r_code : Optional[str] = None) -> None:
        """
        Setting workflow parameters to fetch, process and save data. Fetching data parameters, directories and consolidation before Gold layer.

    Args:
        series (str): Series ID at IPEA database to fetch data.
        year (int): Year filter for the data fetched.
        filename (str): Filename to save the data fetched.
        r_code (Optional[str]): R code to ipeadatar for fetching data that isn't at ipeadatapy.

    Returns:
        DataFrame: If there is no Data, do bronze_fetch,\n
        if bronze_fetch is done, and silver_transform isn't, do silver_transform,\n
        if silver_transform is done, prepare pandas DataFrame to be processed at gold_finish.
        """
        bronze_df = self.bronze_fetch(series
                                    , year
                                    , filename
                                    , r_code)
        if bronze_df is not None:
            silver_df = self.silver_transform(bronze_df
                                            , filename)
            if silver_df is not None:
                self.join_list.append(silver_df)

    def analyze_data(self
                   , df : pd.DataFrame) -> None:
        """
        Statistical calculations to the finished data. Stablishing a correlation matrix, applying Linear Regression and ANOVA to the given variables.

    Args:
        data (DataFrame): Finished data at Gold layer, ready to use.
        
    Returns:
        Statistical Model calculations and conversion to HTML.\n
        Correlation Matrix, Linear Regression and ANOVA, all saved in a single HTML file at Statistical Analysis folder.
        """
        try:
            corr_matrix = df[['IDHM 2010'
                                    , 'Carga Tributária Municipal 2010'
                                    , 'PIB 2010 (R$)']].corr(method = 'pearson')
            print('Correlation Matrix:\n'
                , corr_matrix)
            corr_matrix_html = corr_matrix.to_html()

            model = smf.ols(formula = "Q('IDHM 2010') ~ Q('Carga Tributária Municipal 2010') + Q('PIB 2010 (R$)')"
                          , data = df).fit()
            print(model.summary())
            model_summary = model.summary().as_html()

            anova_table = sm.stats.anova_lm(model
                                          , typ = 2)
            print('ANOVA Table:\n'
                , anova_table)
            anova_html = anova_table.to_html()

            corr_matrix_html = corr_matrix.to_html(classes = 'table table-striped text-center')
            anova_html = anova_table.to_html(classes = 'table table-striped text-center')

            html_report = f"""
    <html>
    <head>
        <title>Data Analysis Report</title>
        <link rel = 'stylesheet' href = 'https://stackpath.bootstrapcdn.com/bootstrap/4.3.1/css/bootstrap.min.css'>
        <style>
        .model-summary {{
                margin : auto;
                width : 80%;
                padding : 20px;
                border : 1px solid #ccc;
                background-color : #f9f9f9;
            }}
        </style>
    </head>
    <body>
        <h1>Data Analysis Report</h1>
        <section>
            <h2>Correlation Matrix</h2>
            {corr_matrix_html}
        </section>
        <section>
            <h2>ANOVA Results</h2>
            {anova_html}
        </section>
        <section>
            <h2>Linear Regression Model Summary</h2>
            <div class = 'model-summary'>
                {model_summary}
            </div>
        </section>
    </body>
    </html>
    """

            report_filename = os.path.join(self.statistical_analysis_folder
                                         , 'Analysis Report.html')
            with open(report_filename
                    , 'w') as f:
                f.write(html_report)
        except Exception as e:
            logging.error(f'Error analyzing data: {e}')

def main():
    config = {'bronze' : os.getenv('BRONZE_FOLDER', 'Bronze')
            , 'silver' : os.getenv('SILVER_FOLDER', 'Silver')
            , 'gold' : os.getenv('GOLD_FOLDER', 'Gold')
            , 'statistical_analysis' : os.getenv('STATISTICAL_ANALYSIS_FOLDER', 'Statistical Analysis')}
    processor = DataProcessor(config['bronze']
                            , config['silver']
                            , config['gold']
                            , config['statistical_analysis'])
    processor.create_folders()

    data_series = [('PIB_IBGE_5938_37', 2010, 'PIB_2010.parquet')
                 , ('RECORRM', 2010, 'Arrecadação_2010.parquet')
                 , ('POPTOT', 2010, 'População_2010.parquet')
                 , ('Municípios', None, 'Municípios.parquet')]
    for series \
      , year \
      , filename \
        in data_series:
        processor.process_data(series
                             , year
                             , filename)

    r_code = """
    install.packages('ipeadatar', repos = 'http://cran.r-project.org')
    library(ipeadatar)
    data_IDHM <- ipeadatar::ipeadata(code = 'ADH_IDHM')
    data_IDHM
    """
    processor.process_data(None
                         , None
                         , 'IDHM_2010.parquet'
                         , r_code = r_code)

    if processor.join_list:
        df = processor.gold_finish('AppData.parquet')
        processor.analyze_data(df)

if __name__ == '__main__':
    main()

# %%
