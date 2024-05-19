#%%
import os
import pandas as pd
import ipeadatapy as ipea
import rpy2.robjects as robjects
from rpy2.robjects import pandas2ri
from rpy2.robjects.conversion import localconverter
import logging
from typing import Optional
import matplotlib.pyplot as plt
import seaborn as sns
import statsmodels.api as sm
import statsmodels.formula.api as smf
from patsy.builtins import *

# Set error capture logging
logging.basicConfig(level=logging.ERROR)

class DataProcessor:
    def __init__(self
               , bronze_folder: str
               , silver_folder: str
               , gold_folder: str
               , descriptive_analysis_folder: str
               , model_folder: str):
        self.bronze_folder = bronze_folder
        self.silver_folder = silver_folder
        self.gold_folder = gold_folder
        self.descriptive_analysis_folder = descriptive_analysis_folder
        self.model_folder = model_folder
        self.join_list = []

    def create_folders(self) -> None:
        folders = [self.bronze_folder
                 , self.silver_folder
                 , self.gold_folder
                 , self.descriptive_analysis_folder
                 , self.model_folder]
        for folder in folders:
            os.makedirs(folder, exist_ok=True)

    def bronze_fetch(self
                   , series: str
                   , year: int
                   , filename: str
                   , r_code: Optional[str] = None) -> Optional[pd.DataFrame]:
        """
        Fetches IPEA raw data based on the series and year provided; 
        ipeadatar and ipeadatapy (ipea.territories(), ipea.timeseries()).

    Args:
        series (str): Series ID at IPEA database to fetch data.
        year (int): Year filter for the data fetched.
        filename (str): Filename to save the data fetched.
        r_code (Optional[str]): R code to ipeadatar for fetching data that isn't at ipeadatapy.

    Returns:
        DataFrame: Fetched data as pandas DataFrame, then saving at Bronze layer, or None if an error occurs (with an error log).
        """
        try:
            if filename == 'IDHM_2010.csv':
                data = robjects.r(r_code)
                with localconverter(robjects.default_converter + pandas2ri.converter) as cv:
                    raw_data = cv.rpy2py(data)
                if 'date' in raw_data.columns and raw_data['date'].dtype == 'float64':
                    raw_data['date'] = pd.to_datetime(raw_data['date']
                                                    , unit='D'
                                                    , origin='1970-01-01')
                raw_data = pd.DataFrame(raw_data)
            elif series == 'Municípios':
                raw_data = ipea.territories()
                raw_data = pd.DataFrame(raw_data)
            else:
                raw_data = ipea.timeseries(series=series
                                         , year=year)
                raw_data = pd.DataFrame(raw_data)

            self.saving_step(raw_data
                           , self.bronze_folder
                           , filename)
            return raw_data
        except Exception as e:
            logging.error(f"Error fetching data for {filename}: {e}")
            return None

    def saving_step(self
                  , data: pd.DataFrame
                  , folder: str
                  , filename: str) -> None:
        """
        Save data at respective layer with specified filename; 
        Bronze, Silver and Gold layers.

    Args:
        data (DataFrame): Fetched data at step before.
        folder (str): Directory emulating Medallion layer.
        filename (str): Filename to save the data fetched.

    Returns:
        File: Saved file at specified directory.
        """
        path = os.path.join(folder
                          , filename)
        data.to_csv(path
                  , index=False
                  , encoding='utf-8')

    def silver_transform(self
                       , df: pd.DataFrame
                       , filename: str) -> Optional[pd.DataFrame]:
        """
        Process IPEA Bronze data fetched to get it ready to consolidate; 
        Removing unused data, Relabeling fields, Row filtering, based on the series singularities.

    Args:
        df (DataFrame): DataFrame from Series ID fetched at IPEA.
        filename (str): Filename to save the data fetched.

    Returns:
        DataFrame: Processed data as pandas DataFrame, then saving at Silver layer, or None if an error occurs (with an error log).
        """
        try:
            if 'IDHM_2010.csv' in filename:
                df = df.query('(uname == "Municipality") & (date == "2010-01-01")') \
                       .drop(columns=['code'
                                    , 'uname'
                                    , 'date'])
                df = df.rename(columns={'tcode': 'CodMunIBGE'
                                      , 'value': 'IDHM 2010'})
            elif filename == 'Municípios.csv':
                df = df.query('LEVEL == "Municípios"') \
                       .drop(columns=['LEVEL'
                                    , 'AREA'
                                    , 'CAPITAL']) \
                       .rename(columns={'NAME': 'Município'
                                      , 'ID': 'CodMunIBGE'})
            else:
                df = df.query('NIVNOME == "Municípios"') \
                       .drop(columns=['CODE'
                                    , 'RAW DATE'
                                    , 'YEAR'
                                    , 'NIVNOME'])
                if 'PIB_2010.csv' in filename:
                    df['VALUE (R$ (mil), a preços do ano 2010)'] = df['VALUE (R$ (mil), a preços do ano 2010)'].astype(float) \
                                                                                                               .round(3) * 1000
                    df = df.rename(columns={'TERCODIGO': 'CodMunIBGE'
                                          , 'VALUE (R$ (mil), a preços do ano 2010)': 'PIB 2010 (R$)'})
                    df['PIB 2010 (R$)'] = pd.to_numeric(df['PIB 2010 (R$)'], errors='coerce')
                elif 'Arrecadação_2010.csv' in filename:
                    df = df.rename(columns={'TERCODIGO': 'CodMunIBGE'
                                          , 'VALUE (R$)': 'Receitas Correntes 2010 (R$)'})
                    df['Receitas Correntes 2010 (R$)'] = pd.to_numeric(df['Receitas Correntes 2010 (R$)'], errors='coerce')
                elif 'População_2010.csv' in filename:
                    df = df.rename(columns={'TERCODIGO': 'CodMunIBGE'
                                          , 'VALUE (Habitante)': 'Habitantes 2010'})
                    df = df.astype({'Habitantes 2010': int
                                  , 'CodMunIBGE': str}
                                  , errors='ignore')

            self.saving_step(df
                           , self.silver_folder
                           , filename)
            return df
        except Exception as e:
            logging.error(f"Error transforming data for {filename}: {e}")
            return None

    def gold_finish(self
                  , filename: str) -> pd.DataFrame:
        """
        Reunite IPEA Silver data processed to get it ready to use; 
        Merging variables, Reordering fields, N/A Row filtering, Sorting.

    Args:
        df (DataFrame): DataFrame from Series ID fetched at IPEA.
        filename (str): Filename to save the data processed.

    Returns:
        DataFrame: Processed data as a single pandas DataFrame, then saving at Gold layer, or None if an error occurs.
        """
        merged_df = self.join_list[0]
        for df in self.join_list[1:]:
            df['CodMunIBGE'] = df['CodMunIBGE'].astype(str)
            merged_df = merged_df.merge(df
                                      , how='left'
                                      , on='CodMunIBGE')
        order_set = ['CodMunIBGE'
                   , 'Município'
                   , 'Habitantes 2010'
                   , 'IDHM 2010'
                   , 'Receitas Correntes 2010 (R$)'
                   , 'PIB 2010 (R$)'
                   , 'Carga Tributária Municipal 2010']
        merged_df.dropna(inplace=True)
        merged_df = merged_df.reindex(columns=order_set)
        merged_df.sort_values(by='CodMunIBGE'
                            , inplace=True)
        merged_df['Carga Tributária Municipal 2010'] = merged_df['Receitas Correntes 2010 (R$)'].div(merged_df['PIB 2010 (R$)'], fill_value=0).astype(float)

        # Perform and save descriptive statistics analysis
        summary = merged_df.describe()
        print("Descriptive Statistics:\n", summary)
        summary.to_csv(os.path.join(self.descriptive_analysis_folder
                                  , 'Descriptive Statistics Initial Analysis.csv'))

        pib_95th = merged_df['PIB 2010 (R$)'].quantile(0.95)
        receitas_95th = merged_df['Receitas Correntes 2010 (R$)'].quantile(0.95)

        def plot_histogram(column: str
                         , title: str
                         , xlabel: str
                         , percentile = None
                         , filename = None):
            """
            Plot Histograms (Frequency Distribution Charts) from CleanData variables; 
            Also saved in .pdf at descriptive_analysis folder.

        Args:
            column (str): Selected field/variable from CleanData.
            title (str): Title to be presented at the chart.
            xlabel (str): X axis label.
            percentile (None): Conditional trigger to adjust plot data for outliers.
            filename (None): Filename to save the plotted chart.

        Returns:
            Object: Plotted histograms for every selected variable, then saving at descriptive_analysis folder.
            """
            if percentile is not None:
                data = merged_df[merged_df[column] <= percentile][column]
            else:
                data = merged_df[column]
            plt.figure(figsize=(10, 6))
            sns.color_palette('viridis')
            sns.set_style('whitegrid')
            sns.histplot(data
                       , bins=100
                       , kde=True)
            plt.title(title)
            plt.xlabel(xlabel)
            plt.ylabel('Frequency')
            plt.savefig(os.path.join(self.descriptive_analysis_folder
                                   , f'{filename}.pdf')
                                   , bbox_inches='tight')
            plt.show()
            plt.close()

        plot_histogram('IDHM 2010'
                     , 'Distribution of IDHM 2010'
                     , 'IDHM 2010'
                     , filename='Histogram IDHM 2010')
        plot_histogram('Carga Tributária Municipal 2010'
                     , 'Distribution of Carga Tributária Mun. 2010'
                     , 'Carga Tributária Mun. 2010'
                     , filename='Histogram Carga Tributaria Mun. 2010')
        plot_histogram('PIB 2010 (R$)'
                     , 'Distribution of PIB 2010 (R$) - Adjusted for Outliers'
                     , 'PIB 2010 (R$)'
                     , pib_95th
                     , filename='Histogram PIB 2010 adjusted')
        plot_histogram('Receitas Correntes 2010 (R$)'
                     , 'Distribution of Receitas Correntes 2010 (R$) - Adjusted for Outliers'
                     , 'Receitas Correntes 2010 (R$)'
                     , receitas_95th
                     , filename='Histogram Receitas Correntes 2010 adjusted')

        # Create a scatter plot (IDHM vs Carga Tributária Mun.) with a trendline
        plt.figure(figsize=(10, 5))
        sns.color_palette('viridis')
        sns.set_style('whitegrid')
        sns.lmplot(x='Carga Tributária Municipal 2010'
                 , y='IDHM 2010'
                 , data=merged_df
                 , scatter_kws={"alpha": 0.7}
                 , line_kws={"color": "red"})
        plt.title('ScatterPlot - IDHM vs Carga Tributária Mun. (2010)')
        plt.xlabel('Carga Tributária Mun. 2010')
        plt.ylabel('IDHM 2010')
        plt.savefig(os.path.join(self.descriptive_analysis_folder
                               , 'ScatterPlot IDHM vs Carga Tributaria.pdf')
                               , bbox_inches='tight')
        plt.show()
        plt.close()

        # Save the final DataFrame
        self.saving_step(merged_df
                       , self.gold_folder
                       , filename)
        return merged_df

    def process_data(self
                   , series: str
                   , year: int
                   , filename: str
                   , r_code: Optional[str] = None) -> None:
        """
        Setting workflow parameters to fetch, process and save data; 
        Fetching data parameters, directories and consolidation before Gold layer.

    Args:
        series (str): Series ID at IPEA database to fetch data.
        year (int): Year filter for the data fetched.
        filename (str): Filename to save the data fetched.
        r_code (Optional[str]): R code to ipeadatar for fetching data that isn't at ipeadatapy.

    Returns:
        DataFrame: If there is no Data, do bronze_fetch,
        if bronze_fetch is done, and silver_transform isn't, do silver_transform,
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

    def analyze_data(self, df: pd.DataFrame) -> None:
        # Correlation Matrix
        corr_matrix = df[['IDHM 2010', 'Carga Tributária Municipal 2010', 'PIB 2010 (R$)']].corr(method='pearson')
        print("Correlation Matrix:\n", corr_matrix)
        corr_matrix_html = corr_matrix.to_html()

        # Linear Regression Model
        model = smf.ols(formula="Q('IDHM 2010') ~ Q('Carga Tributária Municipal 2010') + Q('PIB 2010 (R$)')", data=df).fit()
        print(model.summary())
        model_summary = model.summary().as_html()
        #with open(os.path.join(self.model_folder, 'Model_Summary.html'), 'w') as f:
            #f.write(model_summary)

        # ANOVA
        anova_table = sm.stats.anova_lm(model, typ=2)
        print("ANOVA Table:\n", anova_table)
        anova_html = anova_table.to_html()

        # Assuming all data processing and analysis is done above this point

        # Convert correlation matrix to HTML
        corr_matrix_html = corr_matrix.to_html(classes='table table-striped text-center')

        # Convert ANOVA table to HTML
        anova_html = anova_table.to_html(classes='table table-striped text-center')

        # Combine all HTML sections into one document
        html_report = f"""
<html>
<head>
    <title>Data Analysis Report</title>
    <link rel="stylesheet" href="https://stackpath.bootstrapcdn.com/bootstrap/4.3.1/css/bootstrap.min.css">
    <style>
       .model-summary {{
            margin: auto;
            width: 80%;
            padding: 20px;
            border: 1px solid #ccc;
            background-color: #f9f9f9;
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
        <div class="model-summary">
            {model_summary}
        </div>
    </section>
</body>
</html>
"""

        # Save the HTML report to a file
        report_filename = os.path.join(self.model_folder, 'Analysis Report.html')
        with open(report_filename, 'w') as f:
            f.write(html_report)

        print(f"Report saved to {report_filename}")

        
def main():
    config = {
        'bronze': os.getenv('BRONZE_FOLDER', 'Bronze')
      , 'silver': os.getenv('SILVER_FOLDER', 'Silver')
      , 'gold': os.getenv('GOLD_FOLDER', 'Gold')
      , 'descriptive_analysis': os.getenv('DESCRIPTIVE_ANALYSIS_FOLDER', 'Descriptive Analysis')
      , 'model': os.getenv('MODEL_FOLDER', 'Model')
    }
    processor = DataProcessor(config['bronze']
                            , config['silver']
                            , config['gold']
                            , config['descriptive_analysis']
                            , config['model'])
    processor.create_folders()

    data_series = [
        ('PIB_IBGE_5938_37', 2010, 'PIB_2010.csv')
      , ('RECORRM', 2010, 'Arrecadação_2010.csv')
      , ('POPTOT', 2010, 'População_2010.csv')
      , ('Municípios', None, 'Municípios.csv')
    ]
    for series, year, filename in data_series:
        processor.process_data(series
                             , year
                             , filename)

    r_code = """
    install.packages('ipeadatar', repos='http://cran.r-project.org')
    library(ipeadatar)
    data_IDHM <- ipeadatar::ipeadata(code = 'ADH_IDHM')
    data_IDHM
    """
    processor.process_data(None
                         , None
                         , 'IDHM_2010.csv'
                         , r_code=r_code)

    if processor.join_list:
        clean_data = processor.gold_finish('CleanData.csv')
        clean_data

        # Analyze the data
        processor.analyze_data(clean_data)

if __name__ == "__main__":
    main()