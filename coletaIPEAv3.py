#%%
# Standard Library Imports
import os
import logging
from typing import Optional

# Third-Party Libraries
import pandas as pd
import ipeadatapy as ipea
import rpy2.robjects as robjects
from rpy2.robjects import pandas2ri
from rpy2.robjects.conversion import localconverter
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import seaborn as sns
import statsmodels.api as sm
import statsmodels.formula.api as smf
from patsy.builtins import *

# Set error capture logging
logging.basicConfig(level = logging.INFO)

class DataProcessor:
    def __init__(self
               , bronze_folder: str
               , silver_folder: str
               , gold_folder: str
               , statistical_analysis_folder: str):
        self.bronze_folder = bronze_folder
        self.silver_folder = silver_folder
        self.gold_folder = gold_folder
        self.statistical_analysis_folder = statistical_analysis_folder
        self.join_list = []
        self.pdf_pages = PdfPages(os.path.join(self.statistical_analysis_folder, 'Plots.pdf'))

    def create_folders(self) -> None:
        """Create required directories as layer folders."""
        folders = [self.bronze_folder
                 , self.silver_folder
                 , self.gold_folder
                 , self.statistical_analysis_folder]
        for folder in folders:
            os.makedirs(folder, exist_ok = True)


    def saving_step(self
                  , data: pd.DataFrame
                  , folder: str
                  , filename: str) -> None:
        """
        Save data at respective layer with specified filename.

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
                  , index = False
                  , encoding = 'utf-8')


    def bronze_fetch(self
                   , series: str
                   , year: int
                   , filename: str
                   , r_code: Optional[str] = None) -> Optional[pd.DataFrame]:
        """
        Fetches IPEA raw data based on the series and year provided. ipeadatar and ipeadatapy (ipea.territories(), ipea.timeseries()).

    Args:
        series (str): Series ID at IPEA database to fetch data.
        year (int): Year filter for the data fetched.
        filename (str): Filename to save the data fetched.
        r_code (Optional[str]): R code to ipeadatar for fetching data that isn't at ipeadatapy.

    Returns:
        DataFrame: Fetched data as pandas DataFrame, then saving at Bronze layer, or None if an error occurs (with an error log).
        """
        try:
            # Special handling for IDHM 2010 (IPEAdataR)
            if filename == 'IDHM_2010.csv':
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
            logging.error(f"Error fetching data for {filename}: {e}")
            return None


    def silver_transform(self
                       , df: pd.DataFrame
                       , filename: str) -> Optional[pd.DataFrame]:
        """
        Process IPEA Bronze data fetched to get it ready to consolidate. Removing unused data, Relabeling fields, Row filtering.

    Args:
        df (DataFrame): DataFrame from Series ID fetched at IPEA.
        filename (str): Filename to save the data fetched.

    Returns:
        DataFrame: Processed data as pandas DataFrame, then saving at Silver layer, or None if an error occurs (with an error log).
        """
        try:
            transformations = {
                'IDHM_2010.csv': lambda x: x.query('(uname == "Municipality") & (date == @pd.to_datetime("2010-01-01"))')
                                            .drop(columns = ['code'
                                                           , 'uname'
                                                           , 'date']) \
                                            .rename(columns = {'tcode': 'CodMunIBGE'
                                                             , 'value': 'IDHM 2010'})
              , 'Municípios.csv': lambda x: x.query('LEVEL == "Municípios"') \
                                             .drop(columns = ['LEVEL'
                                                            , 'AREA'
                                                            , 'CAPITAL']) \
                                             .rename(columns = {'NAME': 'Município'
                                                              , 'ID': 'CodMunIBGE'})
              , 'PIB_2010.csv': lambda x: x.assign(VALUE=(x['VALUE (R$ (mil), a preços do ano 2010)'].astype(float) * 1000).round(3)) \
                                           .query('NIVNOME == "Municípios"') \
                                           .drop(columns = ['CODE'
                                                          , 'RAW DATE'
                                                          , 'YEAR'
                                                          , 'NIVNOME']) \
                                           .rename(columns={'TERCODIGO': 'CodMunIBGE'
                                                          , 'VALUE (R$ (mil), a preços do ano 2010)': 'PIB 2010 (R$)'}) \
              , 'Arrecadação_2010.csv': lambda x: x.assign(VALUE=x['VALUE (R$)'].astype(float).round(2)) \
                                                 .rename(columns={'TERCODIGO': 'CodMunIBGE'
                                                                , 'VALUE (R$)': 'Receitas Correntes 2010 (R$)'}).drop(columns=['CODE', 'RAW DATE', 'YEAR', 'NIVNOME'])
              , 'População_2010.csv': lambda x: x.drop(columns=['CODE'
                                                              , 'RAW DATE'
                                                              , 'YEAR'
                                                              , 'NIVNOME']) \
                                                 .rename(columns={'TERCODIGO': 'CodMunIBGE'
                                                                , 'VALUE (Habitante)': 'Habitantes 2010'}) \
                                                 .astype({'Habitantes 2010': int
                                                        , 'CodMunIBGE': str}
                                                        , errors='ignore')}
            transformation_func = transformations.get(filename)
            if transformation_func:
                df = transformation_func(df)

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
        Reunite Silver data processed to get it ready to use. Merging variables, Reordering fields, N/A Row filtering, Sorting. Applying statistical calculations and plots.

    Args:
         filename (str): Filename to save the data processed.

    Returns:
        DataFrame: Processed data as a single pandas DataFrame, then saving at Gold layer, or None if an error occurs. Descriptive analysis plots as a single pdf file, then saving at Statistical Analysis folder, or None if an error occurs.
        """
        try:
            clean_data = self.join_list[0]
            for df in self.join_list[1:]:
                df['CodMunIBGE'] = df['CodMunIBGE'].astype(str)
                clean_data = clean_data.merge(df
                                            , how = 'left'
                                            , on = 'CodMunIBGE')
            order_set = ['CodMunIBGE'
                    , 'Município'
                    , 'Habitantes 2010'
                    , 'IDHM 2010'
                    , 'Receitas Correntes 2010 (R$)'
                    , 'PIB 2010 (R$)'
                    , 'Carga Tributária Municipal 2010']
            clean_data.dropna(inplace = True)
            clean_data = clean_data.reindex(columns = order_set)
            clean_data.sort_values(by = 'CodMunIBGE'
                                , inplace = True)
            clean_data['Carga Tributária Municipal 2010'] = clean_data['Receitas Correntes 2010 (R$)'].div(clean_data['PIB 2010 (R$)'], fill_value = 0).astype(float)

            summary = clean_data.describe()
            print("Descriptive Statistics:\n", summary)
            summary.to_csv(os.path.join(self.statistical_analysis_folder
                                    , 'Descriptive Statistics Initial Analysis.csv'))

            pib_95th = clean_data['PIB 2010 (R$)'].quantile(0.95)
            receitas_95th = clean_data['Receitas Correntes 2010 (R$)'].quantile(0.95)

            if not hasattr(self, 'pdf_pages'):
                self.pdf_pages = PdfPages(os.path.join(self.statistical_analysis_folder, 'Plots.pdf'))

            def plot_histogram(column: str
                            , title: str
                            , xlabel: str
                            , percentile = None
                            , pdf_pages = None):
                """
                Plot Histograms (Frequency Distribution Charts) from CleanData variables; 
                Also saved in .pdf at Statistics Analysis folder.

            Args:
                column (str): Selected field/variable from CleanData.
                title (str): Title to be presented at the chart.
                xlabel (str): X axis label.
                percentile (None): Conditional trigger to adjust plot data for outliers.
                pdf_pages (None): Call to reunite all plots in a single file.

            Returns:
                Object: Plotted histograms for every selected variable, then saving at Statistics Analysis folder.
                """
                if percentile is not None:
                    data = clean_data[clean_data[column] <= percentile][column]
                else:
                    data = clean_data[column]
                plt.figure(figsize = (10, 6))
                sns.set_style('whitegrid')
                sns.color_palette('viridis')
                sns.histplot(data
                        , bins = 100
                        , kde = True)
                plt.title(title)
                plt.xlabel(xlabel)
                plt.ylabel('Frequency')
                if pdf_pages is not None:
                    plt.savefig(pdf_pages
                            , format = 'pdf'
                            , bbox_inches = 'tight')
                plt.show()
                plt.close()

            plot_histogram('IDHM 2010'
                        , 'Distribution of IDHM 2010'
                        , 'IDHM 2010'
                        , pdf_pages = self.pdf_pages)
            plot_histogram('Receitas Correntes 2010 (R$)'
                        , 'Distribution of Receitas Correntes 2010 (R$) - Adjusted for Outliers'
                        , 'Receitas Correntes 2010 (R$)'
                        , receitas_95th
                        , pdf_pages = self.pdf_pages)
            plot_histogram('PIB 2010 (R$)'
                        , 'Distribution of PIB 2010 (R$) - Adjusted for Outliers'
                        , 'PIB 2010 (R$)'
                        , pib_95th
                        , pdf_pages = self.pdf_pages)
            plot_histogram('Carga Tributária Municipal 2010'
                        , 'Distribution of Carga Tributária Mun. 2010'
                        , 'Carga Tributária Mun. 2010'
                        , pdf_pages = self.pdf_pages)
                    
            plt.figure(figsize = (12, 5))
            sns.set_style('whitegrid')
            sns.color_palette('viridis')
            sns.lmplot(x = 'Carga Tributária Municipal 2010'
                    , y = 'IDHM 2010'
                    , data = clean_data
                    , scatter_kws = {"alpha": 0.7}
                    , line_kws = {"color": "red"})
            plt.title('ScatterPlot - IDHM vs Carga Tributária Mun. (2010)')
            plt.xlabel('Carga Tributária Mun. 2010')
            plt.ylabel('IDHM 2010')
            plt.savefig(self.pdf_pages
                    , format = 'pdf'
                    , bbox_inches = 'tight')
            plt.show()
            plt.close()

            self.saving_step(clean_data
                        , self.gold_folder
                        , filename)
            return clean_data
        except Exception as e:
            logging.error(f"Error finalizing data for {filename}: {e}")
            return None

    def process_data(self
                   , series: str
                   , year: int
                   , filename: str
                   , r_code: Optional[str] = None) -> None:
        """
        Setting workflow parameters to fetch, process and save data. Fetching data parameters, directories and consolidation before Gold layer.

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


    def analyze_data(self
                   , clean_data: pd.DataFrame) -> None:
        """
        Statistical calculations to the merged data. Stablishing a correlation matrix, applying Linear Regression and ANOVA to the given variables.

    Args:
        clean_data (DataFrame): Merged data post processing, ready to use.
        
    Returns:
        Statistical Model calculations and conversion to HTML,
        HTML file report with Correlation Matrix, Linear Regression and ANOVA,
        Correlation Heatmap plot, all saved at Statistical Analysis folder
        """
        try:
            corr_matrix = clean_data[['IDHM 2010'
                                    , 'Carga Tributária Municipal 2010'
                                    , 'PIB 2010 (R$)']].corr(method = 'pearson')
            print("Correlation Matrix:\n"
                , corr_matrix)
            corr_matrix_html = corr_matrix.to_html()

            model = smf.ols(formula = "Q('IDHM 2010') ~ Q('Carga Tributária Municipal 2010') + Q('PIB 2010 (R$)')", data = clean_data).fit()
            print(model.summary())
            model_summary = model.summary().as_html()

            anova_table = sm.stats.anova_lm(model
                                        , typ = 2)
            print("ANOVA Table:\n"
                , anova_table)
            anova_html = anova_table.to_html()

            correlation_heatmap = sns.heatmap(corr_matrix
                                            , annot = True
                                            , cmap = 'viridis')
            correlation_heatmap.get_figure().savefig(self.pdf_pages
                                                , format = 'pdf'
                                                , bbox_inches = 'tight')

            self.pdf_pages.close()

            corr_matrix_html = corr_matrix.to_html(classes = 'table table-striped text-center')
            anova_html = anova_table.to_html(classes = 'table table-striped text-center')

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

            report_filename = os.path.join(self.statistical_analysis_folder
                                        , 'Analysis Report.html')
            with open(report_filename
                    , 'w') as f:
                f.write(html_report)
            print(f"Report saved to {report_filename}")
        except Exception as e:
            logging.error(f"Error analyzing data: {e}")

def main():
    config = {
        'bronze': os.getenv('BRONZE_FOLDER', 'Bronze')
      , 'silver': os.getenv('SILVER_FOLDER', 'Silver')
      , 'gold': os.getenv('GOLD_FOLDER', 'Gold')
      , 'statistical_analysis': os.getenv('STATISTICAL_ANALYSIS_FOLDER', 'Statistical Analysis')
    }
    processor = DataProcessor(config['bronze']
                            , config['silver']
                            , config['gold']
                            , config['statistical_analysis'])
    processor.create_folders()

    data_series = [
        ('PIB_IBGE_5938_37', 2010, 'PIB_2010.csv')
      , ('RECORRM', 2010, 'Arrecadação_2010.csv')
      , ('POPTOT', 2010, 'População_2010.csv')
      , ('Municípios', None, 'Municípios.csv')
    ]
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
                         , 'IDHM_2010.csv'
                         , r_code = r_code)

    if processor.join_list:
        clean_data = processor.gold_finish('CleanData.csv')
        processor.analyze_data(clean_data)

if __name__ == "__main__":
    main()

# %%