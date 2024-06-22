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
import duckdb as ddb
import geobr
import geopandas as gpd
import plotly.express as px
import folium
from folium.plugins import StripePattern

logging.basicConfig(level = logging.INFO
                  , format = '%(asctime)s - %(levelname)s - %(message)s')

class DataProcessor:
    def __init__(self
               , bronze_folder : str
               , silver_folder : str
               , gold_folder : str
               , statistical_analysis_folder : str
               , db_path : str):
        self.bronze_folder = bronze_folder
        self.silver_folder = silver_folder
        self.gold_folder = gold_folder
        self.statistical_analysis_folder = statistical_analysis_folder
        self.db_path = db_path
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
                elif 'RecCorr_2010.parquet' in filename:
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
            DataFrame: Processed data as a single pandas DataFrame, then saving at Gold layer and DuckDB, or None if an error occurs. Also, Descriptive Summary as a csv file, then saving at Statistical Analysis folder, or None if an error occurs.
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
            
            # Save DataFrame to DuckDB
            conn = ddb.connect(self.db_path)
            conn.execute('CREATE TABLE IF NOT EXISTS df AS SELECT * FROM df')
            conn.close()

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

class Database:
    def __init__(self):
        """Create connection to DuckDB database."""
        self._install_extensions()
        self.conn = ddb.connect('ipea.db')

    def _install_extensions(self):
        """Install extensions to DuckDB database."""
        ddb.sql("""
        INSTALL spatial;
        INSTALL parquet;        
        LOAD spatial;
        LOAD parquet;
        """)

class DataFetcher:
    def __init__(self
               , db_path : str):
        self.db_path = db_path

    def fetch_data(self) -> pd.DataFrame:
        """
        Load data from DuckDB database.

        Returns:
            DataFrame: The finished pandas DataFrame.
        """
        try:
            conn = ddb.connect(self.db_path)
            df = conn.execute('SELECT * FROM df').fetchdf()
            conn.close()  
            return df
        except Exception as e:
            logging.error(f'Error loading data from DuckDB: {e}')
            return None

    def fetch_geodata(self) -> gpd.GeoDataFrame:
        """
        Fetch geodata from Municipalities geobr database.

        Returns:
            GeoDataFrame: The finished GeoDataFrame.
        """
        try:
            gdf = geobr.read_municipality(code_muni = 'all'
                                        , year = 2010)
            gdf = gpd.GeoDataFrame(gdf).drop(columns = ['name_muni'
                                                      , 'code_state']).rename(columns = {'abbrev_state' : 'UF'})
            return gdf
        except Exception as e:
            logging.error(f'Error fetching geodata: {e}')
            return None

class DataMerger:
    @staticmethod
    def merge_data(data : pd.DataFrame
                 , geodata : gpd.GeoDataFrame
                 , gold_folder : str) -> gpd.GeoDataFrame:
        """
        Merge finished DataFrame to Municipalities geodata.

        Args:
            data (DataFrame): Finished data at Gold layer, ready to use.
            geodata (GeoDataFrame): Polygons from each city in Brazil.

        Returns:
            GeoDataFrame: A GeoDataFrame containing the selected IPEA data.
        """
        data.loc[:, 'CodMunIBGE'] = data['CodMunIBGE'].astype(int)
        geodata['code_muni'] = geodata['code_muni'].astype(int)
        geodata = geodata.rename(columns = {'code_muni' : 'CodMunIBGE'})
        app_data = data.merge(geodata
                            , how = 'left'
                            , on = 'CodMunIBGE')
        app_data = gpd.GeoDataFrame(app_data, geometry='geometry')
        file_path = os.path.join(gold_folder, 'AppData.parquet')
        app_data.to_parquet(file_path, index=None, compression='snappy', schema_version=None)
        return gpd.GeoDataFrame(app_data)
    
class Visualizer:
    def __init__(self
               , app_data : gpd.GeoDataFrame):
        self.app_data = app_data[app_data['data_status'] == 'complete']
        self.plot_palette = px.colors.diverging.PiYG[::2]

    def histogram_layout(self
                       , column_name
                       , start_value
                       , end_value
                       , bins = 100):
        histogram = px.histogram(self.app_data
                               , x = column_name
                               , nbins = bins
                               , width = 550
                               , height = 275
                               , color_discrete_sequence = self.plot_palette)
        histogram.update_layout(yaxis_title = 'Frequency'
                              , margin = {'l' : 0
                                        , 'r' : 0
                                        , 't' : 0
                                        , 'b' : 0}
                              , bargap = 0.01)
        histogram.update_traces(xbins = dict(start = start_value
                                           , end = end_value
                                           , size = (end_value-start_value)/bins))
        return histogram

    def plot_histograms(self):
        """Plotting statistical charts to illustrate the model analysis."""
        histograms_col1 = [self.histogram_layout('IDHM 2010', 0, 1, 100)
                         , self.histogram_layout('Receitas Correntes 2010 (R$)', 0, 165000000, 100)]
        histograms_col2 = [self.histogram_layout('Carga Tributária Municipal 2010', 0, 0.8, 100)
                         , self.histogram_layout('PIB 2010 (R$)', 0, 2000000000, 100)]
        return histograms_col1, histograms_col2

    def plot_correlation_heatmap(self):
        """Plotting statistical charts to illustrate the model analysis."""
        # Stablishing a correlation matrix to plot as a heatmap, and masking it superior half.
        corr_matrix = self.app_data[['IDHM 2010'
                                   , 'PIB 2010 (R$)'
                                   , 'Receitas Correntes 2010 (R$)'
                                   , 'Carga Tributária Municipal 2010']].corr(method = 'pearson')
        corr_matrix = corr_matrix.mask(np.triu(np.ones_like(corr_matrix
                                                          , dtype = bool))).round(2)

        corr_heatmap = px.imshow(corr_matrix
                               , text_auto = True
                               , color_continuous_scale = 'PuRd')
        custom_hover_template = '<br>'.join(['X: %{x}'
                                           , 'Y: %{y}'
                                           , 'Correlation: %{z}'])
        corr_heatmap.update_layout(width = 550
                                 , height = 275
                                 , margin = {'l' : 0
                                           , 'r' : 0
                                           , 't' : 0
                                           , 'b' : 0})
        corr_heatmap.update_traces(xgap = 1
                                 , ygap = 1
                                 , hovertemplate = custom_hover_template)
        return corr_heatmap

    def plot_bubble_chart(self):
        """Plotting statistical charts to illustrate the model analysis."""
        bubble_trend = px.scatter(self.app_data
                                , x = 'Carga Tributária Municipal 2010'
                                , y = 'IDHM 2010'
                                , size = 'Habitantes 2010'
                                , hover_name = 'Município'
                                , trendline = 'lowess'
                                , width = 550
                                , height = 275
                                , color_discrete_sequence = self.plot_palette)
        bubble_trend.update_layout(margin = {'l' : 0
                                           , 'r' : 0
                                           , 't' : 0
                                           , 'b' : 0})
        return bubble_trend

class Mapper:
    def __init__(self
               , app_data : gpd.GeoDataFrame):
        self.app_data = app_data

    def create_map(self):
        """Fetching Brazilian basemap, setting data layers, interaction parameters and styles to display the data collected for the model analysis."""
        mapa = folium.Map(location = [-14, -53.25]
                        , zoom_start = 4
                        , tiles = 'cartodbdark_matter')
        # Style and highlight functions for hover functionality
        style_function = lambda x : {'fillColor' : '#ffffff'
                                   , 'color' : '#000000'
                                   , 'fillOpacity' : 0.1
                                   , 'weight' : 0.1}
        highlight_function = lambda x : {'fillColor' : '#000000'
                                       , 'color' : '#000000'
                                       , 'fillOpacity' : 0.50
                                       , 'weight' : 0.1}
        # Tooltip data format
        self.app_data['Formatted Carga Tributária'] = self.app_data['Carga Tributária Municipal 2010'].apply(lambda x : '{:.3%}'.format(x))
        # IDHM as scale layer
        folium.Choropleth(geo_data = self.app_data.__geo_interface__
                        , data = self.app_data
                        , key_on = 'feature.properties.CodMunIBGE'
                        , columns = ['CodMunIBGE'
                                   , 'IDHM 2010']
                        , fill_color = 'PuRd'
                        , fill_opacity = 0.75
                        , line_color = '#0000'
                        , line_opacity = 0.25
                        , nan_fill_color = 'White'
                        , legend_name = 'IDHM 2010'
                        , name = 'IDHM 2010'
                        , smooth_factor = 0
                        , show = True
                        , overlay = True).add_to(mapa)
        # Add hover functionality
        tooltip_function = folium.features.GeoJson(data = self.app_data
                                                 , style_function = style_function
                                                 , control = False
                                                 , highlight_function = highlight_function
                                                 , tooltip = folium.features.GeoJsonTooltip(fields = ['Município'
                                                                                                    , 'IDHM 2010'
                                                                                                    , 'Formatted Carga Tributária']
                                                                                          , aliases = ['2010'
                                                                                                     , 'Índ. Desenv. Hum'
                                                                                                     , 'Carga Trib. Mun.']
                                                                                          , style = ('background-color : white; color : #333333; font-family : arial; font-size : 12px; padding : 2px')))
        # Striped Pattern for Incomplete Data
        incomplete = self.app_data[self.app_data['data_status'] == 'incomplete']
        stripe_pattern = StripePattern(angle = 120
                                     , color = 'black')
        folium.features.GeoJson(name = 'Mun. com dados incompletos'
                              , data = incomplete
                              , style_function = lambda x : {'fillColor' : '#ffffff'
                                                           , 'color' : '#000000'
                                                           , 'fillOpacity' : 0.75
                                                           , 'weight' : 0.1
                                                           , 'fillPattern' : stripe_pattern}
                              , show = True
                              , overlay = True).add_to(mapa)
        mapa.add_child(tooltip_function)
        mapa.keep_in_front(tooltip_function)
        return mapa

def main():
    config = {'bronze' : os.getenv('BRONZE_FOLDER', 'Bronze')
            , 'silver' : os.getenv('SILVER_FOLDER', 'Silver')
            , 'gold' : os.getenv('GOLD_FOLDER', 'Gold')
            , 'statistical_analysis' : os.getenv('STATISTICAL_ANALYSIS_FOLDER', 'Statistical Analysis')
            , 'db_path' : os.getenv('DB_PATH', 'ipea.db')}
    
    # Extract values from the config dictionary
    bronze_folder = config['bronze']
    silver_folder = config['silver']
    gold_folder = config['gold']
    statistical_analysis_folder = config['statistical_analysis']
    db_path = config['db_path']
    
    processor = DataProcessor(bronze_folder
                            , silver_folder
                            , gold_folder
                            , statistical_analysis_folder
                            , db_path)
    processor.create_folders()

    data_series = [('PIB_IBGE_5938_37', 2010, 'PIB_2010.parquet')
                 , ('RECORRM', 2010, 'RecCorr_2010.parquet')
                 , ('POPTOT', 2010, 'População_2010.parquet')
                 , ('Municípios', None, 'Municípios.parquet')]
    
    for series, year, filename in data_series:
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
        df = processor.gold_finish('DescriptiveData.parquet')
        processor.analyze_data(df)
        df = Database()
        fetcher = DataFetcher(db_path)
        data = fetcher.fetch_data()
        geodata = fetcher.fetch_geodata()
        app_data = DataMerger.merge_data(data
                                       , geodata
                                       , gold_folder)

if __name__ == '__main__':
    main()
# %%
