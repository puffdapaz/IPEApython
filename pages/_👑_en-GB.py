#%%
import os
import logging
import pandas as pd
from patsy.builtins import *
import numpy as np
import geopandas as gpd
import plotly.express as px
import folium
from folium.plugins import StripePattern
import streamlit as st
from streamlit_folium import folium_static

logging.basicConfig(level = logging.INFO
                  , format = '%(asctime)s - %(levelname)s - %(message)s')

class DataFetcher:
    def __init__(self, path : str):
        self.path = path

    def fetch_data(self) -> gpd.GeoDataFrame:
        """
        Load data from parquet generated file.

        Returns:
            DataFrame: The finished GeoPandas DataFrame.
        """
        try:
            app_data = gpd.read_parquet(self.path)
            if app_data.empty:
                logging.warning("Loaded DataFrame is empty.")
                return pd.DataFrame()  # Return an empty DataFrame if the loaded DataFrame is empty
            return app_data
        except FileNotFoundError:
            logging.error(f"File not found at path: {self.path}")
            return None
        except Exception as e:
            logging.error(f'Error loading parquet data: {e}')
            return None

class Visualizer:
    def __init__(self
               , app_data : gpd.GeoDataFrame):
        if app_data.empty:
            logging.warning("No complete data found for visualization.")
            self.app_data = pd.DataFrame()  # Use an empty DataFrame for further operations
        else:
            self.app_data = app_data[app_data['data_status'] == 'complete']
            self.plot_palette = px.colors.diverging.PiYG[::2]

    def histogram_layout(self
                       , column_name
                       , start_value
                       , end_value
                       , bins
                       , xaxis_title):
        histogram = px.histogram(self.app_data
                               , x = column_name
                               , nbins = bins
                               , width = 550
                               , height = 275
                               , color_discrete_sequence = self.plot_palette)
        histogram.update_layout(yaxis_title = 'Frequency'
                              , xaxis_title = xaxis_title
                              , margin = {'l' : 0
                                        , 'r' : 0
                                        , 't' : 0
                                        , 'b' : 0}
                              , bargap = 0.01)
        histogram.update_traces(xbins = dict(start = start_value
                                           , end = end_value
                                           , size = (end_value - start_value) / bins))
        return histogram

    def plot_histograms(self):
        """Plotting statistical charts to illustrate the model analysis."""
        histograms_col1 = [self.histogram_layout('IDHM 2010', 0, 1, 100, 'MHDI 2010')
                         , self.histogram_layout('Receitas Correntes 2010 (R$)', 0, 165000000, 100, 'Current Revenue 2010 (R$)')]
        histograms_col2 = [self.histogram_layout('Carga Tributária Municipal 2010', 0, 0.8, 100, 'Tax Burden 2010')
                         , self.histogram_layout('PIB 2010 (R$)', 0, 2000000000, 100, 'GDP 2010 (R$)')]
        return histograms_col1, histograms_col2
    
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
        bubble_trend.update_layout(yaxis_title = 'MHDI 2010'
                                 , xaxis_title = 'Tax Burden 2010'
                                 , margin = {'l' : 0
                                           , 'r' : 0
                                           , 't' : 0
                                           , 'b' : 0})
        return bubble_trend

    def plot_correlation_heatmap(self):
        """Plotting statistical charts to illustrate the model analysis."""
        # Stablishing a correlation matrix to plot as a heatmap, and masking it superior half.
        corr_matrix = self.app_data[['IDHM 2010'
                                   , 'PIB 2010 (R$)'
                                   , 'Receitas Correntes 2010 (R$)'
                                   , 'Carga Tributária Municipal 2010']].corr(method = 'pearson')
        corr_matrix = corr_matrix.mask(np.triu(np.ones_like(corr_matrix
                                                          , dtype = bool))).round(2)

        y_labels = ['MHDI 2010'
                  , 'GDP 2010 (R$)'
                  , 'Current Revenue 2010 (R$)'
                  , 'Tax Burden 2010']
        x_labels = y_labels

        corr_heatmap = px.imshow(corr_matrix
                               , x = x_labels
                               , y = y_labels
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
                                                                                                     , 'Hum. Devel. Index'
                                                                                                     , 'Mun. Tax Burden']
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

class StreamlitApp:
    def __init__(self
               , visualizer
               , mapper):
        self.visualizer = visualizer
        self.mapper = mapper
        self.histograms = self.visualizer.plot_histograms()

    def app_layout(self):
        """Defining Streamlit Dashboard parameters."""
        st.set_page_config(page_title = 'IPEA python Project'
                         , page_icon = '🐍'
                         , layout = 'wide'
                         , initial_sidebar_state = 'expanded')
        with st.sidebar:
            st.title('Brazilian Municipalities HDI vs Tax Burden in 2010')
            st.caption('A python learning project')
            st.subheader('Impact of tax burden on economic and social development analysis, using IPEA public data')
            st.write('The aim of this journey was to learn python, to apply concepts from Medallion and OOP, and to explore ETL good practices for data engineering, analysis and science, through a research carried out on 2015 in a scientific article, as a reference')
            st.info('In addition, this project contains R, CSS/HTML and SQL snippets to support it, also uses GitHub, Pandas, DuckDB and Streamlit tools')
            st.caption('for more info, see below:')
            st.markdown('[![GitHub](https://img.shields.io/badge/GitHub-181717.svg?style=for-the-badge&logo=GitHub&logoColor=white)](https://github.com/puffdapaz/pythonIPEA)')
            st.markdown('[![Article](https://img.shields.io/badge/Adobe%20Acrobat%20Reader-EC1C24.svg?style=for-the-badge&logo=Adobe-Acrobat-Reader&logoColor=white)](https://github.com/puffdapaz/pythonIPEA/blob/main/Impacto%20da%20receita%20tributária%20no%20desenvolvimento%20econômico%20e%20social.%20um%20estudo%20nos%20municípios%20brasileiros.pdf)')
            st.markdown('[![linkedIn](https://img.shields.io/badge/LinkedIn-0A66C2.svg?style=for-the-badge&logo=LinkedIn&logoColor=white)](https://www.linkedin.com/in/silvaph)')

        tab1, tab2, tab3 = st.tabs(['| Histograms |'
                                  , '| Analysis |'
                                  , '| Map |'])
        with tab1:
            col1, col2 = st.columns([1, 1]
                                  , gap = 'large')
            with col1:
                st.write(f'## Histograms')
                for i, hist in enumerate(self.histograms[0]):
                    st.plotly_chart(hist
                                  , use_container_width = True)
            with col2:
                st.write(f'##  ')
                for i, hist in enumerate(self.histograms[1]):
                    st.plotly_chart(hist
                                  , use_container_width = True)
        with tab2:
            col1, col2, col3  = st.columns([2, 8, 2]
                                         , gap = 'small')
            with col1:
                ""
            with col2:
                st.write(f'## Trend BubbleChart')
                bubble_trend = self.visualizer.plot_bubble_chart()
                st.plotly_chart(bubble_trend
                              , use_container_width = True)
                st.write(f'## Correlation Heatmap')
                corr_heatmap = self.visualizer.plot_correlation_heatmap()
                st.plotly_chart(corr_heatmap
                              , use_container_width = True)
            with col3:
                ""
        with tab3:
            col1, col2, col3  = st.columns([1, 8, 1]
                                         , gap = 'medium')
            with col1:
                ""
            with col2:
                st.write(f'## Map Detail')
                st.caption('*Striped cities got incomplete data')
                mapa = self.mapper.create_map()
                folium_static(mapa)
            with col3:
                ""

def main():
        path = os.path.join(os.getcwd(), "Gold", "AppData.parquet")
        fetcher = DataFetcher(path)
        app_data = fetcher.fetch_data()
        visualizer = Visualizer(app_data)
        mapper = Mapper(app_data)
        mapper.create_map()
        app = StreamlitApp(visualizer, mapper)
        app.app_layout()

if __name__ == '__main__':
    main()
# %%
