#%%
import pandas as pd
import duckdb as ddb
import geobr
import geopandas as gpd
import plotly.express as px
import numpy as np
import folium
from folium.plugins import StripePattern
from IPython.display import display

class Database:
    def __init__(self):
        self._install_extensions()
        self.conn = ddb.connect('ipea.db')

    def _install_extensions(self):
        ddb.sql("""
        INSTALL spatial;
        INSTALL parquet;        
        LOAD spatial;
        LOAD parquet;
        """)

class DataFetcher:
    def __init__(self, db : Database):
        self.db = db

    def fetch_data(self) -> pd.DataFrame:
        query = "SELECT * FROM parquet_scan('C:\\Users\\puffd\\Desktop\\python2024\\pythonIPEA\\Gold\\AppData.parquet')"
        result = self.db.conn.execute(query)
        df = result.fetchdf()  # Convert the result to a Pandas DataFrame
        self.db.conn.register('df_view'
                            , df)  # Register the DataFrame with the DuckDB connection
        return df

    def fetch_geodata(self) -> gpd.GeoDataFrame:
        gdf = geobr.read_municipality(code_muni = 'all'
                                    , year = 2010)
        gdf = gpd.GeoDataFrame(gdf).drop(columns = ['name_muni'
                                                  , 'code_state']).rename(columns = {'abbrev_state' : 'UF'})
        return gdf

class DataMerger:
    @staticmethod
    def merge_data(data : pd.DataFrame
                 , geodata : gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        data['CodMunIBGE'] = data['CodMunIBGE'].astype(int)
        geodata['code_muni'] = geodata['code_muni'].astype(int)
        geodata = geodata.rename(columns = {'code_muni' : 'CodMunIBGE'})
        return gpd.GeoDataFrame(data.merge(geodata
                                         , how = 'left'
                                         , on = 'CodMunIBGE'))

class Visualizer:
    def __init__(self
               , app_data : gpd.GeoDataFrame
               , plot_data : pd.DataFrame):
        self.app_data = app_data
        self.plot_palette = px.colors.diverging.PiYG[::2]

    def plot_histograms(self):
        fig = px.histogram(plot_data
                         , x = 'IDHM 2010'
                         , nbins = 100
                         , width = 550
                         , height = 275
                         , color_discrete_sequence = self.plot_palette)
        fig.update_layout(yaxis_title = 'Frequency'
                        , margin = {'l' : 0
                                  , 'r' : 0
                                  , 't' : 0
                                  , 'b' : 0}
                        , bargap = 0.01)
        fig.update_traces(xbins = dict(start = 0
                                     , end = 1
                                     , size = 0.005))
        fig.show()
        pass

        fig = px.histogram(plot_data
                         , x = 'Receitas Correntes 2010 (R$)'
                         , nbins = 100
                         , width = 550
                         , height = 275
                         , color_discrete_sequence = self.plot_palette)
        fig.update_layout(yaxis_title = 'Frequency'
                        , margin = {'l' : 0
                                  , 'r' : 0
                                  , 't' : 0
                                  , 'b' : 0}
                        , bargap = 0.01)
        fig.update_traces(xbins = dict(start = 0
                                     , end = 165000000
                                     , size = 1500000))
        fig.show()
        pass

        fig = px.histogram(plot_data
                         , x = 'PIB 2010 (R$)'
                         , nbins = 100
                         , width = 550
                         , height = 275
                         , color_discrete_sequence = self.plot_palette)
        fig.update_layout(yaxis_title = 'Frequency'
                        , margin = {'l' : 0
                                  , 'r' : 0
                                  , 't' : 0
                                  , 'b' : 0}
                        , bargap = 0.01)
        fig.update_traces(xbins = dict(start = 0
                                     , end = 2000000000
                                     , size = 25000000))
        fig.show()
        pass

        fig = px.histogram(plot_data
                         , x = 'Carga Tributária Municipal 2010'
                         , nbins = 100
                         , width = 550
                         , height = 275
                         , color_discrete_sequence = self.plot_palette)
        fig.update_layout(yaxis_title = 'Frequency'
                        , margin = {'l' : 0
                                  , 'r' : 0
                                  , 't' : 0
                                  , 'b' : 0}
                        , bargap = 0.01)
        fig.update_traces(xbins = dict(start = 0
                                     , end = 0.8
                                     , size = 0.0075))
        fig.show()
        pass

    def plot_correlation_heatmap(self):
        corr_matrix = plot_data[['IDHM 2010'
                               , 'PIB 2010 (R$)'
                               , 'Receitas Correntes 2010 (R$)'
                               , 'Carga Tributária Municipal 2010']].corr(method = 'pearson')
        corr_matrix = corr_matrix.mask(np.triu(np.ones_like(corr_matrix
                                                          , dtype = bool))).round(2)

        # plotting the correlation heatmap
        fig = px.imshow(corr_matrix
                      , text_auto = True
                      , color_continuous_scale = 'PuRd')
        custom_hover_template = '<br>'.join(['X: %{x}'
                                           , 'Y: %{y}'
                                           , 'Correlation: %{z}'])
        fig.update_layout(title_text = 'Correlation'
                        , title_x = 0.5
                        , width = 550
                        , height = 275
                        , margin = {'l' : 0
                                  , 'r' : 0
                                  , 't' : 0
                                  , 'b' : 0})
        fig.update_traces(xgap = 1
                        , ygap = 1
                        , hovertemplate = custom_hover_template)
        fig.show()
        pass

    def plot_bubble_chart(self):
        fig = px.scatter(plot_data
                       , x = 'Carga Tributária Municipal 2010'
                       , y = 'IDHM 2010'
                       , size = 'Habitantes 2010'
                       , hover_name = 'Município'
                       , trendline = 'lowess'
                       , width = 550
                       , height = 275
                       , color_discrete_sequence = self.plot_palette)
        fig.update_layout(margin = {'l' : 0
                                  , 'r' : 0
                                  , 't' : 0
                                  , 'b' : 0})
        fig.show()
        pass

class Mapper:
    def __init__(self
               , app_data : gpd.GeoDataFrame):
        self.app_data = app_data

    def create_map(self):
        mapa = folium.Map(location = [-14, -53.25]
                        , zoom_start = 4
                        , tiles = None)

        # Define style and highlight functions for hover functionality
        style_function = lambda x : {'fillColor' : '#ffffff'
                                   , 'color' : '#000000'
                                   , 'fillOpacity' : 0.1
                                   , 'weight' : 0.1}
        highlight_function = lambda x : {'fillColor' : '#000000'
                                       , 'color' : '#000000'
                                       , 'fillOpacity' : 0.50
                                       , 'weight' : 0.1}

        # Data format for tooltip
        app_data['Formatted Carga Tributária'] = app_data['Carga Tributária Municipal 2010'].apply(lambda x : '{:.3%}'.format(x))

        # Add Choropleth Layer
        folium.Choropleth(geo_data = app_data.__geo_interface__
                        , data = app_data
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
        tooltip_function = folium.features.GeoJson(data = app_data
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

        # Add Striped Pattern for Incomplete Data
        incomplete = app_data[app_data['data_status'] == 'incomplete']
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

        # Dark and Light displays switcher
        folium.TileLayer('cartodbpositron'
                       , name = 'Light mode'
                       , control = True).add_to(mapa)
        folium.TileLayer('cartodbdark_matter'
                       , name = 'Dark mode'
                       , control = True).add_to(mapa)

        # Layer Control Setup
        folium.LayerControl(collapsed = False
                          , position = 'topright').add_to(mapa)

        mapa.add_child(tooltip_function)
        mapa.keep_in_front(tooltip_function)
        mapa.save('mapa.html')
        display(mapa)
        pass

if __name__ == '__main__':
    db = Database()
    fetcher = DataFetcher(db)
    data = fetcher.fetch_data()  # This now returns a DataFrame
    geodata = fetcher.fetch_geodata()
    app_data = DataMerger.merge_data(data
                                      , geodata)
    plot_data = app_data[app_data['data_status'] == 'complete']

    visualizer = Visualizer(app_data
                          , plot_data)
    visualizer.plot_histograms()
    visualizer.plot_correlation_heatmap()
    visualizer.plot_bubble_chart()
    
    mapper = Mapper(app_data)
    mapper.create_map()
# %%
