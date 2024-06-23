# Collection and Process of IPEA public data using python: Redoing a Scientific Article project

[![pt-br](https://img.shields.io/badge/lang-pt--br-green.svg)](https://github.com/puffdapaz/pythonIPEA/blob/main/README.pt-BR.md)

[![App](https://img.shields.io/badge/Streamlit-FF4B4B.svg?style=for-the-badge&logo=Streamlit&logoColor=white)](https://ipeapython.streamlit.app)

### [Full Article](https://github.com/puffdapaz/pythonIPEA/blob/main/Impacto%20da%20receita%20tributária%20no%20desenvolvimento%20econômico%20e%20social.%20um%20estudo%20nos%20municípios%20brasileiros.pdf)

## Impact of tax revenue on economic and social development: a study in Brazilian municipalities
- Municipal Human Development Index 2010;
- Current Revenue 2010;
- Municipal Gross Domestic Product 2010;
- Current Revenue 2010 / GDP 2010 = Tax Burden 2010.

### [Data Source](http://www.ipeadata.gov.br/Default.aspx)

### Increments
- Expand research to all possible municipalities;
- Include dashboard with data visualization;
- Document and publish it.

## The Project
The aim of the project is to learn and improve the use of python for data engineering, analysis and science through a research carried out on 2015 in a scientific article, as a reference.

In addition to explore good practices in python, the purpose is to apply concepts from Medallion Architecture, object-oriented programming and ETL, using public social data from Brazilian municipalities.

## The Code
With the intends to incorporate usage of tool's features and best practices in code construction, an initial approach was taken and kept for record [IPEAv1.py](https://github.com/puffdapaz/pythonIPEA/blob/main/IPEAv1.py), but was modified with improvement in sophistication and data visualization.

1. The workflow begins with directories creation, that emulate the Medallion architecture layers - Bronze, Silver and Gold - progressively increasing the structure and quality of the saved data, and an additional folder to the results and analysis.
2. In this project, five data series are searched, already filtered by the year 2010\*, in the IPEAdata database (public database of the Institute of Applied Economic Research, a federal public foundation linked to the Ministry of Planning and Budget, in Brazil):
     - Four of them come from python library ['ipeadatapy'](https://pypi.org/project/ipeadatapy/)<br/>
         Three of the series lists available:<br/>
             GDP;<br/>
             Population; (It was sample filter criteria on the original project)<br/>
             Current Revenue;<br/>
         One from the list of territories:<br/>
             Municipalities.<br/>
     - One of them comes from the R package ['ipeadatar'](https://cran.r-project.org/web/packages/ipeadatar/index.html) because it is not available at municipal granularity in the python library.<br/>
         IDHM.<br/>
The fetched series are saved as DataFrame in their original/raw structure at the Bronze layer.<br/>
\* The IDHM series has a date field conversion for 2010 year filtering, using a different fetch structure from the python library.
3. The DataFrames go through a transformation process, filtering only occurrences by municipality, carrying out DataType conversions, renaming fields and removing unused fields and eventual duplication of occurrences.<br/>
The transformed DataFrames are saved at the Silver layer.
4. At this stage, data from previously treated variables are consolidated into a single Dataframe, gathering (through the Municipal Code established by [IBGE - Brazilian Institute of Geography and Statistics](https://servicodados.ibge.gov.br/api/docs/)) the names of the municipalities and other selected variables.<br/>
Next, there is removal of occurrences that do not contain all variables, fields reordering, occurrences sort (by Municipal Codes, ascending) and a creation of a calculated 'Tax Burden' field composed by the ratio between municipal Current Revenues and GDP.<br/>
The DataFrame ready for analysis is saved at the Gold layer.
5. From the finished DataFrame, the following applies:<br/>
- Summary of basic descriptive statistics;<br/>
- Correlation matrix using Pearson method;<br/>
- Ordinary Least Squares Regression;<br/>
- Variance Analysis (ANOVA).<br/>
The descriptive summary is saved in parquet format; the other statistical model results are saved in a single HTML file; both in the Statistical Analysis folder.
6. Then the obtaining of municipal geographic polygons through [geobr](https://pypi.org/project/geobr/) and again through the Municipal Code established by IBGE, the consolidation of socioeconomic information centralized on the DataFrame saved in Gold layer , with geographic coordinates.
7. After finishing the data processing, are displayed, histograms of the selected variables, a scatter plot, using IDHM and Tax Burden, with a trendline, a correlation heatmap, and the map.
8. The scripts and its results files are saved in this [GitHub](https://github.com/puffdapaz/pythonIPEA) repository, and processed through Streamlit for data plot.

## Methods
### Correlation Matrix (Pearson)
- *Variables: MHDI 2010, Tax Burden, GDP 2010, Current Revenue 2010;*
### Ordinary Least Squares Regression (OLS)
- *Predictors: (Constant), Tax Burden, Current Revenue 2010, GDP 2010;*<br/>
- *Dependent Variable: MHDI 2010.*
### Variance Analysis (ANOVA)
- *Predictors: (Constant), Tax Burden, Current Revenue 2010, GDP 2010;*<br/>
- *Dependent Variable: MHDI 2010.*

## Results
The extension of the study to more municipalities reinforced the results obtained in the original research in 2015. <br/>
>"... in many cases there is resources availability to meet population needs, however, there is a lack of effectiveness in management of public expenditure, without showing proportional progress in social development indicators, just as, there are municipalities that have high indicators, without a large pool of resources.
><br/>
>The capacity to execute public expenditure is clearly not satisfactory, given the immediate need for tax regulatory reform and distribution of this revenue by the government, valuing social justice and the effective provision of public services. The Fiscal Responsibility Brazilian Law [(LC 101/2000)](https://www.planalto.gov.br/ccivil_03/leis/lcp/lcp101.htm) restricts public policy decisions, prioritizing macroeconomic balance over Social well-being"

The relationship between MHDI and tax burden is considered moderate and negative (-0.6). The HDI and GDP ratio with 0.12 shows that the relationship is weak and positive. <br/>
The least squares model presented a R² of 0.368, that is, GDP and tax burden explain 36.8% of the HDI value of each municipality. <br/>
The F test with a result of 1602 and significance of 0.00 allows us to infer that the model is significant at a 5% confidence level. The variables tax burden with -55.399 and GDP with 5.869 allow us to reject the null hypothesis; both with significance of 0.00. <br/>