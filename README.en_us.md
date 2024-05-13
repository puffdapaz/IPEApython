# Collection and Process of IPEA public data using Python: Redoing the Undergraduate Scientific Article project

[![pt-br](https://img.shields.io/badge/lang-pt--br-green.svg)](https://github.com/puffdapaz/pythonIPEA/blob/58a7074737df5de5b85b8b66311f1e299fbd77e6/README.pt-br.md)

### [Full Article](https://github.com/puffdapaz/TCC/blob/66a3e445755dc30225056ef4bb92fabd85f85d14/Impacto%20da%20receita%20tribut%C3%A1ria%20no%20desenvolvimento%20econ%C3%B4mico%20e%20social.%20um%20estudo%20nos%20munic%C3%ADpios%20brasileiros.pdf)

## Impact of tax revenue on economic and social development: a study in Brazilian municipalities
- Municipal Human Development Index 2010;
- Current Revenue 2010;
- Municipal Gross Domestic Product 2010;
- Current Revenue 2010 / GDP 2010 = Tax Burden 2010.

### [Data Source](http://www.ipeadata.gov.br/Default.aspx)

### Increments
- Expand research to all possible municipalities;
- Include dashboard/visuals;
- Document and publish it.

## The Project
The aim of the project is to learn and improve the use of python for data engineering, analysis and science through a research carried out in 2015 in a scientific article, as a reference.

In addition to exploring good practices in python, the purpose is to apply concepts from Medallion Architecture, object-oriented programming and ETL, using public social data from Brazilian municipalities. This repository also has three versions of code that execute the same process, with improvement in sophistication at each version.

## The Code
All three script versions execute the same process generating the same result.
The first contains an initial approach to python, the second uses more of the tool's features, and the third intends to incorporate best practices in code construction.

1. The workflow begins with directories creation, that emulate the Medallion architecture layers - Bronze, Silver and Gold - progressively increasing the structure and quality of the saved data.
2. In this analysis, five data series are searched, already filtered by the year 2010\*, in the IPEAdata database (public database of the Institute of Applied Economic Research, a federal public foundation linked to the Ministry of Planning and Budget, in Brazil):
     - Four of them come from python library ['ipeadatapy'](https://pypi.org/project/ipeadatapy/)
         Three of the series lists available:
             GDP;
             Population; (It was sample filter criteria in the original project)
             Current Revenue;
         One from the list of territories:
             Municipalities.
     - One of them comes from the R package ['ipeadatar'](https://cran.r-project.org/web/packages/ipeadatar/index.html) because it is not available at municipal granularity in the python library.
         IDHM.
The obtained series are saved as DataFrame in their original/full structure in Bronze layer.
\* The IDHM series has a date field conversion for 2010 year filtering, using a different fetch structure from the python library.
3. The DataFrames go through a transformation process, filtering only occurrences by municipality, carrying out DataType conversions, renaming fields and removing unused fields and possible duplication of occurrences.
The transformed DataFrames are saved in Silver layer.
4. At this stage, data from previously treated variables are consolidated into a single Dataframe, gathering (through the Municipal Code established by [IBGE - Brazilian Institute of Geography and Statistics](https://servicodados.ibge.gov.br/api/docs/)) the names of the municipalities and other selected variables.
Next, there is removal of occurrences that do not contain all variables, fields reordering, occurrences sort (by Municipal Codes, ascending) and a creation of a calculated 'Tax Burden' field composed by the ratio between municipal Current Revenues and GDP.
The DataFrame ready for analysis is saved at the Gold layer.

## Methods
### Model Summary (R, R², Adjusted R², Std error Estimate, Durbin-Watson)
- *Predictors: (Constant), Tax Burden;*
- *Predictors: (Constant), Tax Burden, Collection 2010;*
- *Predictors: (Constant), Tax Burden, Revenue 2010, GDP 2010;*
- *Dependent Variable: IDHM 2010.*
### ANOVA (Regression, Residual, Total)
- *Predictors: (Constant), Tax Burden;*
- *Predictors: (Constant), Tax Burden, Collection 2010;*
- *Predictors: (Constant), Tax Burden, Revenue 2010, GDP 2010;*
- *Dependent Variable: IDHM 2010.*