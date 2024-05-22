# Coleta e Tratamento de dados públicos do IPEA utilizando python: Refazendo projeto de Artigo Científico

[![en-us](https://img.shields.io/badge/lang-en--us-red.svg)](https://github.com/puffdapaz/pythonIPEA/blob/main/README.en-US.md)

### [Artigo Integral](https://github.com/puffdapaz/pythonIPEA/blob/main/Impacto%20da%20receita%20tributária%20no%20desenvolvimento%20econômico%20e%20social.%20um%20estudo%20nos%20municípios%20brasileiros.pdf)

## Impacto da receita tributária no desenvolvimento econômico e social: um estudo nos municípios brasileiros
- IDHM 2010;
- Receitas Correntes 2010;
- Produto Interno Bruto Municipal 2010;
- Receitas Correntes 2010 / PIB 2010 = Carga Tributária 2010.

### [Fonte dos Dados](http://www.ipeadata.gov.br/Default.aspx)

### Incrementos
- Expandir estudo para todos os municípios possíveis;
- Incluir painel/visuais;
- Documentar e publicar.

## O Projeto
O intuito do projeto é aprender e aperfeiçoar a utilização de python para engenharia, análise e ciência de dados através de uma pesquisa realizada em 2015 em artigo científico, como referência.

Além de explorar boas práticas em python, o propósito é de aplicar conceitos de Arquitetura Medallion, programação orientada a objetos e ETL, utilizando dados públicos sociais dos municípios brasileiros. Este repositório conta ainda com duas versões de código que executam o mesmo processo, com incremento de sofisticação na última versão.

## O Código
As versões de script, executam o mesmo processo gerando o mesmo resultado. <br/>
A primeira contém uma abordagem inicial de python, e a última tem a intenção de incorporar as melhores práticas quanto à utilização da ferramenta e construção de código.

1. O fluxo inicia com a criação de diretórios que emulam as camadas de arquitetura Medallion - Bronze, Silver e Gold - incrementando progressivamente a estrutura e qualidade dos dados salvos, e uma pasta adicional para as análises e resultados.
2. Neste projeto cinco séries de dados são buscadas, já filtradas pelo ano de 2010, na base de dados do IPEAdata (base pública de dados do Instituto de Pesquisa Econômica Aplicada, fundação pública federal vinculada ao Ministério do Planejamento e Orçamento, do Brasil):
    - Quatro delas oriundas da biblioteca python ['ipeadatapy'](https://pypi.org/project/ipeadatapy/)<br/>
        Três das listas de séries disponíveis:<br/>
            PIB;<br/>
            População; (Era critério de filtro da amostra no projeto original)<br/>
            Receitas Correntes;<br/>
        Uma da lista de territórios:<br/>
            Municípios.<br/>
    - Uma delas oriunda do pacote R ['ipeadatar'](https://cran.r-project.org/web/packages/ipeadatar/index.html) por nao estar disponível em granularidade municipal na biblioteca python.<br/>
        IDHM.<br/>
As séries obtidas são salvas como DataFrame em sua estrutura original/integral na camada Bronze.<br/>
\* A série IDHM tem uma conversão de campo de data para filtro do ano de 2010, por estrutura de busca diferente da biblioteca python.
3. Os DataFrames passam por processo de transformação, filtrando somente as ocorrências por município, efetuando conversões de DataType, renomeação de campos e remoção de campos não utilizados e eventual duplicidade de ocorrências.<br/>
Os DataFrames transformados são salvos na camada Silver.
4. Nessa etapa, consolidam-se os dados das variáveis préviamente tratadas em um único Dataframe reunindo (através do Código de Município estabelecido pelo [IBGE - Instituto Brasileiro de Geografia e Estatística](https://servicodados.ibge.gov.br/api/docs/)) os nomes dos municípios e as demais variáveis selecionadas.<br/>
Na sequência, há a remoção de ocorrências que nao contenham todas as variáveis, reordenação dos campos, reordenação das ocorrências (com base nos Códigos de Município, de forma crescente) e a criação de uma coluna calculada de 'Carga Tributária' composta pela relação entre as Receitas Correntes e PIB municipais.<br/>
O DataFrame em condições para a análise é salvo na camada Gold.
5. Após a finalização do tratamento dos dados, são exibidos histogramas das variáveis selecionadas, um gráfico de dispersão, entre IDHM e Carga Tributária, contendo uma linha de tendência.<br/>
Os gráficos são salvos em um único arquivo em formato pdf na pasta de Análise Estatística.
6. A partir do DataFrame finalizado, aplica-se então:<br/>
- Sumário de estatísticas descritivas básicas;<br/>
- Matriz de correlação através do método de Pearson(com um gráfico de mapa de calor);<br/>
- Regressão por Mínimos Quadrados Ordinários;<br/>
- Análise de Variância (ANOVA).<br/>
O sumário descritivo é salvo em formato csv; o gráfico de mapa de calor é salvo no mesmo arquivo com os demais gráficos da etapa anterior, em formato pdf; e os resultados dos modelos estatísticos são salvos em um único arquivo HTML. Todos na pasta de Análise Estatística.

## Métodos
### Matriz de Correlação (Pearson)
- *Variáveis: IDHM 2010, Carga Tributária, PIB 2010;*
### Regressão por Mínimos Quadrados Ordinários (OLS)
- *Preditoras: (Constante), Carga Tributária, Arrecadação 2010, PIB 2010;*<br/>
- *Variável Dependente: IDHM 2010.*
### Análise de Variância (ANOVA)
- *Preditoras: (Constante), Carga Tributária, Arrecadação 2010, PIB 2010;*<br/>
- *Variável Dependente: IDHM 2010.*

## Resultados
A extensão do estudo a mais municípios reforçou os resultados obtidos na pesquisa original em 2015. <br/>
>"... em muitos casos há a disponibilidade de recursos para atender as necessidades da população, contudo, falta efetividade na gestão dos gastos públicos, sem mostrar avanço proporcional nos indicadores de desenvolvimento social, assim como existem municípios que possuem altos indicadores, sem serem grandes recebedores de recursos.
><br/>
>A capacidade de executar os gastos públicos claramente não é satisfatória, dada a constatação de imediata necessidade de reforma normativa tributaria e de distribuição dessa receita pelo governo, prezando pela justiça social e efetiva prestação de serviços públicos. A Lei de Responsabilidade Fiscal [(LC 101/2000)](https://www.planalto.gov.br/ccivil_03/leis/lcp/lcp101.htm) engessa as decisões de políticas públicas priorizando equilíbrio macroeconômico frente ao bem-estar social"

![scatterplot](https://github.com/puffdapaz/pythonIPEA/blob/main/scatterplot.png) ![heatmap](https://github.com/puffdapaz/pythonIPEA/blob/main/heatmap.png)

A relação entre IDH e Carga tributária é considerada moderada e negativa (-0,6). A relação IDH e PIB com 0,12 mostra que a relação é fraca e positiva. <br/>
O modelo de mínimos quadrados apresentou R² de 0,368, ou seja, o PIB e a carga tributária explicam 36,8% do valor de IDH de cada município. <br/>
O teste F com resultado de 1602 e significância de 0,00, permite inferir que o modelo é significativo a um grau de 5% de confiança. As variáveis carga tributária com -55,399 e PIB com 5,869 permitem rejeitar a hipótese nula; ambas com significância de 0,00. <br/>