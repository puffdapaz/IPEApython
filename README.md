# Coleta e Tratamento de dados públicos do IPEA utilizando python: Refazendo o projeto de Artigo Científico de Graduação

### [Artigo Integral](https://github.com/puffdapaz/TCC/blob/66a3e445755dc30225056ef4bb92fabd85f85d14/Impacto%20da%20receita%20tribut%C3%A1ria%20no%20desenvolvimento%20econ%C3%B4mico%20e%20social.%20um%20estudo%20nos%20munic%C3%ADpios%20brasileiros.pdf)

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
O intuito do projeto é aprender e aperfeiçoar a utilização de python para engenharia, análise e ciência de dados através de uma pesquisa realizada em 2015 em artigo científico para graduação, como referência.

Além de explorar boas práticas em python, o propósito é de aplicar conceitos de Arquitetura Medallion, programação orientada a objetos e ETL, utilizando dados públicos sociais dos municípios brasileiros. Este repositório conta ainda com três versões de código que executam o mesmo processo, com incremento em sofisticação a cada versão.

## O Código
As três versões de script, executam o mesmo processo gerando o mesmo resultado. <br/>
A primeira contém uma abordagem inicial de python, a segunda se vale de mais recursos da ferramenta, e a terceira tem a intenção de incorporar as melhores práticas quanto à construção de código.

1. O fluxo inicia com a criação de diretórios que emulam as camadas de arquitetura Medallion - Bronze, Silver e Gold - incrementando progressivamente a estrutura e qualidade dos dados salvos.
2. Nesta análise cinco séries de dados são buscadas, já filtradas pelo ano de 2010, na base de dados do IPEAdata (base pública de dados do Instituto de Pesquisa Econômica Aplicada, fundação pública federal vinculada ao Ministério do Planejamento e Orçamento, do Brasil):
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

## Métodos
### Model Summary (R, R², Adjusted R², Std error Estimate, Durbin-Watson)
- *Predictors: (Constant), Carga Tributária;*
- *Predictors: (Constant), Carga Tributária, Arrecadação 2010;*
- *Predictors: (Constant), Carga Tributária, Arrecadação 2010, PIB 2010;*
- *Dependent Variable: IDHM 2010.*
### ANOVA (Regression, Residual, Total)
- *Predictors: (Constant), Carga Tributária;*
- *Predictors: (Constant), Carga Tributária, Arrecadação 2010;*
- *Predictors: (Constant), Carga Tributária, Arrecadação 2010, PIB 2010;*
- *Dependent Variable: IDHM 2010.*