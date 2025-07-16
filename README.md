# Harmonização de vinhos e pratos

**Bacharelado em Ciência da Computação**  
**Universidade Federal de São Carlos**

### Grupo 14
- **Bruna Scarpelli**  
- **Gustavo Sanches Martins Kis**  
- **Ricardo Yugo Suzuki**

## Introdução
Escolher o vinho certo para acompanhar uma refeição nem sempre é tão simples. Além das preferências pessoais, é preciso levar em conta fatores como os ingredientes do prato, o método de preparo e as características do vinho (acidez, corpo, taninos e notas aromáticas). Diante da enorme variedade de receitas e rótulos disponíveis, a tarefa de encontrar harmonizações que funcionem pode ser complexa.

Para tornar esse processo mais acessível e baseado em dados, este projeto propõe a construção de um banco de dados, alimentado por informações de receitas e vinhos, com todos os seus atributos e conexões de harmonização. Utilizamos Neo4j para modelar as relações entre ingredientes e perfis de sabor dos vinhos, e Apache Spark para processar esses dados.

## Resumo

O trabalho consistiu na escolha de 3 diferentes datasets para a harmonização dos vinhos com pratos, sendo eles: dataset de receitas, dataset de vinhos e dataset de moléculas de ingredientes. Logo após isto, os dados foram tratados de modo a ter uma correlação entre os dataframes, sem tem inconsistência entre termos de diferentes dataframes. Com isso feito, o modelo foi criado no Neo4J e os dados do Databricks foram incluídos na modelagem de grafos. Por fim, foram feitas as análises de acordo com os dados obtidos.

## Objetivo

O objetivo deste projeto é conseguir facilitar a harmonização de pratos com os mais diversos tipos de vinhos por meio do sabor destes.

## Planejamento inicial

Inicialmente a ideia foi de usar dois datasets, um de vinho e outro de pratos e com base nisso criar um terceiro dataset do zero com os ingredientes dos pratos e as uvas dos vinhos, criando uma harmonia por meio do sabor e da intensidade de cada ingrediente. No entanto, foi percebido pelo grupo que não era uma ideia cabível criar um dataset inteiro do zero e também que o dataset de pratos escolhido não tinha os dados necessários para o projeto. Dessa forma, escolhemos outro dataset de receitas que tinha todas as informações necessárias e encontramos um dataset de moléculas de ingredientes e seus sabores. Com este dataset, a conexão entre os outros datasets foi permitida, criando assim um desenvolvimento mais condizente com a realidade.

## Fundamentação teórica

* **[Apache Spark](https://spark.apache.org/docs/latest/sql-programming-guide.html)**: Usado para processar e transformar os dados dos arquivos CSV.
* **[Neo4j (Banco de Dados de Grafo)](https://neo4j.com/developer/graph-database/)**: Utilizado para modelar e armazenar as conexões entre vinhos, ingredientes e moléculas.
* **[Cypher (Linguagem de Consulta)](https://neo4j.com/docs/cypher-refcard/current/)**: Usada para buscar e descobrir as harmonizações dentro do grafo de dados.

## Implementação

### Tecnologias escolhidas
- **Apache Spark**: framework de processamento distribuído, adotado para tratar grandes volumes de dados de forma eficiente e integrada ao ecossistema de Big Data.
- **Neo4j**: banco de dados orientado a grafos, ideal para explorar e consultar conexões entre perfis de sabor de vinhos e ingredientes de receitas.
- **Plataforma:** Databricks

### Fontes de dados
1. **[Recipe Dataset (over 2M) Food](https://www.kaggle.com/datasets/wilmerarltstrmberg/recipe-dataset-over[2m])**  
   - title
   - ingredients 
   - directions 
   - link 
   - source 
   - NER 

2. **[Wine Dataset](https://www.kaggle.com/datasets/elvinrustam/wine-dataset)**  
   - wine_id  
   - name  
   - description  
   - price  
   - capacity (ml ou cl)  
   - grape  
   - secondary_grape_varieties  
   - closure  
   - country  
   - region  
   - (quando disponíveis) avaliações ou notas de degustação

3. **[FlavorDB2](https://cosylab.iiitd.edu.in/flavordb2/)**  
   - category 
   - category_readable  
   - entity_alias  
   - entity_alias_basket
   - entity_alias_readable
   - entity_alias_synonyms
   - entity_alias_url
   - entity_id
   - molecules (apresenta todas as moléculas que cada ingrediente possui, com seus nomes, sabores, ids e pesos)
   - natural_source_name
   - natural_source_url

## Limitações 

As limitações tiveram início no tratamento dos dados. Como se tratavam de 3 datasets e os datasets de vinhos e receitas só poderiam ter ingredientes e sabores que estivessem presentes no dataset de moléculas, foi necessária um grande remoção e normalização de dados, algo que demandou bastante tempo e esforço.

Outra limitação é que não conseguimos expandir nossa harmonização para o corpo do vinho, a textura do prato e a acidez do vinho. Com isso, não conseguimos ter uma análise tão profunda em relação principalmente ao vinho, algo que não atrapalhou no resultado final, mas estava na ideia primária do grupo.

Por fim, como utilizamos a versão grátis do Neo4j, ele nos limitou a uma quantidade baixa de nós e relacionamentos, impedindo uma visão mais ampla de receitas.

## Conclusão

Foi criado um sistema funcional que cria harmonizações de vinhos e receitas com base em combinações moleculares. No entanto, ele apresenta limitações mais subjetivas, como por exemplo não tratar da acidez do vinho e da textura do prato. Além disso, como não foi utilizada a versão paga dos programas a variedade de pratos e vinhos se torna reduzida.


## Metodologia  
1. **Ingestão de dados**  
   - Carregamento de três datasets:  
     - `recipes.csv`: detalhes de receitas e ingredientes  
     - `wines.csv`: características de vinhos e uvas  
     - `flavor_profiles`: foi construido a partir de extração das páginas do flavordb2, usando o script flavordb2-downloader
2. **Processamento com Spark**  
   - Limpeza e padronização dos dados 
    - fizemos uma análise exploratória para definir o que seria transformado em cada dataset: para receitas, limpamos o nome dos ingredientes e renomeamos colunas; para o de vinhos, limpanos colunas e fizemos a conversão de preços de libra esterlina para reais; no de moleculas, dividimos os arquivos em duas tabelas intermediarias antes de ir para o neo4j: ingredients e molecules, as transformações principais foram nos tipos dos dados.
3. **Modelagem em grafo (Neo4j)**  
   - Carga dos dados processados em um grafo  
   - **Nós principais:**  
     - `:Recipe`  
     - `:Molecule` 
     - `:Wine`  
     - `:Ingredient`  
     - `:Grape`  
     - `:Flavor`  
     - `:PriceRange` 
   - **Relações:**  
     - `[:CONTAINS_INGREDIENT]`  
     - `[:CONTAINS_MOLECULE]`  
     - `[:MADE_FROM]`  
     - `[:HAS_FLAVOR_PROFILE]`
     - `[:HAS_CHARACTERISTICS]`
     - `[:IN_PRICE_RANGE]`
4. Diagrama de etapas: path relative to root: notebooks.png

## Resultado
- Geração de um relatório com o top‑N harmonizações para pratos predefinidos e faixas de preço de vinho.  
- Recomendações extraídas via consultas Cypher.
