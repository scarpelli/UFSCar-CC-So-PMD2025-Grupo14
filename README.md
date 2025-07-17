# 🍷 Harmonização de vinhos e pratos

**Bacharelado em Ciência da Computação**  
**Universidade Federal de São Carlos**  
**Disciplina:** Processamento Massivo de Dados

---

## 👥 Grupo 14

- **Bruna Scarpelli**
- **Gustavo Sanches Martins Kis**
- **Ricardo Yugo Suzuki**

---

## 📋 Sumário

- [Introdução](#introdução)
- [Objetivos](#objetivos)
- [Metodologia](#metodologia)
- [Tecnologias utilizadas](#tecnologias-utilizadas)
- [Fontes de dados](#fontes-de-dados)
- [Arquitetura do sistema](#arquitetura-do-sistema)
- [Modelo de dados](#modelo-de-dados)
- [Limitações](#limitações)
- [Resultados](#resultados)
- [Conclusão](#conclusão)

---

## 🎯 Introdução

A harmonização entre vinhos e pratos é uma arte complexa que envolve a análise de múltiplos fatores como ingredientes, métodos de preparo, acidez, corpo, taninos e notas aromáticas. Com a vasta variedade de receitas e rótulos disponíveis, encontrar combinações harmoniosas pode ser desafiador.

Este projeto propõe uma solução baseada em dados para facilitar o processo de harmonização, utilizando análise molecular de ingredientes e características de vinhos para gerar recomendações personalizadas.

---

## 🎯 Objetivos

O projeto foi criado com o objetivo de facilitar a harmonização de pratos com vinhos por meio dos sabores que eles possuem. Isto pode ser feito por meio de uma análise molecular dos ingredientes de um prato e por meio do sabor do vinho. Tendo estas informações, buscamos encontrar o maior número de sabores similares entre um prato e um vinho, criando assim uma harmonia no paladar. Logo, serão desenvolvidas, durante o trabalho, análises das melhores harmonizações de acordo também com a faixa de preço dos vinhos, encontrando os melhores vinhos dentro da faixa de preço estipulada pelo usuário.
  
**Objetivo principal:**
- Desenvolver um sistema que facilite a harmonização de pratos com vinhos baseado em análise molecular de sabores.

**Objetivos específicos:**
- Construir um banco de dados de grafos integrando receitas, vinhos e perfis moleculares
- Criar consultas para descobrir harmonizações ótimas
- Desenvolver análises considerando faixas de preço dos vinhos

---

## 🔬 Metodologia

Para o desenvolvimento do projeto, o primeiro passo foi encontrar datasets específicos para o desenvolvimento das respectivas tabelas. Com isso selecionamos datasets de vinhos e receitas que possuíam as informações de nosso interesse. No entanto, os dados moleculares de ingredientes não estavam dentro de um único arquivo, sendo necessária a criação de um script que uniu todas essas informações em um dataset de ingredientes. Este script baixava cada página de ingrediente em um JSON próprio, sendo elas unidas em um dataset.

Após a ingestão dos dados, foi preciso fazer um tratamento específico em cada dataset, para normalizá-los, permitindo uma modelagem final em Neo4j consistente. Para isto, foi necessária uma padronização na coluna de ingredientes no dataset de receitas, onde foram removidos ingredientes que não estavam presentes no dataset de moléculas. Com isso, foi possível remover receitas que não apresentavam ao mínimo 3 ingredientes, visto que com uma baixa quantidade de dados não seria possível alcançar a melhor combinação de valores possível. 

Além disso, foi necessário fazer um tratamento similar com o dataset de vinhos. No entanto, essa padronização foi feita na coluna de sabores do vinho, sendo esta padronizada de acordo com o sabor que as moléculas obtidas possuíam. 

Por fim, para que tudo isto fosse possível, por meio do dataset de ingredientes e suas moléculas foram criadas duas tabelas intermediárias, sendo uma com os ingredientes e suas moléculas e outra específica para as moléculas existentes e seus sabores.

Finalmente, os dados foram persistidos em tabelas intermediárias no Databricks Catalog, a fim de facilitar a integração com o Neo4j. A partir dessas tabelas, foram criados nós e relacionamentos no Neo4j, onde as análises em Cypher ocorreram.

### 1. **Ingestão de dados**
- Carregamento e integração de três datasets distintos
- Extração de dados do FlavorDB2 via script
- Validação e limpeza inicial dos dados

### 2. **Processamento com Apache Spark**
- **Receitas**: Normalização de ingredientes e padronização de colunas
- **Vinhos**: Limpeza de dados e conversão de preços (£ → R$)
- **Moléculas**: Divisão em tabelas intermediárias (ingredients e molecules)
- Transformação de tipos de dados e criação de relacionamentos

### 3. **Modelagem em grafo (Neo4j)**
- Design do modelo de dados orientado a grafos
- Implementação de nós e relacionamentos
- Otimização de consultas Cypher

---

## 🛠️ Tecnologias utilizadas

| Tecnologia | Descrição | Uso no projeto |
|------------|-----------|----------------|
| **Apache Spark** | Framework de processamento distribuído | Processamento e transformação de grandes volumes de dados |
| **Neo4j** | Banco de dados orientado a grafos | Modelagem e armazenamento de relações complexas |
| **Cypher** | Linguagem de consulta para grafos | Descoberta de harmonizações e análises |
| **Databricks** | Plataforma de análise de dados | Ambiente de desenvolvimento e processamento |

---

## 📊 Fontes de dados

### 1. **Recipe Dataset (over 2M)**
- **Fonte**: [Kaggle](https://www.kaggle.com/datasets/wilmerarltstrmberg/recipe-dataset-over[2m])
- **Atributos**: title, ingredients, directions, link, source, NER
- **Volume**: Mais de 2 milhões de receitas

### 2. **Wine Dataset**
- **Fonte**: [Kaggle](https://www.kaggle.com/datasets/elvinrustam/wine-dataset)
- **Atributos**: wine_id, name, description, price, capacity, grape, country, region
- **Características**: Inclui avaliações e notas de degustação

### 3. **FlavorDB2**
- **Fonte**: [CoSyLab IIITD](https://cosylab.iiitd.edu.in/flavordb2/)
- **Atributos**: category, entity_alias, molecules, natural_source_name
- **Funcionalidade**: Perfis moleculares de ingredientes e sabores

---

## 🏗️ Arquitetura do sistema

```
[Datasets] → [Apache Spark] → [Processamento] → [Neo4j] → [Consultas Cypher]
```

### Fluxo de dados:
1. **Extração**: Carregamento dos datasets brutos
2. **Transformação**: Limpeza e padronização com Spark
3. **Carga**: Inserção no grafo Neo4j
4. **Análise**: Consultas Cypher para harmonizações

![](notebooks.png)

---

## 🗂️ Modelo de dados

O modelo de dados foi criado com a intenção de conectar receitas e vinhos por meio de seus sabores. Dessa forma, cada receita possui ingredientes, estes ingredientes podem ter diversas moléculas e as moléculas tem sabores. Olhando pelo outro lado, o nó vinho tem suas características, que são o sabor, logo vinhos também tem sabores. Com isso, as consultas visam encontrar vinhos e ingredientes ou receitas que possuem o maior número de nós de sabor conectando eles, resultando na harmonização ótima. Além disso, existem nós de faixa de preço do vinho e de suas uvas, podendo também essa escolha ser feito por uva preferida ou por faixa de preço que deseja gastar na compra. 

Para uma melhor compreensão as conexões podem ser vistas no diagrama acima.

### **Nós principais:**
- `:Recipe` - Receitas culinárias
- `:Wine` - Vinhos e suas características
- `:Ingredient` - Ingredientes dos pratos
- `:Molecule` - Moléculas aromáticas
- `:Grape` - Variedades de uvas
- `:Flavor` - Perfis de sabor
- `:PriceRange` - Faixas de preço

### **Relacionamentos:**
- `[:CONTAINS_INGREDIENT]` - Receita contém ingrediente
- `[:CONTAINS_MOLECULE]` - Ingrediente contém molécula
- `[:MADE_FROM]` - Vinho feito de uva
- `[:HAS_FLAVOR_PROFILE]` - Elemento tem perfil de sabor
- `[:HAS_CHARACTERISTICS]` - Vinho possui características
- `[:IN_PRICE_RANGE]` - Vinho está em faixa de preço

---

## ⚠️ Limitações

### **Limitações técnicas:**
- **Versão gratuita do Neo4j**: Restrição no número de nós e relacionamentos
- **Processamento de dados**: Remoção significativa de dados durante a normalização
- **Compatibilidade**: Apenas ingredientes presentes no FlavorDB2 foram mantidos

### **Limitações funcionais:**
- **Análise incompleta**: Não considera corpo do vinho, textura do prato e acidez

---

## 📈 Resultados

### **Entregas:**
- ✅ Sistema funcional de harmonização baseado em análise molecular
- ✅ Banco de dados de grafos integrado com três fontes de dados
- ✅ Algoritmo de recomendação considerando faixas de preço
- ✅ Relatórios com top-10 harmonizações para pratos predefinidos

---

## 🏁 Conclusão

O projeto desenvolveu um sistema de harmonização de vinhos e pratos baseado em análise molecular. Apesar das limitações técnicas e funcionais identificadas, o sistema demonstra a viabilidade de usar tecnologias de Big Data e bancos de grafos para resolver problemas complexos de recomendação.

---

## 📖 Fundamentação teórica


* **[Apache Spark](https://spark.apache.org/docs/latest/sql-programming-guide.html)**: Usado para processar e transformar os dados dos arquivos CSV.

* **[Neo4j (Banco de Dados de Grafo)](https://neo4j.com/developer/graph-database/)**: Utilizado para modelar e armazenar as conexões entre vinhos, ingredientes e moléculas.

* **[Cypher (Linguagem de Consulta)](https://neo4j.com/docs/cypher-refcard/current/)**: Usada para buscar e descobrir as harmonizações dentro do grafo de dados.