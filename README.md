# Harmonização de vinhos e pratos  
**Grupo 14 | Ciência da Computação | UFSCar Sorocaba**

## 1. Objetivo  
- Construir um sistema de recomendação para harmonização de vinhos e pratos.  
- Utilizar processamento para identificar as melhores combinações com base em perfis de sabor.

## 2. Tecnologias  
- **Processamento:** Apache Spark  
- **Banco de dados:** Neo4j (Grafo)  
- **Plataforma:** Databricks

## 3. Metodologia  
1. **Ingestão de dados**  
   - Carregamento de três datasets CSV:  
     - `recipes.csv`: detalhes de receitas e ingredientes  
     - `wines.csv`: características de vinhos e uvas  
     - `flavor_profiles.csv`: mapeamento de sabores para ingredientes e uvas  
2. **Processamento com Spark**  
   - Limpeza e padronização dos dados  
   - Enriquecimento dos datasets para criar “vetores de sabor” para cada receita e vinho  
3. **Modelagem em grafo (Neo4j)**  
   - Carga dos dados processados em um grafo  
   - **Nós principais:**  
     - `:Recipe`  
     - `:Wine`  
     - `:Ingredient`  
     - `:Grape`  
     - `:Flavor`  
   - **Relações:**  
     - `[:USES]`  
     - `[:MADE_FROM]`  
     - `[:HAS_FLAVOR]`

## 4. Resultado esperado  
- Geração de um relatório com o top‑N de harmonizações entre vinhos e pratos.  
- Recomendações extraídas via consultas Cypher.
