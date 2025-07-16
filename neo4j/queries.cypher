// 1. Top 3 budget-friendly wines for “Croque Monsieur”
MATCH (r:Recipe {title:"Croque Monsieur"})
MATCH (r)-[:CONTAINS_INGREDIENT]->(:Ingredient)-[:CONTAINS_MOLECULE]->(m:Molecule)
MATCH (m)-[:HAS_FLAVOR_PROFILE]->(f:Flavor)<-[:HAS_CHARACTERISTIC]-(w:Wine)-[:IN_PRICE_RANGE]->(:PriceRange {name:"Budget"})
WITH w, count(DISTINCT f) AS Score
RETURN w.title AS Wine, w.price AS Price, Score
ORDER BY Score DESC, w.price ASC
LIMIT 3;

// 2. Premium wine for Baked Shrimp La Viata
MATCH (r:Recipe {title: "Baked Shrimp La Viata"})-[:CONTAINS_INGREDIENT]->(i:Ingredient)
MATCH (i)-[:CONTAINS_MOLECULE]->(m:Molecule)-[:HAS_FLAVOR_PROFILE]->(f:Flavor)
MATCH (w:Wine)-[:HAS_CHARACTERISTIC]->(f)
MATCH (w)-[:MADE_FROM]->(g:Grape)
OPTIONAL MATCH (w)-[:IN_PRICE_RANGE]->(p:PriceRange)
RETURN DISTINCT 
    w.title AS wine, 
    g.name AS grape, 
    p.name AS price_range,
    count(DISTINCT f) AS flavor_overlap
ORDER BY flavor_overlap DESC
LIMIT 1

// 3. Wines that match with recipes that contains Parmesan Cheese
MATCH (i:Ingredient {name: "parmesan cheese"})
      -[:CONTAINS_MOLECULE]->(m:Molecule)
      -[:HAS_FLAVOR_PROFILE]->(f:Flavor)
      <-[:HAS_CHARACTERISTIC]-(w:Wine)
MATCH (w)-[:IN_PRICE_RANGE]->(:PriceRange {name: "Budget"})
MATCH (w)-[:MADE_FROM]->(g:Grape)
RETURN DISTINCT 
    w.title       AS wine, 
    g.name        AS grape, 
    w.price       AS price, 
    count(DISTINCT f) AS flavor_overlap
ORDER BY flavor_overlap DESC, price DESC
LIMIT 10