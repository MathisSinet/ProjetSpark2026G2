import json
import os 
from pyspark.sql import functions as F
from graphframes import GraphFrame
from spark_core import df, SCHEMA  # lfux de données et schema


JSON_OUTPUT_PATH = "graph_data.json"

def process_batch_graph(batch_df, batch_id):
    if batch_df.isEmpty():
        return 
    
    print(f"\n---- [Batch {batch_id}] Analyse du Graphe en cours----")

    # Création des sommets
    # On extrait les utilisateurs, produits et vendeurs du flux pour en faire des noeuds

    users = (batch_df
             .select(F.col("user_id").alias("id"), F.col("user_city").alias("label"))
             .distinct()
             .withColumn("type", F.lit("user")))
    

    products = (batch_df
             .select(F.col("product_id").alias("id"), F.col("product_cat").alias("label"))
             .distinct()
             .withColumn("type", F.lit("product")))
    

    sellers = (batch_df
             .select(F.col("seller_id").alias("id"), F.col("seller_id").alias("label"))
             .distinct()
             .withColumn("type", F.lit("seller")))
    
    vertices = users.union(products).union(sellers).distinct()

    #Création des aretes

    #Utilisateurs -> Produits (AIME, VOUT, ACHAT)
    edges_user_prod = batch_df.select(
        F.col("user_id").alias("src"),
        F.col("product_id").alias("dst"),
        F.col("action_type").alias("action")
    )

    # Vendeurs -> Produits (PROPOSE)
    edges_seller_prod = batch_df.select(
        F.col("seller_id").alias("src"),
        F.col("product_id").alias("dst")
        ).withColumn("action",F.lit("PROPOSE"))


    edges = edges_user_prod.union(edges_seller_prod).distinct()


    # Création du graph complet

    g = GraphFrame(vertices, edges)
    degrees_df = g.degrees

    vertices_enriched = g.vertices.join(degrees_df, "id", "left").fillna(0, subset=["degree"])

    local_vertices = vertices_enriched.collect()
    local_edges = g.edges.collect()

    cytoscape_elements = []

    for row in local_vertices:
        cytoscape_elements.append({
            'data' : {
                'id' : row['id'],
                'label' : f"{row['label']} (Connexions: {row['degree']})"
            },
            'classes': row['type']
        })


    for row in local_edges:
        cytoscape_elements.append({
            'data': {
                'source': row['src'],
                'target': row['dst'],
                'action': row['action']
            }
        })

    with open(JSON_OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(cytoscape_elements, f, ensure_ascii=False, indent=4)


    print(f" Graphe sauvegardé avec succès ({len(local_vertices)} noeuds analysés)")



writer = (
    df
    .withWatermark("timestamp", "10 minutes")
    .writeStream
    .trigger(processingTime="5 seconds")
    .foreachBatch(process_batch_graph)
    .outputMode("update")
)


def start_spark_graph():
    print("Démarrage du moteur de Graphes temps réel...")
    query = writer.start()
    query.awaitTermination()

if __name__ == "__main__":
    start_spark_graph()