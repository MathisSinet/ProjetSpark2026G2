from pyspark.sql import SparkSession
from pyspark.sql.streaming.query import StreamingQuery
from pyspark.sql.types import StructType, StructField, TimestampType, StringType, DoubleType
import pyspark.sql.functions as F
from threading import Thread
import time

from graphframes import GraphFrame
import os
import json

OUTPUT_DIR = "streaming_data"
JSON_OUTPUT_PATH = "graph_data.json"


SCHEMA = StructType([
    StructField("timestamp", TimestampType(), False),
    StructField("user_id", StringType(), False),
    StructField("user_city", StringType(), False),
    StructField("product_id", StringType(), False),
    StructField("product_cat", StringType(), False),
    StructField("seller_id", StringType(), False),
    StructField("action_type", StringType(), False),
    StructField("price", DoubleType(), False)
])


spark = (
    SparkSession.builder
        .appName("SparkProjectG2")
        .master("local[*]")
        .config("spark.jars.packages", "graphframes:graphframes:0.8.4-spark3.5-s_2.13")
        .config("spark.executor.memory", "2g")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.default.parallelism", "8")
        .getOrCreate()
)

df = (
    spark.readStream
        .schema(SCHEMA)
        .json(OUTPUT_DIR)
)

def process_batch_graph(batch_df, batch_id):
    global JSON_OUTPUT_PATH

    if batch_df.isEmpty():
        return

    print(f"\n--- [Batch {batch_id}] Analyse du Graphe en cours ---")

    users = batch_df.select(F.col("user_id").alias("id"), F.col("user_city").alias("label")).distinct().withColumn("type", F.lit("user"))
    products = batch_df.select(F.col("product_id").alias("id"), F.col("product_cat").alias("label")).distinct().withColumn("type", F.lit("product"))
    sellers = batch_df.select(F.col("seller_id").alias("id"), F.col("seller_id").alias("label")).distinct().withColumn("type", F.lit("seller"))
    
    new_vertices = users.union(products).union(sellers).distinct()

    edges_user_prod = batch_df.select(F.col("user_id").alias("src"), F.col("product_id").alias("dst"), F.col("action_type").alias("action"))
    edges_seller_prod = batch_df.select(F.col("seller_id").alias("src"), F.col("product_id").alias("dst")).withColumn("action", F.lit("PROPOSE"))
    
    new_edges = edges_user_prod.union(edges_seller_prod).distinct()

    if os.path.exists(JSON_OUTPUT_PATH):
        try:
            with open(JSON_OUTPUT_PATH, 'r', encoding='utf-8') as f:
                old_data = json.load(f)
            
            old_v_list = [(x['data']['id'], x['data']['label'].split(" (")[0], x['classes']) for x in old_data if 'source' not in x['data']]
            old_e_list = [(x['data']['source'], x['data']['target'], x['data']['action']) for x in old_data if 'source' in x['data']]
            
            if old_v_list:
                old_vertices_df = spark.createDataFrame(old_v_list, ["id", "label", "type"])
                new_vertices = new_vertices.union(old_vertices_df).distinct() # Maintenant, new_vertices existe bien au-dessus !
            if old_e_list:
                old_edges_df = spark.createDataFrame(old_e_list, ["src", "dst", "action"])
                new_edges = new_edges.union(old_edges_df).distinct()
        except Exception as e:
            print(f"Note : Impossible de fusionner avec l'ancien état, création initiale. ({e})")

    g = GraphFrame(new_vertices, new_edges)
    degrees_df = g.degrees
    vertices_enriched = g.vertices.join(degrees_df, "id", "left").fillna(0, subset=["degree"])

    local_vertices = vertices_enriched.collect()
    local_edges = g.edges.collect()

    cytoscape_elements = []

    for row in local_vertices:
        dynamique_size = 45 + (row['degree'] * 5)
        cytoscape_elements.append({
            'data': {
                'id': row['id'],
                'label': f"{row['label']} (Deg: {row['degree']})",
                'size': dynamique_size
                },
                'classes': row['type']
        })

    for row in local_edges:
        cytoscape_elements.append({
            'data': {'source': row['src'], 'target': row['dst'], 'action': row['action']}
        })

    with open(JSON_OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(cytoscape_elements, f, ensure_ascii=False, indent=4)
        
    print(f"Graphe sauvegardé avec succès ({len(local_vertices)} nœuds analysés)")

writer = (
    df
    .withWatermark("timestamp", "10 minutes")
    .writeStream
    .trigger(processingTime="5 seconds")
    .foreachBatch(process_batch_graph)
    .outputMode("update")
)

def stop_query_after(delay: float, query: StreamingQuery):
    time.sleep(delay)
    query.stop()

def spark_core(delay: float | None = None):
    query = writer.start()
    if delay is not None:
        Thread(target=stop_query_after, args = (30, query)).start()
    query.awaitTermination()

if __name__ == "__main__":
    spark_core()