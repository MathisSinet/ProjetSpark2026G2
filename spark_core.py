from pyspark.sql import SparkSession
from pyspark.sql.streaming.query import StreamingQuery
from pyspark.sql.types import StructType, StructField, TimestampType, StringType, DoubleType
import pyspark.sql.functions as F
from multiprocessing import queues
from threading import Thread
import time

from graphframes import GraphFrame

OUTPUT_DIR = "streaming_data"

g = None
graph_queue: queues.Queue | None = None

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
        .appName("SparkProjectG2_StatefulGraph")
        .master("local[*]")
        .config("spark.jars.packages", "graphframes:graphframes:0.8.4-spark3.5-s_2.13")
        .config("spark.executor.memory", "2g")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")
spark.sparkContext.setCheckpointDir("checkpoint_dir")

df = (
    spark.readStream
        .schema(SCHEMA)
        .json(OUTPUT_DIR)
)


def make_process_batch_graph(output_queue):
    def process_batch_graph(batch_df, batch_id):
        global g

        if batch_df.isEmpty():
            return

        print(f"\n--- [Batch {batch_id}] Mise à jour Incrémentale du GraphFrame ---")

        users = batch_df.select(F.col("user_id").alias("id"), F.col("user_id").alias("label")).distinct().withColumn("type", F.lit("user"))
        products = batch_df.select(F.col("product_id").alias("id"), F.col("product_id").alias("label")).distinct().withColumn("type", F.lit("product"))
        sellers = batch_df.select(F.col("seller_id").alias("id"), F.col("seller_id").alias("label")).distinct().withColumn("type", F.lit("seller"))

        new_vertices = users.union(products).union(sellers).distinct()

        edges_user_prod = batch_df.select(F.col("user_id").alias("src"), F.col("product_id").alias("dst"), F.col("action_type").alias("action"))
        edges_seller_prod = batch_df.select(F.col("seller_id").alias("src"), F.col("product_id").alias("dst")).withColumn("action", F.lit("PROPOSE"))

        new_edges = edges_user_prod.union(edges_seller_prod).distinct()

        if g is None:
            g = GraphFrame(new_vertices, new_edges)
        else:
            g = GraphFrame(
                g.vertices.union(new_vertices).distinct(),
                g.edges.union(new_edges).distinct()
            )

        degrees_df = g.degrees
        vertices_enriched = g.vertices.join(degrees_df, "id", "left").fillna(0, subset=["degree"])

        local_vertices = vertices_enriched.collect()
        local_edges = g.edges.collect()

        cytoscape_elements = []

        for row in local_vertices:
            dynamic_size = 45 + (row["degree"] * 3)
            cytoscape_elements.append({
                "data": {
                    "id": row["id"],
                    "label": f"{row['label']} (Global Deg: {row['degree']})",
                    "size": dynamic_size
                },
                "classes": row["type"]
            })

        for row in local_edges:
            cytoscape_elements.append({
                "data": {"source": row["src"], "target": row["dst"], "action": row["action"]}
            })

        payload = {
            "elements": cytoscape_elements,
            "vertex_count": len(local_vertices),
            "edge_count": len(local_edges)
        }

        if output_queue is not None:
            try:
                output_queue.put_nowait(payload)
            except Exception:
                try:
                    output_queue.get_nowait()
                except Exception:
                    pass
                output_queue.put_nowait(payload)

        print(f"-> Graphe Global mis à jour : {len(local_vertices)} nœuds et {len(local_edges)} arêtes stockés en RAM.")

    return process_batch_graph


def stop_query_after(delay: float, query: StreamingQuery):
    time.sleep(delay)
    query.stop()


def spark_core(delay: float | None = None, graph_state=None):
    process_batch = make_process_batch_graph(graph_state)
    writer = (
        df
        .withWatermark("timestamp", "10 minutes")
        .writeStream
        .trigger(processingTime="5 seconds")
        .foreachBatch(process_batch)
        .outputMode("update")
    )
    query = writer.start()
    if delay:
        t = Thread(target=stop_query_after, args=(delay, query))
        t.start()
    query.awaitTermination()


if __name__ == "__main__":
    spark_core()