import json
from pathlib import Path
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.streaming.query import StreamingQuery
from pyspark.sql.types import StructType, StructField, TimestampType, StringType, DoubleType
import pyspark.sql.functions as F
from queue import Queue
from threading import Thread
import time
from typing import Any

from graphframes import GraphFrame

OUTPUT_DIR = "streaming_data"
METRICS_DIR = Path("metrics")
WINDOW_DURATION = "10 seconds"
WATERMARK_DELAY = "30 seconds"

graph: GraphFrame | None = None

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


def make_process_batch_graph(output_queue: Queue[dict[str, Any]] | None):
    def process_batch_graph(batch_df: DataFrame, batch_id: int):
        global graph

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

        if graph is None:
            graph = GraphFrame(new_vertices, new_edges)
        else:
            vertices: DataFrame = graph.vertices
            edges: DataFrame = graph.edges
            graph = GraphFrame(
                vertices.union(new_vertices).distinct(),
                edges.union(new_edges).distinct()
            )

        vertices: DataFrame = graph.vertices
        edges: DataFrame = graph.edges

        degrees_df = graph.degrees
        vertices_enriched = vertices.join(degrees_df, "id", "left").fillna(0, subset=["degree"])

        local_vertices = vertices_enriched.collect()
        local_edges = edges.collect()

        cytoscape_elements: list[dict[str, Any]] = []

        for row in local_vertices:
            dynamic_size = 45 + (row["degree"] * 3)
            cytoscape_elements.append({
                "data": {
                    "id": row["id"],
                    "label": f"{row['label']} (Deg: {row['degree']})",
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


def make_process_batch_window_stats(output_queue: Queue[dict[str, Any]] | None):
    def process_batch_window_stats(batch_df: DataFrame, batch_id: int):
        if batch_df.isEmpty():
            return

        print(f"\n--- [Batch {batch_id}] Agrégation temporelle des interactions ---")

        window_rows = (
            batch_df
            .orderBy("window", "action_type")
            .collect()
        )

        windows: list[dict[str, Any]] = []
        actions_by_window: dict[tuple[str, str], dict[str, Any]] = {}

        for row in window_rows:
            window_start = row["window"]["start"].isoformat()
            window_end = row["window"]["end"].isoformat()
            action_type = row["action_type"]
            count = row["count"]

            window_key = (window_start, window_end)
            if window_key not in actions_by_window:
                actions_by_window[window_key] = {
                    "window_start": window_start,
                    "window_end": window_end,
                    "actions": {},
                    "total_actions": 0,
                }

            actions_by_window[window_key]["actions"][action_type] = count
            actions_by_window[window_key]["total_actions"] += count

        windows = [actions_by_window[key] for key in sorted(actions_by_window)]

        try:
            METRICS_DIR.mkdir(parents=True, exist_ok=True)
        except Exception as exc:
            print(f"[WARN] Impossible de créer le dossier {METRICS_DIR}: {exc}")
            return

        for window_payload in windows:
            # Deterministic name so each time window maps to one file.
            safe_start = window_payload["window_start"].replace(":", "-")
            safe_end = window_payload["window_end"].replace(":", "-")
            file_path = METRICS_DIR / f"window_{safe_start}__{safe_end}.json"

            try:
                file_path.write_text(
                    json.dumps(window_payload, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )
            except Exception as exc:
                print(f"[WARN] Impossible d'écrire {file_path}: {exc}")

    return process_batch_window_stats


def stop_queries_after(delay: float, queries: list[StreamingQuery]):
    time.sleep(delay)
    for query in queries:
        query.stop()


def spark_core(delay: float | None = None, queue: Queue[dict[str, Any]] | None = None):
    process_batch_graph = make_process_batch_graph(queue)
    process_batch_window_stats = make_process_batch_window_stats(queue)

    graph_writer = (
        df
        .writeStream
        .trigger(processingTime="10 seconds")
        .foreachBatch(process_batch_graph)
        .outputMode("append")
    )

    windowed_metrics = (
        df
        .withWatermark("timestamp", WATERMARK_DELAY)
        .groupBy(F.window("timestamp", WINDOW_DURATION), F.col("action_type"))
        .count()
    )

    metrics_writer = (
        windowed_metrics
        .writeStream
        .trigger(processingTime="10 seconds")
        .foreachBatch(process_batch_window_stats)
        .outputMode("update")
    )

    graph_query = graph_writer.start()
    metrics_query = metrics_writer.start()

    if delay:
        t = Thread(target=stop_queries_after, args=(delay, [graph_query, metrics_query]))
        t.start()
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    spark_core()