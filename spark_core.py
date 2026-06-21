from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.streaming.query import StreamingQuery
from pyspark.sql.types import StructType, StructField, TimestampType, StringType, DoubleType
import pyspark.sql.functions as F
from graphframes import GraphFrame

import json
import time
from pathlib import Path
from multiprocessing import Queue
from threading import Thread
from typing import Any



# Dossier des données d'entrée
DATA_DIR = "streaming_data"
# Dossier des mesures de fenêtres
METRICS_DIR = Path("metrics")
# Durée des fenêtres
WINDOW_DURATION = "10 seconds"
# Délai à partir duquel Spark peut libérer des données pour le fenêtrage
WATERMARK_DELAY = "30 seconds"

graph: GraphFrame | None = None

# Schéma des données
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

# Session spark
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

spark.sparkContext.setLogLevel("ERROR")
spark.sparkContext.setCheckpointDir("checkpoint_dir")

# Dataframe de lecture des données
df = (
    spark.readStream
        .schema(SCHEMA)
        .json(DATA_DIR)
)


def make_process_batch_graph(output_queue: Queue[dict[str, Any]] | None):
    """Retourne la fonction qui permet d'analyser un batch pour le graphe visuel"""
    def process_batch_graph(batch_df: DataFrame, batch_id: int):
        """Fonction qui permet l'analyse d'un batch pour le graphe visuel.

        Prend en paramètre le DataFrame du batch et l'id du batch"""
        global graph

        if batch_df.isEmpty():
            return

        print(f"\n--- [Batch {batch_id}] Mise à jour Incrémentale du GraphFrame ---")

        # Mise à jour des utilisateurs, des produits et des vendeurs
        users = batch_df.select(F.col("user_id").alias("id"), F.col("user_id").alias("label")).distinct().withColumn("type", F.lit("user"))
        products = batch_df.select(F.col("product_id").alias("id"), F.col("product_id").alias("label")).distinct().withColumn("type", F.lit("product"))
        sellers = batch_df.select(F.col("seller_id").alias("id"), F.col("seller_id").alias("label")).distinct().withColumn("type", F.lit("seller"))

        # Nouveaux sommets
        new_vertices = users.union(products).union(sellers).distinct()

        # Arêtes entre les utilisateurs et les produits
        edges_user_prod = batch_df.select(F.col("user_id").alias("src"), F.col("product_id").alias("dst"), F.col("action_type").alias("action"))
        # Arêtes entre les vendeurs et les produits
        edges_seller_prod = batch_df.select(F.col("seller_id").alias("src"), F.col("product_id").alias("dst")).withColumn("action", F.lit("PROPOSE"))

        # Nouvelles arêtes
        new_edges = edges_user_prod.union(edges_seller_prod).distinct()

        # Mise à jour du graphe
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

        # Ajout du degré de chaque sommet
        degrees_df = graph.degrees
        vertices_enriched = vertices.join(degrees_df, "id", "left").fillna(0, subset=["degree"])

        # Sommets et arêtes finales à envoyer au Dash
        local_vertices = vertices_enriched.collect()
        local_edges = edges.collect()

        # Eléments du dashboard Cytoscape à envoyer
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

        # JSON des éléments à envoyer au processus dash
        payload = {
            "elements": cytoscape_elements,
            "vertex_count": len(local_vertices),
            "edge_count": len(local_edges)
        }

        # Envoi des données au processus dash en utilisant la file inter-processus
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


def make_process_batch_window_stats():
    """Retourne la fonction qui permet d'analyser un batch pour les statistiques temporelles."""

    def process_batch_window_stats(batch_df: DataFrame, batch_id: int):
        """Fonction qui permet d'analyser un batch pour produire les statistiques des fenêtres.

        Prend en paramètre le DataFrame du batch et l'id du batch"""

        if batch_df.isEmpty():
            return

        print(f"\n--- [Batch {batch_id}] Agrégation temporelle des interactions ---")

        # Récupère les lignes agrégées et les trie pour construire les fenêtres.
        window_rows = (
            batch_df
            .orderBy("window", "action_type")
            .collect()
        )

        windows: list[dict[str, Any]] = []
        # Agrège les actions pour chaque fenêtre
        actions_by_window: dict[tuple[str, str], dict[str, Any]] = {}

        # Récupère les statistiques des fenêtres depuis les données reçues
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
            # Crée le dossier de sortie des métriques si nécessaire.
            METRICS_DIR.mkdir(parents=True, exist_ok=True)
        except Exception as exc:
            print(f"[WARN] Impossible de créer le dossier {METRICS_DIR}: {exc}")
            return

        # Ecrit les fenêtres
        for window_payload in windows:
            # Génère un nom déterministe pour associer chaque fenêtre à un fichier.
            safe_start = window_payload["window_start"].replace(":", "-")
            safe_end = window_payload["window_end"].replace(":", "-")
            file_path = METRICS_DIR / f"window_{safe_start}__{safe_end}.json"

            # Ecrit les statistiques dans un fichier JSON
            try:
                file_path.write_text(
                    json.dumps(window_payload, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )
            except Exception as exc:
                print(f"[WARN] Impossible d'écrire {file_path}: {exc}")

    return process_batch_window_stats


def stop_queries_after(delay: float, queries: list[StreamingQuery]):
    """Permet d'arrêter toutes les requêtes après un délai donné"""

    time.sleep(delay)
    for query in queries:
        query.stop()


def spark_core(delay: float | None = None, queue: Queue[dict[str, Any]] | None = None):
    """Fonction principale du noyau Spark"""

    process_batch_graph = make_process_batch_graph(queue)
    process_batch_window_stats = make_process_batch_window_stats()

    # Requête d'écriture des graphes
    graph_writer = (
        df
        .writeStream
        .trigger(processingTime="10 seconds")
        .foreachBatch(process_batch_graph)
        .outputMode("append")
    )

    # Dataframe des statistiques des fenêtres
    windowed_metrics = (
        df
        .withWatermark("timestamp", WATERMARK_DELAY)
        .groupBy(F.window("timestamp", WINDOW_DURATION), F.col("action_type"))
        .count()
    )

    # Requête d'écriture des statistiques des fenêtres
    metrics_writer = (
        windowed_metrics
        .writeStream
        .trigger(processingTime="10 seconds")
        .foreachBatch(process_batch_window_stats)
        .outputMode("update")
    )

    # On lance les requêtes
    graph_query = graph_writer.start()
    metrics_query = metrics_writer.start()

    if delay:
        t = Thread(target=stop_queries_after, args=(delay, [graph_query, metrics_query]))
        t.start()
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    spark_core()