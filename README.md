# 🚀 Projet PySpark Streaming & Graphes

Pipeline Big Data en temps réel pour analyser les interactions (clics, intentions d'achat, ventes) d'une plateforme style *LeBonCoin*. Les données sont traitées au fil de l'eau pour mettre à jour un graphe de relations.

## 🛠️ Architecture
`generateurJSON.py` (Flux JSON) ➡️ `spark_core.py` (Streaming + GraphFrames) ➡️ `dashboard.py` (Visualisation)

## 📂 Fichiers
* `generateurJSON.py` : Simulateur de flux.
* `spark_core.py` : Traitement Spark.
* `dashboard.py` : Interface graphique.
* `main.py` : Point d'entrée pour tout lancer.

---

## 🚀 Comment lancer le projet

1. **Installer uv** :

   ###### Linux & macOS
   ```bash
   curl -LsSf https://astral.sh/uv/install.sh | sh
   ```
   Relancer le terminal pour que `uv` deviens disponible.

2. **Créer un environnement virtuel** :
   ```bash
   uv venv
   ```
3. **Activer l'environnement**
   ###### Linux & macOS
   ```bash
   source .venv/bin/activate  .venv\Scripts\activate      # Windows
   ```
   ###### Windows
   ```bash
     .venv\Scripts\activate
   ```

4. **Lancer le programme**
```bash
   uv run main.py
   ```
