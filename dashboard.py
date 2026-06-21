import dash
from dash import html, dcc, Input, Output
import dash_cytoscape as cyto
from queue import Empty, Queue
from threading import Thread
from typing import Any

from style_graphe import mon_style_cytoscape

# Initialisation de l'application Dash
app = dash.Dash(__name__)
# Initialisation des variables globales pour la communication avec l'orchestrateur (main.py)
graph_queue: Queue[dict[str, Any]] | None = None # File d'attente qui recevra les données de l'extérieur
cached_graph_elements: list[dict[str, Any]] | None = None # Variable de "cache" pour stocker le graphe actuel


def watch_graph_queue():
    global cached_graph_elements

    while True:
        if graph_queue is None:
            return # Arrêt de sécurité si la file d'attente n'existe pas

        try:
            # Récupère les données les plus récentes envoyées dans la Queue (attend maximum 1 seconde)
            latest_payload = graph_queue.get(timeout=1)
        except Empty:
            continue

        if latest_payload and latest_payload.get("elements"):
            cached_graph_elements = latest_payload["elements"]

# Données par défaut affichées au tout premier lancement du Dashboard
initial_elements = [
    {'data': {'id': 'usr_init', 'label': 'User_Init'}, 'classes': 'user'},
    {'data': {'id': 'sel_init', 'label': 'Boutique_Init'}, 'classes': 'seller'},
    {'data': {'id': 'prod_init', 'label': 'Article_Init'}, 'classes': 'product'},
    {'data': {'source': 'usr_init', 'target': 'prod_init', 'action': 'AIME'}}
]

# DESIGN ET STRUCTURE DE LA PAGE WEB (LAYOUT)
app.layout = html.Div([
    # En-tête de la page (Titres)
    html.Header([
        html.H1("📊 Dashboard Streaming LeBonCoin", 
                style={'textAlign': 'center', 'fontFamily': 'Helvetica, Arial, sans-serif', 'color': '#2C3E50', 'paddingTop': '15px'}),
        html.H4("Visualisation en temps réel des interactions", 
                style={'textAlign': 'center', 'color': '#7F8C8D', 'fontFamily': 'Helvetica, Arial, sans-serif', 'marginTop': '-10px', 'paddingBottom': '15px'})
    ], style={'backgroundColor': '#FFFFFF', 'boxShadow': '0px 4px 10px rgba(0,0,0,0.05)', 'marginBottom': '20px', 'borderRadius': '10px'}),

    # Composant principal : Le Graphe de connexions interactif
    cyto.Cytoscape(
        id='live-graph-leboncoin',
        elements=initial_elements, # Chargement des éléments de base
        stylesheet=mon_style_cytoscape, # Application du style importé (couleurs, formes, flèches)
        style={
            'width': '100%', 
            'height': '650px', 
            'backgroundColor': '#FDFEFE', 
            'borderRadius': '15px', 
            'boxShadow': '0px 0px 15px rgba(0,0,0,0.05)',
            'border': '1px solid #E5E8E8'
        },
        layout={'name': 'cose'} # Algorithme de placement automatique des bulles (espacement intelligent)
    ),

    # Chronomètre invisible (déclencheur temps réel) réglé sur 10 000 ms (10 secondes)
    dcc.Interval(
        id='interval-clock',
        interval=10_000, 
        n_intervals=0
    )
], style={'backgroundColor': '#ECF0F1', 'minHeight': '100vh', 'padding': '20px', 'margin': '-8px'})

# LOGIQUE DE RAFRAÎCHISSEMENT (CALLBACK)
@app.callback(
        Output('live-graph-leboncoin', 'elements'), # Cible : Modifie les éléments affichés sur le graphe
        Input('interval-clock', 'n_intervals') # Déclencheur : S'active à chaque tic du dcc.Interval (toutes les 10s)
)
def refresh_graph_automatically(n):
    global cached_graph_elements

    # Si notre cache a été mis à jour par la file, on affiche le nouveau graphe
    if cached_graph_elements:
        return cached_graph_elements
    # Sinon on garde le graphe initial
    return initial_elements

def run_dashboard(queue: Queue[dict[str, Any]] | None = None):
    global graph_queue, cached_graph_elements
    graph_queue = queue
    cached_graph_elements = initial_elements
    # Si une file d'attente est fournie, on lance le thread de surveillance en arrière-plan
    if graph_queue is not None:
        # daemon=True permet au thread de se couper automatiquement si l'application principale s'arrête
        Thread(target=watch_graph_queue, daemon=True).start()
    # Lance le serveur local (debug=False est obligatoire pour éviter les conflits de threads)
    app.run(debug=False)
