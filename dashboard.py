import dash
from dash import html, dcc, Input, Output
import dash_cytoscape as cyto
from queue import Empty, Queue
from threading import Thread
from typing import Any

from style_graphe import mon_style_cytoscape


app = dash.Dash(__name__)
graph_queue: Queue[dict[str, Any]] | None = None
cached_graph_elements: list[dict[str, Any]] | None = None


def watch_graph_queue():
    global cached_graph_elements

    while True:
        if graph_queue is None:
            return

        try:
            latest_payload = graph_queue.get(timeout=1)
        except Empty:
            continue

        if latest_payload and latest_payload.get("elements"):
            cached_graph_elements = latest_payload["elements"]

initial_elements = [
    {'data': {'id': 'usr_init', 'label': 'User_Init'}, 'classes': 'user'},
    {'data': {'id': 'sel_init', 'label': 'Boutique_Init'}, 'classes': 'seller'},
    {'data': {'id': 'prod_init', 'label': 'Article_Init'}, 'classes': 'product'},
    {'data': {'source': 'usr_init', 'target': 'prod_init', 'action': 'AIME'}}
]

app.layout = html.Div([
    html.Header([
        html.H1("📊 Dashboard Streaming LeBonCoin", 
                style={'textAlign': 'center', 'fontFamily': 'Helvetica, Arial, sans-serif', 'color': '#2C3E50', 'paddingTop': '15px'}),
        html.H4("Visualisation en temps réel des interactions", 
                style={'textAlign': 'center', 'color': '#7F8C8D', 'fontFamily': 'Helvetica, Arial, sans-serif', 'marginTop': '-10px', 'paddingBottom': '15px'})
    ], style={'backgroundColor': '#FFFFFF', 'boxShadow': '0px 4px 10px rgba(0,0,0,0.05)', 'marginBottom': '20px', 'borderRadius': '10px'}),
    
    cyto.Cytoscape(
        id='live-graph-leboncoin',
        elements=initial_elements,
        stylesheet=mon_style_cytoscape, 
        style={
            'width': '100%', 
            'height': '650px', 
            'backgroundColor': '#FDFEFE', 
            'borderRadius': '15px', 
            'boxShadow': '0px 0px 15px rgba(0,0,0,0.05)',
            'border': '1px solid #E5E8E8'
        },
        layout={'name': 'cose'}
    ),
    
    dcc.Interval(
        id='interval-clock',
        interval=10_000, 
        n_intervals=0
    )
], style={'backgroundColor': '#ECF0F1', 'minHeight': '100vh', 'padding': '20px', 'margin': '-8px'})


@app.callback(
        Output('live-graph-leboncoin', 'elements'),
        Input('interval-clock', 'n_intervals')
)
def refresh_graph_automatically(n):
    global cached_graph_elements

    if cached_graph_elements:
        return cached_graph_elements

    return initial_elements

def run_dashboard(queue: Queue[dict[str, Any]] | None = None):
    global graph_queue, cached_graph_elements
    graph_queue = queue
    cached_graph_elements = initial_elements
    if graph_queue is not None:
        Thread(target=watch_graph_queue, daemon=True).start()
    app.run(debug=False)