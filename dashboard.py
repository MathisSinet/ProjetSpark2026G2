import dash
from dash import html, dcc, Input, Output
import dash_cytoscape as cyto
import random

from style_graphe import mon_style_cytoscape

import json
import os


app = dash.Dash(__name__)

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
        interval=5000, 
        n_intervals=0
    )
], style={'backgroundColor': '#ECF0F1', 'minHeight': '100vh', 'padding': '20px', 'margin': '-8px'})

'''
@app.callback(
    Output('live-graph-leboncoin', 'elements'),
    Input('interval-clock', 'n_intervals'),
    Input('live-graph-leboncoin', 'elements')
)
def refresh_graph_automatically(n, existing_elements):
    if n == 0:
        return existing_elements

    rand_id = random.randint(100, 999)
    u_id = f"usr_{rand_id}"
    s_id = f"sel_{rand_id}"
    p_id = f"prod_{rand_id}"
    
    action_choisie = random.choice(["AIME", "VOUT", "ACHAT"])
    
    new_nodes = [
        {'data': {'id': u_id, 'label': u_id}, 'classes': 'user'},
        {'data': {'id': s_id, 'label': s_id}, 'classes': 'seller'},
        {'data': {'id': p_id, 'label': p_id}, 'classes': 'product'}
    ]
    
    new_edges = [
        {'data': {'source': u_id, 'target': p_id, 'action': action_choisie}},
        {'data': {'source': s_id, 'target': p_id, 'action': 'PROPOSE'}}
    ]
    
    existing_elements.extend(new_nodes)
    existing_elements.extend(new_edges)
    
    return existing_elements
'''

@app.callback(
        Output('live-graph-leboncoin', 'elements'),
        Input('interval-clock', 'n_intervals')
)
def refresh_graph_automatically(n):

    if os.path.exists("graph_data.json"):
        try:
            with open("graph_data.json", 'r', encoding='utf-8') as f:
                vrais_elements = json.load(f)
                if vrais_elements :
                    return vrais_elements
        except Exception:
            pass
    
    return initial_elements

def run_dashboard():
    app.run(debug=False)