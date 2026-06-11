mon_style_cytoscape = [
    {
        'selector': 'node', 
        'style': {
            'label': 'data(label)', 
            'color': '#2C3E50', 
            'text-valign': 'bottom', 
            'text-margin-y': '8px',
            'font-size': '12px',
            'font-family': 'Helvetica, Arial, sans-serif',
            'font-weight': 'bold',
            'border-width': '2px',
            'border-color': '#FFFFFF'
        }
    },
    {
        'selector': '.user', 
        'style': {
            'background-color': '#3498DB', 
            'shape': 'ellipse', 
            'width': '45px', 
            'height': '45px',
            'border-color': '#2980B9'
        }
    },
    {
        'selector': '.seller', 
        'style': {
            'background-color': '#E67E22', 
            'shape': 'round-rectangle', 
            'width': '50px', 
            'height': '50px',
            'border-color': '#D35400'
        }
    },
    {
        'selector': '.product', 
        'style': {
            'background-color': '#1ABC9C', 
            'shape': 'tag', 
            'width': '55px', 
            'height': '40px',
            'border-color': '#16A085'
        }
    },
    {
        'selector': 'edge', 
        'style': {
            'curve-style': 'bezier',
            'target-arrow-shape': 'triangle',
            'font-size': '11px',
            'font-family': 'Helvetica, Arial, sans-serif',
            'font-weight': 'bold',
            'text-rotation': 'autorotate',
            'text-background-color': '#FFFFFF',
            'text-background-opacity': 0.8,
            'text-background-padding': '3px'
        }
    },
    {
        'selector': 'edge[action = "AIME"]',
        'style': {
            'line-color': '#FF6B81',
            'target-arrow-color': '#FF6B81',
            'label': 'AIME',
            'color': '#FF6B81'
        }
    },
    {
        'selector': 'edge[action = "ACHAT"]',
        'style': {
            'line-color': '#2ED573',
            'target-arrow-color': '#2ED573',
            'label': 'ACHAT',
            'color': '#2ED573',
            'width': 3
        }
    },
    {
        'selector': 'edge[action = "VOUT"]',
        'style': {
            'line-color': '#FFA502',
            'target-arrow-color': '#FFA502',
            'label': 'VEUT',
            'color': '#FFA502'
        }
    },
    {
        'selector': 'edge[action = "PROPOSE"]',
        'style': {
            'line-color': '#A4B0BE',
            'target-arrow-color': '#A4B0BE',
            'line-style': 'dashed',
            'label': 'PROPOSE',
            'color': '#747D8C'
        }
    }
]