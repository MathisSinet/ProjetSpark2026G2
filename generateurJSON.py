import json
import time
import random
from datetime import datetime, timezone
import os

OUTPUT_DIR = "streaming_data"
os.makedirs(OUTPUT_DIR, exist_ok=True)

CITIES = ["Paris", "Lyon", "Marseille", "Toulouse", "Lille", "Bordeaux", "Nantes", "Strasbourg"]
CATEGORIES = ["Véhicules", "Immobilier", "Mode", "Maison", "Multimédia", "Loisirs", "Bricolage"]
ACTIONS = ["AIME", "VOUT", "ACHAT"]

def generate_event():
    """Génère un événement aléatoire respectant la structure demandée."""
    action = random.choices(ACTIONS, weights=[0.6, 0.3, 0.1])[0]
    
    event = {
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "user_id": f"usr_{random.randint(1000, 9999)}",
        "user_city": random.choice(CITIES),
        "product_id": f"prod_{random.randint(1000, 9999)}",
        "product_cat": random.choice(CATEGORIES),
        "seller_id": f"sel_{random.randint(100, 999)}",
        "action_type": action,
        "price": round(random.uniform(5.0, 2500.0), 2)
    }
    
    return event

def run_simulator():
    """Lance la boucle infinie générant le flux de données."""
    print(f"Démarrage du simulateur 'LeBonCoin'...")
    print(f"Les fichiers JSON seront générés en continu dans le dossier : ./{OUTPUT_DIR}/\n")
    print("Appuyez sur CTRL+C pour arrêter le flux.")
    
    event_count = 0
    
    try:
        while True:
            event = generate_event()
            filename = os.path.join(OUTPUT_DIR, f"event_{int(time.time() * 1000)}.json")
            
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(event, f, ensure_ascii=False)
            
            event_count += 1
            
            if event_count % 10 == 0:
                print(f"[{event['timestamp']}] {event_count} événements générés - Dernier : {event['user_id']} a fait {event['action_type']} sur {event['product_id']}")
            time.sleep(random.uniform(0.3, 1.5))
            
    except KeyboardInterrupt:
        print(f"\nArrêt manuel du simulateur. Total généré : {event_count} événements.")
