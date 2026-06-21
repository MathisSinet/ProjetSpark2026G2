import json
import time
import random
from datetime import datetime, timezone
import os

# Dossier cible où seront déversés les événements au format JSON
OUTPUT_DIR = "streaming_data"
# Création sécurisée du dossier s'il n'existe pas encore
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Données de simulation pour reproduire des profils réalistes sur LeBonCoin
CITIES = ["Paris", "Lyon", "Marseille", "Toulouse", "Lille", "Bordeaux", "Nantes", "Strasbourg"]
CATEGORIES = ["Véhicules", "Immobilier", "Mode", "Maison", "Multimédia", "Loisirs", "Bricolage"]
ACTIONS = ["AIME", "VOUT", "ACHAT"]

def generate_event():

    
    """Génère un événement aléatoire respectant la structure demandée."""

    #Distribution statistique des actions pour simuler un comportement humain :
    # 60% de clics 'AIME', 30% d'intentions de vue 'VOUT', 10% d'actes d' 'ACHAT' 
    action = random.choices(ACTIONS, weights=[0.6, 0.3, 0.1])[0]
    
    #Construction du dictionnaire représentant l'interaction e-commerce
    event = {
        #Horodatage précis au format standardisé ISO 8601 UTC (ex: 2026-06-17T15:06:20Z)
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        #Identifiants uniques générés aléatoirement pour simuler la base utilisateur/produit
        "user_id": f"usr_{random.randint(1000, 9999)}",
        "user_city": random.choice(CITIES),
        "product_id": f"prod_{random.randint(1000, 9999)}",
        "product_cat": random.choice(CATEGORIES),
        "seller_id": f"sel_{random.randint(100, 999)}",
        "action_type": action,
        #Prix réaliste compris entre 5€ et 2500€, arrondi proprement à 2 décimales
        "price": round(random.uniform(5.0, 2500.0), 2)
    }
    
    return event

def run_simulator():
    """Lance la boucle infinie générant le flux de données."""
    print(f"Démarrage du simulateur 'LeBonCoin'...")
    print(f"Les fichiers JSON seront générés en continu dans le dossier : ./{OUTPUT_DIR}/\n")
    print("Appuyez sur CTRL+C pour arrêter le flux.")
    
    #Compteur interne pour le suivi de la production dans le terminal
    event_count = 0
    
    try:
        while True:
            #1.Génération d'un nouvel événement 
            event = generate_event()

            #2.Définition d'un nom de fichier unique basé sur le timestamp en millisecondes
            #Cela garantit l'ordonnancement et évite les écrasements de fichiers
            filename = os.path.join(OUTPUT_DIR, f"event_{int(time.time() * 1000)}.json")
            
            #3.Ecriture du fichier sur le disque au format JSON avec un encodage UTF-8
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(event, f, ensure_ascii=False)
            
            event_count += 1
            
            #4. Logs de contrôle : affiche un résumé dans la console tous les 10 événements
            if event_count % 10 == 0:
                print(f"[{event['timestamp']}] {event_count} événements générés - Dernier : {event['user_id']} a fait {event['action_type']} sur {event['product_id']}")
            #5. Temporisation de variable entre 0.3 et 1.5 seconde pour simuler un trafic asynchrone irrégulier
            time.sleep(random.uniform(0.3, 1.5))
            
    except KeyboardInterrupt:
        #Interception du signal de fermeture (CTRL+C) envoyé par main.py ou l'utilisateur
        print(f"\nArrêt manuel du simulateur. Total généré : {event_count} événements.")
