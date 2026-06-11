from multiprocessing import Process
from generateurJSON import run_simulator
from dashboard import run_dashboard
import time

if __name__ == "__main__":
    print("🚀 Démarrage global du projet LeBonCoin...")
    print("-------------------------------------------")
    
    # 1. On prépare nos deux travailleurs (Process)
    process_generateur = Process(target=run_simulator)
    process_dashboard = Process(target=run_dashboard)
    
    # 2. On lance le générateur
    process_generateur.start()
    
    # Petite pause d'une seconde pour laisser le générateur créer les premiers JSON
    time.sleep(1)
    
    # 3. On lance le dashboard
    process_dashboard.start()
    
    print("\n✅ TOUT EST LANCÉ !")
    print("🌐 Dashboard accessible sur : http://127.0.0.1:8050/")
    print("Appuyez sur CTRL+C pour tout arrêter.\n")
    
    try:
        # On demande au programme principal d'attendre que les deux processus tournent
        process_generateur.join()
        process_dashboard.join()
    except KeyboardInterrupt:
        # Si tu fais CTRL+C, on coupe tout proprement
        print("\n🛑 Arrêt d'urgence demandé...")
        process_generateur.terminate()
        process_dashboard.terminate()
        print("Fin du programme.")