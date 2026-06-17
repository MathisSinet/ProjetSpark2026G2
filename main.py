from multiprocessing import Process
from generateurJSON import run_simulator
from spark_core import spark_core
from threading import Thread
from multiprocessing import Process
from time import sleep
from dashboard import run_dashboard
from graph_master import start_spark_graph



'''
def main():
    generator = Process(target = run_simulator)
    spark = Thread(target = spark_core, args=(30,))

    generator.start()
    spark.start()
    
    spark.join()
    generator.terminate()

    print("END")

def main2():
    print("Démarrage global du projet LeBonCoin...")
    print("-------------------------------------------")
    
    # 1. On prépare nos deux travailleurs (Process)
    process_generateur = Process(target=run_simulator)
    process_dashboard = Process(target=run_dashboard)
    
    # 2. On lance le générateur
    process_generateur.start()
    
    # Petite pause d'une seconde pour laisser le générateur créer les premiers JSON
    sleep(1)
    
    # 3. On lance le dashboard
    process_dashboard.start()
    
    print("\nTOUT EST LANCÉ !")
    print("Dashboard accessible sur : http://127.0.0.1:8050/")
    print("Appuyez sur CTRL+C pour tout arrêter.\n")
    
    try:
        # On demande au programme principal d'attendre que les deux processus tournent
        process_generateur.join()
        process_dashboard.join()
    except KeyboardInterrupt:
        # CTRL+C, on coupe tout proprement
        print("\nArrêt d'urgence demandé...")
        process_generateur.terminate()
        process_dashboard.terminate()
        print("Fin du programme.")


if __name__ == "__main__":
    main2()

'''

def main():
    print("Démarrage global du projet LeBonCoin...")
    print("-------------------------------------------")

    process_generateur = Process(target=run_simulator)
    process_spark = Process(target=start_spark_graph)
    process_dashboard = Process(target=run_dashboard)

    process_generateur.start()
    sleep(2)

    process_spark.start()
    sleep(2)

    process_dashboard.start()

    print("\nTOUT EST LANCÉ !")
    print("Dashboard accessible sur : http://127.0.0.1:8050/")
    print("Appuyez sur CTRL+C pour tout arrêter.\n")


    try:
        process_generateur.join()
        process_spark.join()
        process_dashboard.join()
    except KeyboardInterrupt:
        print("\nArrêt d'urgence demandé...")
        process_generateur.terminate()
        process_spark.terminate()
        process_dashboard.terminate()
        print("Fin du programme.")

if __name__ == "__main__":
    main()
