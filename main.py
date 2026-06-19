from multiprocessing import Process, Queue
from generateurJSON import run_simulator
from spark_core import spark_core
from time import sleep
from dashboard import run_dashboard

def main():
    print("Démarrage global du projet LeBonCoin...")
    print("-------------------------------------------")

    graph_queue = Queue(maxsize=1)

    process_generateur = Process(target=run_simulator)
    process_spark = Process(target=spark_core, args=(None, graph_queue))
    process_dashboard = Process(target=run_dashboard, args=(graph_queue,))

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
