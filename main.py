from generateurJSON import run_simulator
from spark_core import spark_core
from threading import Thread
from multiprocessing import Process
from time import sleep


def main():
    generator = Process(target = run_simulator)
    spark = Thread(target = spark_core, args=(30,))

    generator.start()
    spark.start()
    
    spark.join()
    generator.terminate()

    print("END")


if __name__ == "__main__":
    main()
