import sys

if sys.version_info >= (3, 12, 0):
    import six
    sys.modules['kafka.vendor.six.moves'] = six.moves

import socket
import threading
import time
import subprocess
import pickle
from kafka import KafkaProducer, KafkaConsumer
from Clases import *

def receiveMap():
     #Creamos el consumer de Kafka
    consumer = KafkaConsumer('map', bootstrap_servers = f'{sys.argv[1]}:{sys.argv[2]}')

    #Recibimos el mapa
    for message in consumer:
        mapa = pickle.loads(message.value)
        print(mapa.cadenaMapaCustomer(str(sys.argv[3])))

def main():
    if len(sys.argv) != 4:
        print("Uso: python EC_Customer.py <Bootstrap_IP> <Bootstrap_Port> <ID>")
        sys.exit(1)

    id = sys.argv[3]
    print(f"Cliente {id} conectado")

    #Comunicamos por Kafka la existencia del cliente
    producer = KafkaProducer(bootstrap_servers = f'{sys.argv[1]}:{sys.argv[2]}')
    producer.send('clients', value = f"{id}".encode('utf-8'))


    #Creamos el hilo que recibe el mapa
    map_thread = threading.Thread(target=receiveMap)
    map_thread.start()

    map_thread.join()

if __name__ == "__main__":
    main()
