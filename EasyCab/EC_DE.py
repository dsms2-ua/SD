import sys

if sys.version_info >= (3, 12, 0):
    import six
    sys.modules['kafka.vendor.six.moves'] = six.moves

import threading
import time
from kafka import KafkaProducer, KafkaConsumer
import pickle
import socket
from Clases import *

#Variable global para guardar el id del taxi
ID = 0


def authenticateTaxi():
    #Recogemos los datos de los argumentos
    central_ip = f'{sys.argv[1]}'
    central_port = int(sys.argv[2])

    #Nos conectamos a la central por sockets
    client_socket = socket.socket()
    client_socket.connect(('localhost', central_port))

    #Enviamos el id del taxi para comprobar
    taxi_id = int(sys.argv[5])
    client_socket.send(f"{taxi_id}".encode('utf-8'))

    #Recibimos la respuesta de la central
    respuesta = client_socket.recv(1024).decode('utf-8')
    if respuesta == "OK":
        print("Taxi autenticado correctamente")
        client_socket.close()
        return True
    else:
        print("Error en la autenticación del taxi")
        client_socket.close()
        return False
    
def receiveMap():
    #Creamos el consumer de Kafka
    consumer = KafkaConsumer('map', bootstrap_servers = f'{sys.argv[3]}:{sys.argv[4]}')

    #Recibimos el mapa
    for message in consumer:
        mapa = pickle.loads(message.value)
        print(mapa.cadenaMapa())

def receiveServices():
    #Creamos el consumer de Kafka
    consumer = KafkaConsumer('service_assigned', bootstrap_servers = f'{sys.argv[3]}:{sys.argv[4]}')
    #Recibimos los servicios
    for message in consumer:
        servicio = pickle.loads(message.value)
        print(f"Taxi {servicio.getTaxi()} asignado al cliente {servicio.getCliente()}")
        

    
    

def main():
    global ID
    #Comprobamos que los argumetos sean correctos
    if len(sys.argv) != 6:
        print("Error: Usage python EC_DE.py Central_IP Central_Port Bootstrap_IP Bootstrap_Port Taxi_ID")

    #Autenticamos con sockets la existencia del taxi
    if not authenticateTaxi():
        return

    ID = sys.argv[5]
    
    #Creamos el hilo que lleva al consumidor Kafka del mapa
    map_thread = threading.Thread(target=receiveMap)
    map_thread.start()


    #Creamos el hilo que lleva al consumidor Kafka de los servicios asignados
    services_thread = threading.Thread(target=receiveServices)
    services_thread.start()

    


    map_thread.join()
    alert_thread.join()


# Ejecución principal
if __name__ == "__main__":
    main()
