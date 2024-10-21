import sys

if sys.version_info >= (3, 12, 0):
    import six
    sys.modules['kafka.vendor.six.moves'] = six.moves

import threading
import time
import os
import pickle
import json
from kafka import KafkaProducer, KafkaConsumer
from Clases import *

stop_threads = False

taxi_updates = ""

def receiveMap():
    global taxi_updates
     #Creamos el consumer de Kafka
    consumer = KafkaConsumer('map', bootstrap_servers = f'{sys.argv[1]}:{sys.argv[2]}')

    #Recibimos el mapa
    for message in consumer:
        mapa = pickle.loads(message.value)
        #os.system('cls')
        cadena = mapa.cadenaMapaCustomer(str(sys.argv[3])) + taxi_updates
        print(cadena)

def receiveTaxiUpdates():
    global taxi_updates
    # Creamos el consumer de Kafka para los topics taxi_assigned, picked_up, y arrived
    consumer = KafkaConsumer('taxi_assigned', 'picked_up', 'arrived', 'client_service', bootstrap_servers=f'{sys.argv[1]}:{sys.argv[2]}')
    # Recibimos las actualizaciones del taxi
    for message in consumer:
        topic = message.topic
        if(topic == 'client_service'):
            cliente, destino = message.value.decode('utf-8').split()
        else:
            taxi, cliente, destino = message.value.decode('utf-8').split()
        
        if cliente == sys.argv[3]:
            if topic == 'taxi_assigned':
                taxi_updates = f"\n{Back.WHITE}{Fore.BLACK}Taxi {taxi} asignado al cliente {cliente} para ir a {destino}{Style.RESET_ALL}"
            elif topic == 'picked_up':
                taxi_updates = f"\n{Back.WHITE}{Fore.BLACK}Pasajero recogido. Taxi {taxi} yendo a {destino}{Style.RESET_ALL}"
            elif topic == 'arrived':
                taxi_updates = f"\n{Back.WHITE}{Fore.BLACK}Has llegado a su destino{Style.RESET_ALL}"
            elif topic == 'client_service':
                taxi_updates = f"\n{Back.WHITE}{Fore.BLACK}Solicitud de servicio a destino {destino}{Style.RESET_ALL}"

def receiveService(id):
    #Creamos el consumer de Kafka
    consumer = KafkaConsumer('service_assigned_client', bootstrap_servers = f'{sys.argv[1]}:{sys.argv[2]}')

    #Recibimos el servicio
    for message in consumer:
        print(message.value.decode('utf-8'))
        #Sólo podemos imprimir los mensajes que llegan para nosotros
        data = message.value.decode('utf-8').split(" ")
        if data[0] == id:
            if len(data) > 2:
                #El servicio se ha asignado correctamente
                print(f"El servicio ha sido asignado correctamente y el taxi {data[3]} se dirige a tu posición.")
            else:
                #El servicio no se ha podido asignar
                print("No se ha podido asignar el servicio. Inténtalo más tarde.")


def services(id):
    producer = KafkaProducer(bootstrap_servers = f'{sys.argv[1]}:{sys.argv[2]}')
    consumer = KafkaConsumer('service_completed', bootstrap_servers = f'{sys.argv[1]}:{sys.argv[2]}')

    completed = False #Nos marca si el servicio ha sido completado
    #Leemos el archivo servicios.txt y lo recorremos para pedir servicios con kafka
    fileName = f"Requests/EC_Requests{id}.json"
    with open("EC_Requests.json", "r") as file:
        data = json.load(file)
        for request in data['Requests']:
            request_id = request['Id']
            producer.send('service_requests', value=f"{id} {request_id}".encode('utf-8'))

            while not completed:
                for message in consumer:
                    data = message.value.decode('utf-8').split()
                    # Comprobamos que el mensaje es para nosotros
                    if data[0] == id:
                            # El viaje se ha completado y puedo procesar el siguiente
                            completed = True
                            time.sleep(4)
                            break
                if completed:
                    completed = False  # Reset completed for the next request
                    break
                        



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

    #Creamos el hilo que recibe los servicios
    service_thread = threading.Thread(target=receiveService, args=(id, ))
    service_thread.start()

    #Creamos el hilo que envía los servicios
    services_thread = threading.Thread(target=services, args=(id, ))
    services_thread.start()

    taxi_thread = threading.Thread(target=receiveTaxiUpdates)
    taxi_thread.start()

    map_thread.join()
    service_thread.join()
    services_thread.join()
    taxi_thread.join()

if __name__ == "__main__":
    main()