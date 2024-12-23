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

finished = False
centralTimeout = 0
taxi_updates = ""

centralState = False
posicion = None

def receiveMap():
    global taxi_updates, centralTimeout, posicion,centralState

     #Creamos el consumer de Kafka
    consumer = KafkaConsumer('map', bootstrap_servers = f'{sys.argv[1]}:{sys.argv[2]}')

    while True:
        if not centralState:
            centralState = True
        message = consumer.poll(timeout_ms=1000)
        if message:
            centralTimeout = 0
            for tp, messages in message.items():
                for message in messages:
                    mapa = pickle.loads(message.value)
                    posicion = mapa.getPosCliente(str(sys.argv[3]))
                    #os.system('cls')
                    print(f"Cliente {sys.argv[3]}")
                    cadena = mapa.cadenaMapaCustomer(str(sys.argv[3])) + taxi_updates
                    print(cadena)
        else:
            if centralTimeout > 5:
                centralState = False
                os.system('cls')
                imprimirErrorCentral()

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
    global taxi_updates
    #Recibimos el servicio
    for message in consumer:
        print(message.value.decode('utf-8'))
        #Sólo podemos imprimir los mensajes que llegan para nosotros
        data = message.value.decode('utf-8').split(" ")
        if data[0] == id:
            if len(data) > 2:
                #El servicio se ha asignado correctamente
                taxi_updates = f"El servicio ha sido asignado correctamente y el taxi {data[3]} se dirige a tu posición."
            else:
                #El servicio no se ha podido asignar
                taxi_updates = "No se ha podido asignar el servicio. Inténtalo más tarde." 


def services(id):
    global taxi_updates, finished
    producer = KafkaProducer(bootstrap_servers = f'{sys.argv[1]}:{sys.argv[2]}')
    consumer = KafkaConsumer('service_completed', bootstrap_servers = f'{sys.argv[1]}:{sys.argv[2]}')

    #Converitmos de id a numero: a -> 1
    number = ord(id) - 96

    fileName = f"Requests/EC_Requests_{id}.json"
    with open(fileName, "r") as file:
        data = json.load(file)
        for request in data['Requests']:
            time.sleep(4)
            completed = False
            request_id = request['Id']
            taxi_updates = f"\n{Back.WHITE}{Fore.BLACK}Solicitud de servicio a destino {request_id}{Style.RESET_ALL}"
            producer.send('service_requests', value=f"{id} {request_id}".encode('utf-8'))
            start_time = time.time()
            while not completed and time.time() - start_time < 15:
                message = consumer.poll(timeout_ms=500)  # Espera hasta 1 segundo por un mensaje
                if message:
                    for tp, messages in message.items():
                        for msg in messages:
                            data = msg.value.decode('utf-8').split()
                            # Comprobamos que el mensaje es para nosotros
                            if data[0] == id:
                                # El viaje se ha completado y puedo procesar el siguiente
                                completed = True
                                time.sleep(4)
                                break
                if completed:
                    break
    taxi_updates = f"\n{Back.WHITE}{Fore.BLACK}Todos los servicios han sido completados{Style.RESET_ALL}"
    finished = True
    time.sleep(5)
    #paramos todos lo hilos y cerramos la terminal
    os._exit(0)

#En esta función le sumamos 1 a centralTimeout cada segundo                
def centralState():
    global centralTimeout
    while True:
        centralTimeout += 1
        time.sleep(1)
        
def sendState(id):
    global posicion
    producer = KafkaProducer(bootstrap_servers = f'{sys.argv[1]}:{sys.argv[2]}')
    while not finished:
        if posicion is not None:
            cx = posicion.getX()
            cy = posicion.getY()
        else:
            cx = 0
            cy = 0
        producer.send('customerOK', value=f"{id} {cx} {cy}".encode("utf-8"))
        #print(f"Cliente {id} ha enviado su posición: {pos}")
        time.sleep(0.5)
                        

def main():
    global posicion
    
    if len(sys.argv) != 4:
        print("Uso: python EC_Customer.py <Bootstrap_IP> <Bootstrap_Port> <ID>")
        sys.exit(1)

    id = str(sys.argv[3])
    print(f"Cliente {id} conectado")

    #Comunicamos por Kafka la existencia del cliente y le mandamos la posición inicial
    cx, cy = input("Introduce tu posición (x y): "). split()
    posicion = Casilla(int(cx), int(cy))
    
    producer = KafkaProducer(bootstrap_servers = f'{sys.argv[1]}:{sys.argv[2]}')
    producer.send('clients', value = f"{id} {cx} {cy}".encode('utf-8'))
    
    #Recogemos la respuesta de la central por saber si nos podemos conectar
    consumer = KafkaConsumer(
        'client_accepted',
        bootstrap_servers=f'{sys.argv[1]}:{sys.argv[2]}',
        auto_offset_reset='latest',  # Asegúrate de recibir solo mensajes nuevos
        consumer_timeout_ms=10000  # Tiempo de espera para recibir mensajes
    )
    conexion = False
    print("Esperando respuesta de la central...")
    #Esperamos a que la central nos acepte
    for message in consumer:
        mes = message.value.decode('utf-8')
        if mes.split()[0] != id:
            continue
        if mes.split()[1] == "KO":
            print("No se ha podido conectar con la central")
            break
        elif mes.split()[1] == "OK":
            conexion = True
            print("Conexión con la central establecida")            
            break


    """
    print("Esperando respuesta de la central...")
    
    for message in consumer:
        mes = message.value.decode('utf-8')
        print(mes)
        if mes.split()[1] == "KO":
            print("No se ha podido conectar con la central")
            break
        elif mes.split()[1] == "OK":
            conexion = True
            print("Conexión con la central establecida")            
            break
    """        
    #conexion = True

    if conexion:
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
        
        centralState_thread = threading.Thread(target=centralState)
        centralState_thread.start()
        
        sendOK_thread = threading.Thread(target=sendState, args=id,)
        sendOK_thread.start()

        map_thread.join()
        service_thread.join()
        services_thread.join()
        taxi_thread.join()
        centralState_thread.join()
        sendOK_thread.join()
        
    else:
        print("Error al conectar por la central debido a que el identificador ya esta en uso")

if __name__ == "__main__":
    main()