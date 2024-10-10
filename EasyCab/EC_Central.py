import sys

if sys.version_info >= (3, 12, 0):
    import six
    sys.modules['kafka.vendor.six.moves'] = six.moves

import socket
import threading
import time
import subprocess
from kafka import KafkaProducer, KafkaConsumer
from Clases import *

#Aquí guardo las localizaciones leídas por fichero: {ID:CASILLA}
LOCALIZACIONES = {}
# Diccionario para almacenar los taxis que tengo disponibles
TAXIS_DISPONIBLES = []
#Lista de taxis para guardar los que ya están autenticados con elementos Taxi
TAXIS = []
#Lista de clientes
CLIENTES = []

#Creamos las funciones que nos sirven para leer los archivos de configuración
def leerLocalizaciones(localizaciones):
    with open("map_config.txt", "r") as file:
        for linea in file:
            linea = linea.strip()
            if linea:
                partes = linea.split()
                id = partes[0]
                x = int(partes[1])
                y = int(partes[2])
                localizaciones[id] = Casilla(x, y)

def leerTaxis(taxis):
    with open("taxis.txt", "r") as file:
        for linea in file:
            id = int(linea.strip())
            taxis.append(id)

'''
#Definimos las funciones para crear el consumidor y el productor
def createProducer(bootstrapServer):
    conf = {'bootstrap.serves':bootstrapServer}
    return Producer(conf)

def createConsumer(topic, group, bootstrapServer):
    conf = {'bootstrap.serves': bootstrapServer, 'group.id': group, 'auto.offset.reset': 'earliest'}
    consumer = Consumer(conf)
    consumer.subscribe([topic])
    return consumer
'''

#Creamos la función que gestiona la autenticación por sockets
def autheticate_taxi():
    #Creamos el socket del servidor con la direccion por parametros
    server_socket = socket.socket()
    server_socket.bind(('localhost', int(sys.argv[1])))
    server_socket.listen(5)

    while True:
        #Permitimos la conexión y recibimos los datos
        client, addr = server_socket.accept()

        data = client.recv(1024).decode('utf-8')
        if not data:
            client.close()
        #Ahora comprobamos que en la lista de taxis se encuentra el taxi que se ha conectado
        try:
            id = int(data)
            if id in TAXIS_DISPONIBLES:
                #Creamos el objeto taxi
                taxi = Taxi(id)
                #Borramos el taxi de la lista de taxis disponibles
                TAXIS_DISPONIBLES.remove(id)
                #Añadimos el taxi a la lista de taxis
                TAXIS.append(taxi)
                #Respondemos con un OK
                client.send("OK".encode('utf-8'))
                #Cerramos la conexión
                client.close()
            else:
                #Mandamos un mensaje al taxi de error
                client.send("ERROR".encode('utf-8'))
                client.close()
        except ValueError:
            client.send("ERROR".encode('utf-8'))
            client.close()


#Función para enviar el mapa
def sendMap():
    #Creamos el productor de Kafka
    producer = KafkaProducer(bootstrap_servers=f'{sys.argv[2]}:{sys.argv[3]}')
    #Añadimos el mapa

    mapa = Mapa()
    while True:
        cadena = mapa.cadenaMapa(LOCALIZACIONES, TAXIS, CLIENTES)
        producer.send('map', cadena.encode('utf-8'))
        #Aquí esperamos un segundo y lo volvemos a mandar
        #print(cadena)
        time.sleep(1)

"""
# Función para manejar solicitudes de clientes y enviar instrucciones a los taxis
def handle_kafka_messages():
    print("Esperando solicitudes de clientes...")
    while True:
        msg = consumer.poll(1.0)  # Esperar 1 segundo por nuevos mensajes
        if msg is None:
            continue
        if msg.error():
            print(f"Error en Consumer: {msg.error()}")
            continue

        # Procesar solicitud de cliente
        solicitud = msg.value().decode('utf-8')
        print(f"Solicitud recibida: {solicitud}")

        # Buscar un taxi disponible
        taxi_asignado = None
        for taxi_id, estado in taxis_disponibles.items():
            if estado == 'ROJO':  # Taxi libre
                taxi_asignado = taxi_id
                taxis_disponibles[taxi_id] = 'VERDE'  # Marcar como ocupado
                break

        # Enviar respuesta al cliente y asignar taxi
        if taxi_asignado:
            print(f"Asignando {taxi_asignado} a la solicitud {solicitud}")
            producer.produce('taxi_instructions', key=taxi_asignado, value=f'IR A {solicitud}')
            producer.flush()
        else:
            print("No hay taxis disponibles en este momento")
"""

def start_zookeeper():
    try:
        result = subprocess.run(['zookeeper-server-start.bat', 'C:/kafka/config/zookeeper.properties'], check=True)
        print("ZooKeeper iniciado con éxito")
    except subprocess.CalledProcessError as e:
        print(f"Error al iniciar ZooKeeper: {e}")
    
def start_kafka():
    try:
        result = subprocess.run(['kafka-server-start.bat', 'C:/kafka/config/server.properties'], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error al iniciar Kafka: {e}")

def main():
    # Comprobar que se han pasado los argumentos correctos
    if len(sys.argv) != 4:
        print("Usage: python EC_Central.py Port Bootstrap_IP Bootstrap_Port")
        sys.exit(1)
    
    '''
    res = subprocess.run(['kafka-server-stop.bat'],check=True)
    res1 = subprocess.run(['zookeeper-server-stop.bat'],check=True)
    
    zk_thread = threading.Thread(target=start_zookeeper)
    zk_thread.start()
    time.sleep(5)

    kfk_thread = threading.Thread(target=start_kafka)
    kfk_thread.start()

    time.sleep(5)
    '''
    
    # Leer las localizaciones y taxis disponibles
    leerLocalizaciones(LOCALIZACIONES)
    leerTaxis(TAXIS_DISPONIBLES)

    # Crear el mapa
    mapa = Mapa()
    

    # Iniciar el servidor de autenticación en un hilo
    auth_thread = threading.Thread(target=autheticate_taxi)
    auth_thread.start()

    map_thread = threading.Thread(target=sendMap)
    map_thread.start()
    
    auth_thread.join()
    map_thread.join()

    #print(mapa.cadenaMapa(LOCALIZACIONES, TAXIS, CLIENTES))

    '''
    # Crear el productor y consumidor de Kafka
    bootstrap_server = f"{sys.argv[2]}:{sys.argv[3]}"
    producer = createProducer(bootstrap_server)
    consumer = createConsumer("service_requests", "central_group", bootstrap_server)


    # Iniciar el hilo para enviar el mapa
    mapa_thread = threading.Thread(target=sendMap)
    mapa_thread.start()

    

    # Manejar Kafka en el hilo principal
    handle_kafka_messages()
    '''


# Iniciar el servidor de autenticación y el manejo de Kafka en paralelo
if __name__ == "__main__":
    main()
