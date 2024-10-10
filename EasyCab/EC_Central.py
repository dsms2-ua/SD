import sys

if sys.version_info >= (3, 12, 0):
    import six
    sys.modules['kafka.vendor.six.moves'] = six.moves

import socket
import threading
import time
import pickle
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

                #cComprobamos que la posicion del taxi no esta ocupada por otro taxi
                for t in TAXIS:
                    if t.getX() == 1 and t.getY() == 1:
                        t.setCasilla(Casilla(2,1))
                        break

                #Añadimos el taxi a la lista de taxis
                TAXIS.append(taxi)
                #Respondemos con un OK
                client.send("OK".encode('utf-8'))
                print(f"Taxi {id} autenticado correctamente")
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

    mapa = Mapa(LOCALIZACIONES, TAXIS, CLIENTES)
    while True:
        serialized = pickle.dumps(mapa)
        producer.send('map', serialized)
        #Aquí esperamos un segundo y lo volvemos a mandar
        #print(cadena)
        time.sleep(1)



def readClients():
    #Crear un consumidor de Kafka
    consumer = KafkaConsumer('clients', bootstrap_servers=f'{sys.argv[2]}:{sys.argv[3]}')
    #Recibir los clientes
    for message in consumer:
        id = message.value.decode('utf-8')
        print(f"Cliente {id} conectado")
        client = Cliente(id, LOCALIZACIONES, TAXIS, CLIENTES)

        CLIENTES.append(client)

def serviceRequest():
    #Crear un consumidor de Kafka
    consumer = KafkaConsumer('service_requests', bootstrap_servers=f'{sys.argv[2]}:{sys.argv[3]}')
    #Recibir los clientes
    for message in consumer:
        servicio = pickle.loads(message.value)
        for cliente in CLIENTES:
            if cliente.getId() == servicio.getCliente():
                servicio.setOrigen(cliente.getPosicion())
        print(f"Cliente {servicio.getCliente()} solicita un taxi")
        #Buscamos un taxi libre
        for taxi in TAXIS:
            if taxi.getEstado():
                taxi.setEstado(False)
                taxi.setCliente(servicio.getCliente())
                print(f"Taxi {taxi.getId()} asignado al cliente {servicio.getCliente()}")
                producer = KafkaProducer('service_assigned',bootstrap_servers=f'{sys.argv[2]}:{sys.argv[3]}')
                producer.send('service_assigned', value = pickle.dumps(servicio))
                break
                

        #Si no hay taxis libres, lo añadimos a la lista de espera

        

def main():
    # Comprobar que se han pasado los argumentos correctos
    if len(sys.argv) != 4:
        print("Usage: python EC_Central.py Port Bootstrap_IP Bootstrap_Port")
        sys.exit(1)
    
    # Leer las localizaciones y taxis disponibles
    leerLocalizaciones(LOCALIZACIONES)
    leerTaxis(TAXIS_DISPONIBLES)

    # Crear el mapa    

    # Iniciar el servidor de autenticación en un hilo
    auth_thread = threading.Thread(target=autheticate_taxi)
    auth_thread.start()

    map_thread = threading.Thread(target=sendMap)
    map_thread.start()

    #Leer los clientes
    clients_thread = threading.Thread(target=readClients)
    clients_thread.start()

    #Leer los servicios
    services_thread = threading.Thread(target=serviceRequest)
    services_thread.start()
    
    auth_thread.join()
    map_thread.join()
    clients_thread.join()
    services_thread.join()

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
