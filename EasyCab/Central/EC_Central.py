import socket
import threading
from kafka import Producer, Consumer
import sys
from Casilla import Casilla
from Taxi import Taxi
from Mapa import Mapa

#Aquí guardo las localizaciones leídas por fichero: {ID:CASILLA}
LOCALIZACIONES = {}
# Diccionario para almacenar los taxis que tengo disponibles
TAXIS_DISPONIBLES = []
#Lista de taxis para guardar los que ya están autenticados con elementos Taxi
TAXIS = []

#Creamos las funciones que nos sirven para leer los archivos de configuración
def leerLocalizaciones(localizaciones):
    with open("map_config.txt", "r") as file:
        for line in file:
            linea = linea.strip()
            if linea:
                partes = linea.split()
                id = int(partes[0])
                x = int(partes[1])
                y = int(partes[2])
                localizaciones[id] = Casilla(x, y)

def leerTaxis(taxis):
    with open("taxis.text", "r") as file:
        for linea in file:
            id = int(linea.strip())
            taxis.append(id)

#Definimos las funciones para crear el consumidor y el productor
def createProducer(bootstrapServer):
    conf = {'bootstrap.serves': bootstrapServer}
    return Producer(conf)

def createConsumer(topic, group, bootstrapServer):
    conf = {'bootstrap.serves': bootstrapServer, 'group.id': group, 'auto.offset.reset': 'earliest'}
    consumer = Consumer(conf)
    consumer.subscribe([topic])
    return consumer

#Creamos la función que gestiona la autenticación por sockets
def autheticate_taxi(client, address):
    while True:
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
                #Cerramos la conexión
                client.close()
            else:
                #Mandamos un mensaje al taxi de error
                client.send("ERROR".encode('utf-8'))
                client.close()
        except ValueError:
            client.send("ERROR".encode('utf-8'))
            client.close()


#Función que crea el servidor de sockets
def socketServer(port):
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind(('0.0.0.0', int(port)))
    server_socket.listen(5)

    while True:
        client_socket, addr = server_socket.accept()
        client_handler = threading.Thread(target=autheticate_taxi, args=(client_socket, addr))
        client_handler.start()


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





# Iniciar el servidor de autenticación y el manejo de Kafka en paralelo
if __name__ == "__main__":
    #Comprobamos que los argumentos se han pasado correctamente
    if len(sys.argv) != 4:
        print("Usage: python EC_Central.py Port Bootstrap_IP Bootstrap_Port")
        sys.exit(1)

    puerto = sys.argv[1]
    bootstrap_ip = sys.argv[2]
    bootstrap_port = sys.argv[3]

    #Ahora leemos el archivo de configuración del mapa y el de taxis
    leerLocalizaciones(LOCALIZACIONES)
    leerTaxis(TAXIS_DISPONIBLES)

    #Creamos el mapa

    #Creamos el producer y el consumer y un hilo para cada topic
    bootstrapServer = f"{bootstrap_ip}:{bootstrap_port}"
    producer = createProducer(bootstrapServer)

    hilo_mapa = threading.Thread(target=createConsumer, args=("map_updates", "central_group", bootstrapServer))
    

    # Iniciar el servidor de autenticación en un hilo
    auth_thread = threading.Thread(target=socketServer, args=(puerto,))
    auth_thread.start()

    # Manejar Kafka en el hilo principal
    handle_kafka_messages()
