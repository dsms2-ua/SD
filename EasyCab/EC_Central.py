import sys

if sys.version_info >= (3, 12, 0):
    import six
    sys.modules['kafka.vendor.six.moves'] = six.moves

import socket
import threading
import time
import pickle
import os
import json
import subprocess
import keyboard
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

stop_threads = False

#Creamos las funciones que nos sirven para leer los archivos de configuración
def leerLocalizaciones(localizaciones):
    with open("EC_locations.json", "r") as file:
        data = json.load(file)
        for item in data['locations']:
            id = item['Id']
            pos = item['POS']
            x, y = map(int, pos.split(','))
            localizaciones[id] = Casilla(x, y)

def leerTaxis(taxis):
    with open("taxis.txt", "r") as file:
        for linea in file:
            id = int(linea.strip())
            taxis.append(id)

#Creamos la función que gestiona la autenticación por sockets
def autheticate_taxi():
    #Creamos el socket del servidor con la direccion por parametros
    server_socket = socket.socket()
    server_socket.bind(('localhost', int(sys.argv[1])))
    server_socket.listen(5)

    while not stop_threads:
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
                taxi.setCasilla(Casilla(1, 1))
                #Borramos el taxi de la lista de taxis disponibles
                TAXIS_DISPONIBLES.remove(id)
                ocupada = True
                while ocupada:
                    ocupada = False
                    for t in TAXIS:
                        if t.getCasilla() == taxi.getCasilla():
                            # Intentar mover en la dirección X primero
                            nueva_x = taxi.getCasilla().getX() + 1
                            nueva_y = taxi.getCasilla().getY()
                            
                            # Si la nueva posición en X está ocupada, intentar mover en la dirección Y
                            while any(t.getCasilla() == Casilla(nueva_x, nueva_y) for t in TAXIS):
                                nueva_x += 1
                                if nueva_x == 20:
                                    nueva_x = 1
                                    nueva_y += 1
                            
                            taxi.setCasilla(Casilla(nueva_x, nueva_y))
                            ocupada = True
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


#Función para enviar el mapa y para mostrar la tabla
def sendMap():
    #Creamos el productor de Kafka
    producer = KafkaProducer(bootstrap_servers=f'{sys.argv[2]}:{sys.argv[3]}')
    #Añadimos el mapa

    mapa = Mapa(LOCALIZACIONES, TAXIS, CLIENTES)
    while not stop_threads:
        serialized = pickle.dumps(mapa)
        producer.send('map', serialized)
        str = generarTabla(TAXIS, CLIENTES)
        os.system('cls')
        print(str)
        print(mapa.cadenaMapa())

        #Aquí esperamos un segundo y lo volvemos a mandar
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
    #crear productor de Kafka
    producer = KafkaProducer(bootstrap_servers=f'{sys.argv[2]}:{sys.argv[3]}')
    #Recibir los clientes
    for message in consumer:
        #Cogemos el mensaje y creamos el objeto servicio
        id, destino = message.value.decode('utf-8').split()
        servicio = Servicio(id, destino)

        #Enviamos con el producer el cliente que ha solicitado un servicio
        producer.send('client_service', value = f"{id} {destino}".encode('utf-8'))

        #Iteramos sobre la lista de clientes activos para obtener la posición del cliente
        for cliente in CLIENTES:
            if cliente.getId() == servicio.getCliente():
                servicio.setPosCliente(cliente.getPosicion())
                cliente.setDestino(destino)
                for loc in LOCALIZACIONES:
                    if loc == destino:
                        servicio.setPosDestino(LOCALIZACIONES[loc])

        print(f"Cliente {servicio.getCliente()} solicita un taxi")        
        #Buscamos un taxi libre
        asignado = False
        intentos = 0
        max_intentos = 3

        while not asignado and intentos < max_intentos:
            for taxi in TAXIS:
                if not taxi.getOcupado():
                    servicio.setOrigen(taxi.getCasilla())
                    servicio.setTaxi(taxi.getId())

                    asignado = True
                    taxi.setOcupado(True)
                    taxi.setCliente(servicio.getCliente())
                    taxi.setOrigen(taxi.getCasilla()) # Desde donde partimos
                    taxi.setPosCliente(servicio.getPosCliente()) # Desde donde parte el cliente
                    taxi.setDestino(servicio.getDestino()) # A donde va el cliente

                    for loc in LOCALIZACIONES:
                        if loc == servicio.getDestino():
                            taxi.setPosDestino(LOCALIZACIONES[loc])

                    print(f"Taxi {taxi.getId()} asignado al cliente {servicio.getCliente()}")
                    producer.send('service_assigned_client', value=f"{servicio.getCliente()} OK {taxi.getId()} es el taxi asignado.".encode('utf-8'))

                    # Mandamos el objeto servicio
                    producer.send('service_assigned_taxi', pickle.dumps(servicio))
                    break

            if not asignado:
                intentos += 1
                if intentos < max_intentos:
                    print(f"Intento {intentos} fallido. Reintentando en 3 segundos...")
                    time.sleep(3)

        if not asignado:
            producer.send('service_completed', value=f"{servicio.getCliente()} KO".encode('utf-8'))

            
def readTaxiUpdate():
    #Crear un consumidor de Kafka
    consumer = KafkaConsumer('taxiUpdate', bootstrap_servers=f'{sys.argv[2]}:{sys.argv[3]}')
    #Recibir los clientes
    while not stop_threads:
        for message in consumer:
            id, estado = message.value.decode('utf-8').split() 
            for taxi in TAXIS:
                if taxi.getId() == int(id):
                    if estado == "KO" and taxi.getEstado() == True:
                        taxi.setEstado(False) #Establecemos el taxi con estado KO
                        #TODO: ¿Qué hacemos con el cliente cuando está subido a un taxi y se para?

                    elif estado == "OK" and taxi.getEstado() == False:
                        taxi.setEstado(True)

                    break

def readTaxiMovements():
    #Crear un consumidor de Kafka
    consumer = KafkaConsumer('taxiMovements', bootstrap_servers=f'{sys.argv[2]}:{sys.argv[3]}')

    for message in consumer:
        id, x, y = message.value.decode('utf-8').split()
        for taxi in TAXIS:
            if taxi.getId() == int(id):
                taxi.setCasilla(Casilla(int(x), int(y)))
                if taxi.getRecogido():
                    #Tenemos que actualizar la posición del cliente
                    for cliente in CLIENTES:
                        if cliente.getId() == taxi.getCliente():
                            cliente.setPosicion(taxi.getCasilla())
                            break

                if taxi.getPosCliente() == taxi.getCasilla():
                    taxi.setRecogido(True)

                if taxi.getPosDestino() == taxi.getCasilla():
                    #El cliente ya ha llegado y tenemos que actualizar todos los datos
                    for cliente in CLIENTES:
                        if cliente.getId() == taxi.getCliente():
                            cliente.setDestino(None)
                            break
                    
                    #Tengo que notificar al cliente que ha llegado a la posición y puedo procesar la siguiente petición
                    producer = KafkaProducer(bootstrap_servers=f'{sys.argv[2]}:{sys.argv[3]}')
                    producer.send('service_completed', value = f"{taxi.getCliente()} OK".encode('utf-8'))

                    taxi.setOcupado(False)
                    taxi.setRecogido(False)
                    taxi.setCliente(None)
                break

def handleCommands(producer):
        #Enviamos el productor por comandos
        #producer = KafkaProducer(bootstrap_servers=f'{sys.argv[2]}:{sys.argv[3]}')

        while not stop_threads:
            print("Órdenes disponibles:")
            print("1. Parar")
            print("2. Reanudar")
            print("3. Ir a destino")
            print("4. Volver a base")
            
            
            command = input("Ingrese un comando (parar, reanudar, ir_a_destino, volver_a_base): ").strip().lower()
            taxi_id = input("Ingrese el ID del taxi: ").strip()

            if command == "parar":
                for taxi in TAXIS:

                    if str(taxi.getId()) == taxi_id:
                        taxi.setEstado(False)
                        print(f"Taxi {taxi_id} ha parado.")
                        producer.send('taxi_commands', value = f"{taxi_id} KO".encode('utf-8'))
                        break
            elif command == "reanudar":
                for taxi in TAXIS:
                    if taxi.getId() == int(taxi_id):
                        taxi.setEstado(True)
                        print(f"Taxi {taxi_id} ha reanudado el servicio.")
                        producer.send('taxi_commands', value = f"{taxi_id} OK".encode('utf-8'))
                        break
            elif command == "ir_a_destino":
                destino_x = int(input("Ingrese la coordenada X del destino: ").strip())
                destino_y = int(input("Ingrese la coordenada Y del destino: ").strip())
                for taxi in TAXIS:
                    if taxi.getId() == int(taxi_id):
                        taxi.setDestino(Casilla(destino_x, destino_y))
                        producer.send('taxi_commands', value = f"{taxi_id} {destino_x} {destino_y}".encode('utf-8'))
                        print(f"Taxi {taxi_id} se dirige a ({destino_x}, {destino_y}).")
                        break
            elif command == "volver_a_base":
                for taxi in TAXIS:
                    if taxi.getId() == int(taxi_id):
                        taxi.setDestino(Casilla(1, 1))
                        producer.send('taxi_commands', value = f"{taxi_id} {1} {1}".encode('utf-8'))
                        print(f"Taxi {taxi_id} vuelve a la base (1, 1).")
                        break
            else:
                print("Comando no reconocido.")

def open_command_terminal():
    path = os.getcwd().replace("\\", "\\\\")
    if sys.platform == "win32":
        subprocess.Popen(["start", "cmd", "/k", "python", "-c", f'import sys; sys.path.append(r"{path}"); from EC_Central import handleCommands; handleCommands()'], shell=True)
    else:
        subprocess.Popen(["gnome-terminal", "--", "python3", "-c", f'import sys; sys.path.append(r"{path}"); from EC_Central import handleCommands; handleCommands()'])


def leerTeclado():
    global stop_threads
    while not stop_threads:
        if keyboard.is_pressed('t'):
            stop_threads = True
            time.sleep(5)
    

def main():
    # Comprobar que se han pasado los argumentos correctos
    if len(sys.argv) != 4:
        print("Usage: python EC_Central.py Port Bootstrap_IP Bootstrap_Port")
        sys.exit(1)
    
    # Leer las localizaciones y taxis disponibles
    leerLocalizaciones(LOCALIZACIONES)
    leerTaxis(TAXIS_DISPONIBLES)

    # Iniciar el servidor de autenticación en un hilo
    auth_thread = threading.Thread(target=autheticate_taxi)
    auth_thread.start()

    # Enviar el mapa
    map_thread = threading.Thread(target=sendMap)
    map_thread.start()

    #Leer los clientes
    clients_thread = threading.Thread(target=readClients)
    clients_thread.start()

    #Leer los servicios
    services_thread = threading.Thread(target=serviceRequest)
    services_thread.start()

    #Leer las actualizaciones de los taxis
    taxiUpdate_thread = threading.Thread(target=readTaxiUpdate)
    taxiUpdate_thread.start()

    #Leer los movimientos de los taxis
    taxiMovement_thread = threading.Thread(target=readTaxiMovements)
    taxiMovement_thread.start()
    
    #Iniciar la terminal que lee los comandos
    open_command_terminal()

    teclado_thread = threading.Thread(target=leerTeclado)
    teclado_thread.daemon = True
    teclado_thread.start()
    
    auth_thread.join()
    map_thread.join()
    clients_thread.join()
    services_thread.join()
    taxiUpdate_thread.join()
    taxiMovement_thread.join()
    teclado_thread.join()

# Iniciar el servidor de autenticación y el manejo de Kafka en paralelo
if __name__ == "__main__":
    main()
