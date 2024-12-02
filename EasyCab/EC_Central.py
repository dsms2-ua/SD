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
from kafka import KafkaProducer, KafkaConsumer
from Clases import *
import requests

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
    with open("EC_locations.json", "r") as file:
        data = json.load(file)
        for item in data['locations']:
            id = item['Id']
            pos = item['POS']
            x, y = map(int, pos.split(','))
            localizaciones[id] = Casilla(x, y)

def leerTaxis(taxis):
    with open("EC_Taxis.json", "r") as file:
        data = json.load(file)
        for item in data['Taxis']:
            id = item['Id']
            taxis.append(int(id))

#Creamos la función que gestiona la autenticación por sockets
def autheticate_taxi():
    # Creamos el socket del servidor con la dirección pasada por parámetro
    server_socket = socket.socket()
    server_socket.bind(('0.0.0.0', int(sys.argv[1])))
    server_socket.listen(5)

    while True:
        # Permitimos la conexión y recibimos los datos
        client, addr = server_socket.accept()

        # Recibimos el mensaje en formato bytes
        message = client.recv(1024)
        
        # Verificamos el mensaje
        data = verify_message(message)
        if data is None:
            # Enviar un NACK si el mensaje es incorrecto
            client.send(NACK)
            client.close()
            continue

        try:
            # Procesamos el ID del taxi
            taxi_id = int(data)
            if taxi_id in TAXIS_DISPONIBLES:
                # Creamos el objeto taxi y lo ubicamos en una posición no ocupada
                taxi = Taxi(taxi_id)
                taxi.setCasilla(Casilla(1, 1))
                TAXIS_DISPONIBLES.remove(taxi_id)
                
                # Añadimos el taxi a la lista de taxis y respondemos con "OK"
                TAXIS.append(taxi)
                response = create_message("OK")
                client.send(response)
            #comprobamos si el taxi ya está autenticado pero esta en no visible
            else:
                aux = False
                for t in TAXIS:
                    if t.getId() == taxi_id and t.getVisible() == False:
                        t.setVisible(True)
                        response = create_message("OK")
                        client.send(response)
                        aux = True
                        break

                # Mandamos un mensaje al taxi de error
                if not aux:
                    response = create_message("ERROR")
                    client.send(response)
        except ValueError:
            response = create_message("ERROR")
            client.send(response)
        finally:
            client.close()

#Función para enviar el mapa y para mostrar la tabla
def sendMap():
    #Creamos el productor de Kafka
    producer = KafkaProducer(bootstrap_servers=f'{sys.argv[2]}:{sys.argv[3]}')
    #Añadimos el mapa

    mapa = Mapa(LOCALIZACIONES, TAXIS, CLIENTES)
    while True:
        serialized = pickle.dumps(mapa)
        producer.send('map', serialized)
        str = generarTabla(TAXIS, CLIENTES,LOCALIZACIONES)
        os.system('cls')
        print(str)
        print(mapa.cadenaMapa())

        #Aquí esperamos un segundo y lo volvemos a mandar
        time.sleep(0.5)

def readClients():
    #Crear un consumidor de Kafka
    consumer = KafkaConsumer('clients', bootstrap_servers=f'{sys.argv[2]}:{sys.argv[3]}')
    #Recibir los clientes
    for message in consumer:
        id = message.value.decode('utf-8')
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

        #Buscamos un taxi libre
        asignado = False
        intentos = 0
        max_intentos = 3

        while not asignado and intentos < max_intentos:
            for taxi in TAXIS:
                if not taxi.getOcupado() and taxi.getEstado():
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

                    producer.send('service_assigned_client', value=f"{servicio.getCliente()} OK {taxi.getId()} es el taxi asignado.".encode('utf-8'))

                    # Mandamos el objeto servicio
                    producer.send('service_assigned_taxi', pickle.dumps(servicio))
                    break

            if not asignado:
                intentos += 1
                if intentos < max_intentos:
                    time.sleep(3)

        if not asignado:
            producer.send('service_completed', value=f"{servicio.getCliente()} KO".encode('utf-8'))

def readTaxiUpdate():
    #Crear un consumidor de Kafka
    consumer = KafkaConsumer('taxiUpdate', bootstrap_servers=f'{sys.argv[2]}:{sys.argv[3]}')
    #Recibir los clientes
    while True:
        for message in consumer: 
            id, estado, posX, posY = message.value.decode('utf-8').split() 
            estado = True if estado == "True" else False
            for taxi in TAXIS:
                if taxi.getId() == int(id):
                    taxi.setTimeout(0)
                    if not estado and taxi.getEstado() == True:
                        taxi.setEstado(False) #Establecemos el taxi con estado KO
                        taxi.setOcupado(False)
                        taxi.setRecogido(False)
                        taxi.setCliente(None)
                        #TODO: ¿Qué hacemos con el cliente cuando está subido a un taxi y se para?
                    elif estado and taxi.getEstado() == False:
                        taxi.setEstado(True)
                        taxi.setCasilla(Casilla(int(posX), int(posY)))
                    elif estado and taxi.getEstado() == True and taxi.getCliente() == None:
                        taxi.setCasilla(Casilla(int(posX), int(posY)))
                    elif estado and taxi.getEstado() == True and taxi.getCliente() != None: 
                        taxi.setCasilla(Casilla(int(posX), int(posY)))
                        if taxi.getRecogido():
                            #Tenemos que actualizar la posición del cliente
                            for cliente in CLIENTES:
                                if cliente.getId() == taxi.getCliente():
                                    cliente.setPosicion(taxi.getCasilla())
                                    break
                    else:
                        taxi.setCasilla(Casilla(int(posX), int(posY)))
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
                            taxi.setDestino(None)
                            taxi.setPosDestino(None)
                    break
"""
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
"""
def receiveCommand():
    
    #creamos consumer dekafka
    consumer = KafkaConsumer('taxi_commands','taxi_commands2', bootstrap_servers=f'{sys.argv[2]}:{sys.argv[3]}')

    #creamos producer de Kafka
    producer = KafkaProducer(bootstrap_servers=f'{sys.argv[2]}:{sys.argv[3]}')

    for message in consumer:
        command = message.value.decode('utf-8').split()
        topic = message.topic
        taxi_id = command[0]
        action = command[1]

        if topic == "taxi_commands":
            if action == "KO":
                for taxi in TAXIS:
                    if taxi.getId() == int(taxi_id):
                        taxi.setEstado(False)
                        taxi.setCliente(None)
                        taxi.setOcupado(False)
                        taxi.setRecogido(False)
                        taxi.setDestino(None)
                        producer.send('taxi_orders', value = f"{taxi_id} KO".encode('utf-8'))
                        break
            else:
                for taxi in TAXIS:
                    if taxi.getId() == int(taxi_id):
                        taxi.setEstado(True)
                        producer.send('taxi_orders', value = f"{taxi_id} OK".encode('utf-8'))
                        break
        elif topic == "taxi_commands2":
            for taxi in TAXIS:
                if taxi.getId() == int(taxi_id):
                    inicioX = taxi.getCasilla().getX()
                    inicioY = taxi.getCasilla().getY()
                    taxi.setPosDestino(Casilla(int(action.split(',')[0]), int(action.split(',')[1])))
                    producer.send('service_completed', value = f"{taxi.getCliente()} KO".encode('utf-8'))
                    taxi.setRecogido(False)
                    taxi.setCliente(None)
                    taxi.setOcupado(True)
                    dest = f"{action.split(',')[0]},{action.split(',')[1]}"
                    taxi.setDestino(dest)
                    producer.send('taxi_orders', value = f"{taxi_id} {action} {inicioX} {inicioY}".encode('utf-8'))
                    break    

def handleCommands(ip,port):
        
        #Enviamos el productor por comandos
        from kafka import KafkaProducer

        producer = KafkaProducer(bootstrap_servers=f'{ip}:{port}')
        
        while True:
            print("Órdenes disponibles:")
            print("1. Parar")
            print("2. Reanudar")
            print("3. Ir a destino")
            print("4. Volver a base")
            
            command = input("Ingrese una opción : ")
            taxi_id = input("Ingrese el ID del taxi: ")

            if command == "1":
                producer.send('taxi_commands', value = f"{taxi_id} KO".encode('utf-8'))
            elif command == "2":
                producer.send('taxi_commands', value = f"{taxi_id} OK".encode('utf-8'))
            elif command == "3":
                destino = input("Ingrese el destino x,y: ")
                producer.send('taxi_commands2', value = f"{taxi_id} {destino}".encode('utf-8'))
            elif command == "4":
                destino = "1,1"
                producer.send('taxi_commands2', value = f"{taxi_id} {destino}".encode('utf-8'))
            else:
                print("Comando no válido")
                continue

def open_command_terminal():
    path = os.getcwd().replace("\\", "\\\\")
    if sys.platform == "win32":
        subprocess.Popen(["start", "cmd", "/k", "python", "-c", f'import sys; sys.path.append(r"{path}"); from EC_Central import handleCommands; handleCommands("{sys.argv[2]}","{sys.argv[3]}")'], shell=True)
    else:
        subprocess.Popen(["gnome-terminal", "--", "python3", "-c", f'import sys; sys.path.append(r"{path}"); from EC_Central import handleCommands; handleCommands("{sys.argv[2]}","{sys.argv[3]}")'])

def taxiDisconection():
    while True:
        for taxi in TAXIS:
            if taxi.getTimeout() < 5:
                taxi.setTimeout(taxi.getTimeout() + 1)
            else:
                taxi.setEstado(False)
                taxi.setOcupado(False)
                taxi.setRecogido(False)
                taxi.setCliente(None)
                taxi.setDestino(None)
                taxi.setPosDestino(None)
                taxi.setVisible(False)
        time.sleep(1)
        
def customerDisconnection():
    while True:
        for cliente in CLIENTES:
            if cliente.getTimeout() < 5:
                cliente.setTimeout(cliente.getTimeout() + 1)
            else:
                CLIENTES.remove(cliente)
        time.sleep(1)
        
def customerState():
    consumer = KafkaConsumer('customerOK', bootstrap_servers=f'{sys.argv[2]}:{sys.argv[3]}')
    while True:
        for message in consumer:
            id, st = message.value.decode('utf-8').split()
            for cliente in CLIENTES:
                if cliente.getId() == id:
                    cliente.setTimeout(0)

def reconexion():
    start_time = time.time()
    consumer = KafkaConsumer('taxiUpdate', bootstrap_servers=f'{sys.argv[2]}:{sys.argv[3]}')

    while time.time() - start_time < 3:
        message = consumer.poll(timeout_ms=500)  
        if message:
            for tp, messages in message.items():
                for msg in messages:
                    if time.time() - start_time >= 3:
                        break  # Sale del bucle for interno si se excede el tiempo
                    id, estado, posX, posY = msg.value.decode('utf-8').split()
                    estado = True if estado == "True" else False
                    id = int(id)
                    taxi = Taxi(id)
                    taxi.setEstado(estado)
                    taxi.setCasilla(Casilla(int(posX), int(posY)))
                    taxi.setVisible(True)
                    if id in TAXIS_DISPONIBLES:
                        TAXIS.append(taxi)
                        TAXIS_DISPONIBLES.remove(id)
                        print(f"Taxi {id} reconectado")
        if time.time() - start_time >= 3:
            break  # Sale del bucle while si se excede el tiempo
"""
def reconexion():
    start_time = time.time()
    consumer = KafkaConsumer('taxiUpdate', bootstrap_servers=f'{sys.argv[2]}:{sys.argv[3]}')

    while time.time() - start_time < 3:
        message = consumer.poll(timeout_ms=1000)  # Polling con tiempo de espera de 1 segundo
        if message:
            for tp, messages in message.items():
                for msg in messages:
                    if time.time() - start_time >= 3:
                        break  # Sale del bucle for interno si se excede el tiempo
                    id, estado, posX, posY = msg.value.decode('utf-8').split()
                    taxi = Taxi(id)
                    taxi.setEstado(estado)
                    taxi.setCasilla(Casilla(int(posX), int(posY)))
                    TAXIS.append(taxi)
                    if id in TAXIS_DISPONIBLES:
                        TAXIS_DISPONIBLES.remove(id)
        else:
            print("Esperando mensajes...")

        if time.time() - start_time >= 3:
            break  # Sale del bucle while si se excede el tiempo
"""
def weatherState():
    #Hacemos una request a la API expuesta desde EC_CTC
    while True:
        temperatura = int(requests.get("http://localhost:5000/city"))
        time.sleep(10)

        
def main():
    # Comprobar que se han pasado los argumentos correctos
    if len(sys.argv) != 4:
        print("Usage: python EC_Central.py Port Bootstrap_IP Bootstrap_Port")
        sys.exit(1)
    
    
    # Leer las localizaciones y taxis disponibles
    leerLocalizaciones(LOCALIZACIONES)
    leerTaxis(TAXIS_DISPONIBLES)

    reconexion()

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
    #taxiMovement_thread = threading.Thread(target=readTaxiMovements)
    #taxiMovement_thread.start()
    
    #Iniciar la terminal que lee los comandos
    open_command_terminal()
    

    receiveCommand_thread = threading.Thread(target=receiveCommand)
    receiveCommand_thread.start()
    
    taxi_disconection_thread = threading.Thread(target=taxiDisconection)
    taxi_disconection_thread.start()
    
    customerDisconnection_thread = threading.Thread(target=customerDisconnection)
    customerDisconnection_thread.start()
    
    customerState_thread = threading.Thread(target=customerState)
    customerState_thread.start()
    
    weather_thread = threading.Thread(target=weatherState)
    weather_thread.start()
    

    auth_thread.join()
    map_thread.join()
    clients_thread.join()
    services_thread.join()
    taxiUpdate_thread.join()
    #taxiMovement_thread.join()
    receiveCommand_thread.join()
    taxi_disconection_thread.join()
    customerDisconnection_thread.join()
    customerState_thread.join()
    weather_thread.join()

# Iniciar el servidor de autenticación y el manejo de Kafka en paralelo
if __name__ == "__main__":
    main()
