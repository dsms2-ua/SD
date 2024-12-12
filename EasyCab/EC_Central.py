import secrets
import sys

if sys.version_info >= (3, 12, 0):
    import six
    sys.modules['kafka.vendor.six.moves'] = six.moves

import socket, ssl
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

estadoTrafico = "OK"
temperatura = 0
city = ""

ctc = CTC(city, temperatura, estadoTrafico)

#claves AES de los taxis
taxi_keys = {}

def escribirEventos(mensaje):
    #Cogemos la hora y el día para poder escribirlo en el fichero
    hora = time.strftime("%H:%M:%S")
    dia = time.strftime("%Y/%m/%d")
    str = f"{dia} {hora} => {mensaje}\n"
    
    #Abrimos el archivo en modo append y escribimos el mensaje
    with open("Auditoria/events.txt", "a") as file:
        file.write(str)

#Creamos las funciones que nos sirven para leer los archivos de configuración
def leerLocalizaciones(localizaciones):
    with open("EC_locations.json", "r") as file:
        data = json.load(file)
        for item in data['locations']:
            id = item['Id']
            pos = item['POS']
            x, y = map(int, pos.split(','))
            localizaciones[id] = Casilla(x, y)

'''
def leerTaxis(taxis):
    with open("EC_Taxis.json", "r") as file:
        data = json.load(file)
        for item in data['Taxis']:
            id = item['Id']
            taxis.append(int(id))
'''

#Creamos la función que gestiona la autenticación por sockets
#Tenemos que crear un servidor seguro
def authenticate_taxi():
    cert = 'certificados/certSocketsSans.pem'
    key = 'certificados/keySans.pem'
    context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    port = int(sys.argv[1])
    context.load_cert_chain(certfile=cert, keyfile=key)
    
    #context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    #context.load_cert_chain(certfile="mycertfile", keyfile="mykeyfile")
    
    # Creamos el socket del servidor con la dirección pasada por parámetro
    server_socket = socket.socket()
    server_socket.bind(('0.0.0.0', port))
    server_socket.listen(5)

    while True:
        # Permitimos la conexión y recibimos los datos
        client, addr = server_socket.accept()
        consstream = context.wrap_socket(client, server_side=True)
        
        #Gestionamos el login
        try:
            id = consstream.recv(1024).decode('utf-8')
            
            isIn = False
            isVisible = False
            
            #Comprobamos que el taxi no hay iniciado sesión
            print(f"Taxi {id} intenta conectarse")
            for taxi in TAXIS:
                if taxi.getId() == int(id):
                    isIn = True
                    if taxi.getVisible():
                        isVisible = True
                        consstream.send(b'Logged')
                        escribirEventos(f"Intento de conexión de taxi {id} ya conectado desde {addr}")
                        consstream.close()
                        continue
                    
            if not isIn or not isVisible:
                print(f"Taxi {id} conectado")
                
                #Hacemos una petición GET a la API para ver si el texi está registrado o no
                response = requests.get(f'http://localhost:3000/taxi/{id}')
                
                if response.status_code == 404:
                    #Si no hemos encontrado el taxi
                    consstream.send(b'KO')
                    escribirEventos(f"Intento de conexión de taxi {id} no registrado desde {addr}")
                    consstream.close()
                    continue
                elif response.status_code == 200:
                    consstream.send(b'OK')
                    
                    #Ahora recibimos la contraseña y comprobamos si coincide con la registrada
                    password = consstream.recv(1024).decode('utf-8')
                    
                    #Hacemos un GET a la API por la contraseña
                    response = requests.get(f'http://localhost:3000/password/{id}')
                    
                    data = response.json()
                    
                    if password == data['password']:
                        escribirEventos(f"Taxi {id} ha iniciado sesión correctamente desde {addr}")
                        posicion = "1,1"
                        existe = False
                        token = secrets.token_hex(16)  # Genera un token aleatorio de 32 caracteres (16 bytes)
                        for taxi in TAXIS:
                            if taxi.getId() == int(id):
                                existe = True
                                taxi.setVisible(True)
                                taxi.setToken(token)
                                posicion = str(taxi.getCasilla().getX()) + "," + str(taxi.getCasilla().getY())
                                break
                            
                        #Como la contraseñas coinciden, generamos el token y la clave AES 
                        if not existe:
                            #creamos el objeto taxi
                            taxi = Taxi(int(id))
                            taxi.setCasilla(Casilla(1, 1))
                            taxi.setToken(token)
                            TAXIS.append(taxi)
                            aes_key = os.urandom(32)
                            # Guardar la clave AES en algún lugar seguro, como una base de datos o un diccionario en memoria
                            taxi_keys[int(id)] = aes_key
                        else:
                            aes_key = taxi_keys[int(id)]
                            
                        data = {"aes":base64.b64encode(aes_key).decode("utf-8")}
                        #Guardar clave AES en la API
                        response = requests.put(f'http://localhost:3000/aes/{id}', json=data)

                        data = {"token":token}

                        #Guardar el token en la API
                        response = requests.put(f'http://localhost:3000/token/{id}', json=data)

                        # Enviar el token y la clave AES al cliente
                        consstream.send(f'{token} {base64.b64encode(aes_key).decode("utf-8")} {posicion}'.encode('utf-8'))
                    else:
                        consstream.send(b'KO')
                        escribirEventos(f"Intento de conexión de taxi {id} con contraseña incorrecta desde {addr}")
                        consstream.close()
                        continue    
        except:
            print("Error en la autenticación")
            consstream.send(b'KO')
            consstream.close()
            continue
       
def escribirMapa(mapa):
    with open("mapa.txt", "w") as file:
        file.write(mapa)

def escribirTemperatura(ctc):
    cadena = ctc.cadenaCTC()
    return cadena

#Función para enviar el mapa y para mostrar la tabla
#Para la segunda entrega, escribimos el mapa en un archivo para exponerlo en la API
def sendMap():
    global estadoTrafico
    #Creamos el productor de Kafka
    producer = KafkaProducer(bootstrap_servers=f'{sys.argv[2]}:{sys.argv[3]}')
    #Añadimos el mapa

    mapa = Mapa(LOCALIZACIONES, TAXIS, CLIENTES)
    while True:
        serialized = pickle.dumps(mapa)
        producer.send('map', serialized)
        str = generarTabla(TAXIS, CLIENTES,LOCALIZACIONES, ctc)
        
        #Escribimos el mapa en el fichero
        mapaArchivo = generarTablaArchivo(TAXIS, CLIENTES, LOCALIZACIONES) + escribirTemperatura(ctc) + "\n"
        mapaArchivo += mapa.cadenaMapaArchivo() + "\n"
        escribirMapa(mapaArchivo)
        
        #os.system('cls')
        print(str)
        print(mapa.cadenaMapa())
        print("estadoTrafico: " + estadoTrafico)
        #Aquí esperamos un segundo y lo volvemos a mandar
        time.sleep(0.5)

def readClients():
    #Crear un consumidor de Kafka
    consumer = KafkaConsumer('clients', bootstrap_servers=f'{sys.argv[2]}:{sys.argv[3]}')
    producer = KafkaProducer(bootstrap_servers=f'{sys.argv[2]}:{sys.argv[3]}')
    #Recibir los clientes
    for message in consumer:
        id = message.value.decode('utf-8')
        
        isIn = False
        
        for cliente in CLIENTES:
            if cliente.getId() == id:
                isIn = True
                break
                
        if isIn:
            escribirEventos(f"Cliente {id} ya está conectado")
            producer.send('clients', value = f"{id} KO".encode('utf-8'))
        else:
            client = Cliente(id, LOCALIZACIONES, TAXIS, CLIENTES)
            CLIENTES.append(client)
            producer.send('clients', value = f"{id} OK".encode('utf-8'))
            escribirEventos(f"Cliente {id} ha iniciado sesión")

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
                        escribirEventos(f"Cliente {id} ha solicitado un taxi a {destino}")

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
                    escribirEventos(f"El taxi con id {taxi.getId()} ha sido asignado al cliente {servicio.getCliente()} para ir a {servicio.getDestino()}")

                    encrypted_service = encrypt(pickle.dumps(servicio), taxi_keys[taxi.getId()],False)
                    mensaje = f"{taxi.getToken()} {encrypted_service}"
                    # Mandamos el objeto servicio
                    print(mensaje)
                    producer.send('service_assigned_taxi', value=mensaje.encode('utf-8'))
                    break

            if not asignado:
                intentos += 1
                if intentos < max_intentos:
                    time.sleep(3)

        if not asignado:
            producer.send('service_completed', value=f"{servicio.getCliente()} KO".encode('utf-8'))
            escribirEventos(f"No se ha podido asignar un taxi al cliente {servicio.getCliente()} para ir a {servicio.getDestino()}")
  
def readTaxiUpdate():
    
    try:
        #Crear un consumidor de Kafka
        consumer = KafkaConsumer('taxiUpdate', bootstrap_servers=f'{sys.argv[2]}:{sys.argv[3]}')
        #Recibir los taxis
        
        for message in consumer: 
            token,mensaje = message.value.decode('utf-8').split()
            for taxi in TAXIS:
                if taxi.getToken() == token:
                    mensaje_desencriptado = decrypt(mensaje, taxi_keys[taxi.getId()],True)
                    estado, posX, posY = mensaje_desencriptado.split()
                    estado = True if estado == "True" else False
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
                
    except Exception as e:
        print(f"Error al conectar con Kafka: {e}")
            

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
        mensaje = ""
        if topic == "taxi_commands":
            if action == "KO":
                for taxi in TAXIS:
                    if taxi.getId() == int(taxi_id):
                        taxi.setEstado(False)
                        taxi.setCliente(None)
                        taxi.setOcupado(False)
                        taxi.setRecogido(False)
                        taxi.setDestino(None)
                        token = taxi.getToken()
                        mensaje = f"KO"
                        #producer.send('taxi_orders', value = f"{taxi_id} KO".encode('utf-8'))
                        break
            else:
                for taxi in TAXIS:
                    if taxi.getId() == int(taxi_id):
                        taxi.setEstado(True)
                        token = taxi.getToken()
                        mensaje = f"OK"
                        #producer.send('taxi_orders', value = f"{taxi_id} OK".encode('utf-8'))
                        break
        elif topic == "taxi_commands2":
            for taxi in TAXIS:
                if taxi.getId() == int(taxi_id):
                    taxi.setPosDestino(Casilla(int(action.split(',')[0]), int(action.split(',')[1])))
                    producer.send('service_completed', value = f"{taxi.getCliente()} KO".encode('utf-8'))
                    taxi.setRecogido(False)
                    taxi.setCliente(None)
                    taxi.setOcupado(True)
                    dest = f"{action.split(',')[0]},{action.split(',')[1]}"
                    taxi.setDestino(dest)
                    token = taxi.getToken()
                    mensaje = f"{action}"
                    #producer.send('taxi_orders', value = f"{taxi_id} {action} {inicioX} {inicioY}".encode('utf-8'))
    
        mensaje = encrypt(mensaje, taxi_keys[int(taxi_id)],True)
        final = f"{token} {mensaje}"
        producer.send('taxi_orders', value = final.encode('utf-8'))

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
                
            if taxi.getTimeout() == 5:
                escribirEventos(f"Taxi {taxi.getId()} ha sido desconectado por inactividad")
        time.sleep(1)
        
def customerDisconnection():
    while True:
        for cliente in CLIENTES:
            if cliente.getTimeout() < 5:
                cliente.setTimeout(cliente.getTimeout() + 1)
            else:
                CLIENTES.remove(cliente)
                escribirEventos(f"Cliente {cliente.getId()} ha sido desconectado por inactividad")
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
            escribirEventos("La central se ha recuperado de un fallo")
            for tp, messages in message.items():
                for msg in messages:
                    if time.time() - start_time >= 3:
                        break
                    token, mensaje = msg.value.decode('utf-8').split()

                    #Obtenemos clave AES de la API usando el token
                    response = requests.get(f'http://localhost:3000/aes/token/{token}')
                    try:
                        data = response.json()
                    except requests.exceptions.JSONDecodeError:
                        print("Error: La respuesta no contiene un JSON válido.")
                        continue
                    aux = False
                    for taxi in TAXIS:
                        if taxi.getToken() == token:
                            aux = True
                            break
                    #si ya está autenticado, vamos al siguiente mensaje
                    if not aux:          
                        aes_key = base64.b64decode(data['aes'])
                        mensaje_desencriptado = decrypt(mensaje, aes_key, True)
                        estado, posX, posY = mensaje_desencriptado.split()
                        estado = True if estado == "True" else False
                        data = requests.get(f'http://localhost:3000/id/token/{token}')
                        data = data.json()
                        id = data['idTaxi']
                        id = int(id)
                        taxi = Taxi(id)
                        taxi.setEstado(estado)
                        taxi.setCasilla(Casilla(int(posX), int(posY)))
                        taxi.setVisible(True)
                        taxi.setToken(token)
                        TAXIS.append(taxi)
                        print(f"Taxi {id} reconectado")
        if time.time() - start_time >= 3:
            escribirEventos("Iniciando servicio de la central. Esperando conexiones")
            break  # Sale del bucle while si se excede el tiempo


def weatherState():
    global ctc, estadoTrafico
    url = "http://localhost:3002/city"
    #Hacemos una request a la API expuesta desde EC_CTC
    while True:
        try:
            response = requests.get(url)
            escribirEventos(f"El estado del tiempo ha sido actualizado: {ctc.cadenaCTC()}")
            if response.status_code == 200:
                data = response.json()
                status = data.get('status')
                ctc.setCiudad(data.get('city'))
                ctc.setTemperatura(data.get('temperature'))
                ctc.setEstado(data.get('status'))

                if status == "KO" and estadoTrafico == "OK":
                    estadoTrafico = "KO"
                    print("Estado del tráfico malito")
                    #mandamos todos los taxis a la base
                    producer = KafkaProducer(bootstrap_servers=f'{sys.argv[2]}:{sys.argv[3]}')

                    for taxi in TAXIS:
                        taxi_id = taxi.getId()
                        destino = "1,1"
                        producer.send('taxi_commands2', value = f"{taxi_id} {destino}".encode('utf-8'))
                elif status == "OK" and estadoTrafico == "KO":
                    estadoTrafico = "OK"
                time.sleep(10)
        except Exception as e:
            print(f"Error al conectar con la API del tiempo: {e}")
            #Si no podemos acceder al CTC porque esta desconectado, modificamos la ciudad
            #para que se muestre un mensaje de error
            ctc.setCiudad("Error")
            
            #Restringimos el funcionamiento del sistema
            estadoTrafico = "KO"
            producer = KafkaProducer(bootstrap_servers=f'{sys.argv[2]}:{sys.argv[3]}')
            
            for taxi in TAXIS:
                taxi_id = taxi.getId()
                destino = "1,1"
                producer.send('taxi_commands2', value = f"{taxi_id} {destino}".encode('utf-8'))
            
            time.sleep(10)
        
def main():
    # Comprobar que se han pasado los argumentos correctos
    if len(sys.argv) != 4:
        print("Usage: python EC_Central.py Port Bootstrap_IP Bootstrap_Port")
        sys.exit(1)
    
    
    # Leer las localizaciones y taxis disponibles
    leerLocalizaciones(LOCALIZACIONES)
    reconexion()

    # Iniciar el servidor de autenticación en un hilo
    auth_thread = threading.Thread(target=authenticate_taxi)
    auth_thread.start()

    weather_thread = threading.Thread(target=weatherState)
    weather_thread.start()

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

    auth_thread.join()
    map_thread.join()
    clients_thread.join()
    services_thread.join()
    taxiUpdate_thread.join()
    receiveCommand_thread.join()
    taxi_disconection_thread.join()
    customerDisconnection_thread.join()
    customerState_thread.join()
    weather_thread.join()

# Iniciar el servidor de autenticación y el manejo de Kafka en paralelo
if __name__ == "__main__":
    main()
