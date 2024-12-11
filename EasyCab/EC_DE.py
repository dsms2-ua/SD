import sys
import requests
if sys.version_info >= (3, 12, 0):
    import six
    sys.modules['kafka.vendor.six.moves'] = six.moves

import threading
import time
from kafka import KafkaProducer, KafkaConsumer
import pickle
import socket, ssl
import os, hashlib
from Clases import *
from EC_CTC import get_temperature
import pwinput

sensorOut = False
centralStop = False
operativo = True
operativo2 = True
estado="Esperando asignación"
sensores = {} #Aquí guardamos los sensores y su estado
centralOperativa = True
centralTimeout = 0
lock_operativo = threading.Lock()  # Creamos el Lock
posicion = Casilla(1,1)
city=""
token=""
#AES key in bytes
AES_KEY = b''

init(autoreset=True)

#Creamos el servidor seguro de sockets, le enviamos el ID
#Si estamos registrados, nos pedirá la contraseña y si es correcta, accedemos
def authenticateTaxi():
    global AES_KEY, token, posicion
    # Recogemos los datos de los argumentos
    central_ip = f'{sys.argv[1]}'
    central_port = int(sys.argv[2])
    taxi_id = int(sys.argv[5])
    
    context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
    context.load_verify_locations('certificados/certSocketsSans.pem')
    #context = ssl._create_unverified_context()
    
    with socket.create_connection((central_ip, central_port)) as sock:
        with context.wrap_socket(sock, server_hostname=central_ip) as ssock:
            try:
                # Enviamos el ID
                ssock.send(str(taxi_id).encode())
                # Recibimos la respuesta
                response = ssock.recv(1024)
                
                # Si recibimos KO, es que no estamos registrados
                if response.decode() == "KO":
                    print("El taxi no está registrado en el sistema")
                    return False
                elif response.decode() == "Logged":
                    print("El id solicitado ya tiene una sesión activa")
                    return False
                else:
                    # Tenemos que introducir la contraseña, hacerle un HASH y enviarla
                    password = pwinput.pwinput(prompt="Introduce tu contraseña: ", mask="*")
                    hashed = hashlib.md5(password.encode()).hexdigest()
                    ssock.send(hashed.encode())
                    
                # Recibimos si la contraseña es correcta o no
                response = ssock.recv(1024)
                if response.decode() == "KO":
                    print("Contraseña incorrecta")
                    return False
                else:
                    print("Contraseña correcta")
                    print(response.decode())
                    try:
                        tokenAesPos = response.decode()
                        token, aes, pos = tokenAesPos.split()
                        posicion = Casilla(int(pos.split(",")[0]), int(pos.split(",")[1]))
                        AES_KEY = base64.b64decode(aes)
                        return True
                    except ValueError:
                        print("Error al procesar los datos")
                        return False
            finally:
                ssock.close()
                

#Falta encriptar todas las comunicaciones con Registry
def register(id):
    #Pedimos al usuario que introduzca los datos
    password = pwinput.pwinput(prompt="Introduce tu contraseña: ", mask="*")
    password2 = pwinput.pwinput(prompt="Repite tu contraseña: ", mask="*")
    
    #Comprobamos que las contraseñas sean iguales
    while password != password2:
        print("Las contraseñas no coinciden")
        password = pwinput.pwinput("Introduce tu contraseña: ", mask="*")
        password2 = pwinput.pwinput(prompt="Repite tu contraseña: ", mask="*")
        
    #Le hacemos un HASH a la contraseña con md5
    hashed = hashlib.md5(password.encode()).hexdigest()
        
    data = {
        "id": id,
        "password": hashed
    }
    #Hacemos la petición POST a la API
    response = requests.post('https://localhost:3003/registry', json=data, verify='certificados/certRegistrySans.pem')
    
    #Si el intento de registro ha sido correcto, vamos directamente al login
    
    if response.status_code == 200:
        print("Taxi registrado correctamente")
        #Como el registro ha sido correcto, vamos al login
        return authenticateTaxi()
    elif response.status_code == 404:
        print("El taxi ya está registrado")
    else:
        print("Error en el registro del taxi")
    
    return False
    
def borrarCuenta(id):
    #Hacemos una petición DELETE a la API
    data = {
        "id": id
    }
    
    response = requests.delete(f'https://localhost:3003/delete', json = data, verify='certificados/certRegistrySans.pem')
    
    if response.status_code == 200:
        print("Cuenta borrada correctamente")
    elif response.status_code == 404:
        print("El taxi no está registrado")
    else:
        print("Error al borrar la cuenta")
    
    return False
        
def showMenu(id):
    #La primera opción nos llevará al registry, la segunda a la central y la tercera cerrará el programa
    print("Bienvenido al sistema de EasyCab")
    print("1. Registrarse")
    print("2. Acceder")
    print("3. Borrar mi cuenta")
    print("3. Salir")
    
    opcion = input("Introduce la opción deseada: ")
    if opcion == "1":
        return register(id)   
    elif opcion == "2":
        return authenticateTaxi()
    elif opcion == "3":
        return borrarCuenta(id)
    elif opcion == "4":
        return False

def sendHeartbeat():
    global estado,operativo,centralStop,posicion,sensores,AES_KEY,token
    while True:
        aux = False
        if len(sensores) == 0:
            operativo = False
        else:
            for sensor in sensores:
                aux = True
                if sensores[sensor] == "KO" or sensores[sensor] == "Desconectado":
                    aux = False
                    operativo = False
                    break
        if aux and not centralStop:
            operativo = True
            estadoTaxi = "OK"
        else:
            operativo = False
        
        producer = KafkaProducer(bootstrap_servers=f'{sys.argv[3]}:{sys.argv[4]}')
        mensaje = f"{operativo} {posicion.getX()} {posicion.getY()}"
        coded_message = encrypt(mensaje, AES_KEY, True)
        producer.send('taxiUpdate', value=f"{token} {coded_message}".encode('utf-8'))
        time.sleep(0.5)
    
def sensoresStates():
    global sensores,estado
    global operativo
    while True:
        for sensor in sensores:
            if operativo and sensores[sensor] == "KO":
                operativo = False
                estado = "Taxi parado por sensores"
            
    
def receiveMap():
    global estado, centralTimeout
    #Creamos el consumer de Kafka
    consumer = KafkaConsumer('map', bootstrap_servers = f'{sys.argv[3]}:{sys.argv[4]}')

    while True:
        message = consumer.poll(timeout_ms=1000)
        if message:
            centralTimeout = 0
            for tp, messages in message.items():
                for message in messages:
                    mapa = pickle.loads(message.value)
                    #os.system('cls')
                    #Vamos a imprimir el estado de todos los sensores también
                    print("Sensores         |         Estado")
                    for sensor in sensores:
                        print(f"   {sensor}             |           ", end="")
                        if sensores[sensor] == "KO":
                            print(f"{Fore.RED}KO{Style.RESET_ALL}")
                        elif sensores[sensor] == "Desconectado":
                            print(f"{Fore.RED}Desconectado{Style.RESET_ALL}")
                        else:
                            print(f"{Fore.GREEN}OK{Style.RESET_ALL}")
                    cadena = f"\n{Back.WHITE}{Fore.BLACK}{estado}{Style.RESET_ALL}"
                    print(mapa.cadenaMapaTaxi(str(sys.argv[5])) + cadena)
                    
        else:
            if centralTimeout > 5:
                os.system('cls')
                imprimirErrorCentral()

def handleAlerts(client_socket, producer, id):
    global operativo, estado

    client_socket.settimeout(1.0)

    while True:
        sensor_id = None
        try:
            data = client_socket.recv(1024)
            decoded_data = verify_message(data)
            if decoded_data:
                fields = decoded_data.split("#")
                
                # Mensaje de conexión inicial (un campo, ej.: SENSOR)
                if len(fields) == 1:
                    aux = False
                    for sensor in sensores:
                        if sensores[sensor] == "Desconectado":
                            sensores[sensor] = "OK"
                            aux = True
                            client_socket.send(create_message(f"{sensor}"))
                            break
                    if not aux:
                        sensor_id = len(sensores) + 1
                        sensores[sensor_id] = "OK"
                        client_socket.send(create_message(f"{sensor_id}"))
    
                # Mensaje de estado (dos campos, ej.: id#status)
                elif len(fields) == 2:
                    sensor_id = int(fields[0])
                    est = fields[1]
                    if est == "KO":
                        sensores[sensor_id] = "KO"
                        operativo = False
                        estado = "Parado por sensores"
                    elif est == "OK":
                        sensores[sensor_id] = "OK"
                        operativo = True
                    elif est == "STOP":
                        sensores.pop(sensor_id)
                    client_socket.send(ACK if decoded_data else NACK)           
        except socket.timeout:
            if sensor_id is not None:
                sensores[sensor_id] = "Desconectado"
            operativo = False
            estado = "Parado por sensores"
            break
        except Exception as e:
            sensores[sensor_id] = "Desconectado"
            operativo = False
            estado = "Parado por sensores"
            break
        time.sleep(1)

def sendAlerts(id):
    #Creamos el socket de conexión con los sensores
    server_socket = socket.socket()
    server_socket.bind(('localhost', 4999 + id)) #Para que empiece en 5000
    server_socket.listen(5)

    #Creamos el productor de Kafka para mandar las alertas
    producer = KafkaProducer(bootstrap_servers = f'{sys.argv[3]}:{sys.argv[4]}')

    while True:
        client, addr = server_socket.accept()
        
        client_handler = threading.Thread(target=handleAlerts, args=(client, producer, id))
        client_handler.start()

def irA(destino):
    global operativo, operativo2, estado, posicion
    # Implement the logic to move the taxi to the destination
    #inicializamos pos a la posición del taxi
    estado = f"Yendo a {destino.getX()},{destino.getY()}"
    while posicion.getX() != destino.getX() or posicion.getY() != destino.getY():
        if operativo:
            posicion = moverTaxi(posicion, destino)
            time.sleep(1)
        else: 
            estado = "Taxi detenido"    
            break    
    estado = "Esperando asignación"
    operativo2 = True

def process_commands():
    global operativo, operativo2, centralStop, estado, token
    # Configurar el consumidor de Kafka
    consumer = KafkaConsumer('taxi_orders', bootstrap_servers=f'{sys.argv[3]}:{sys.argv[4]}')
    # Recibir los mensajes
    for message in consumer:
        print("Mensaje recibido")
        
        tk,mensaje = message.value.decode('utf-8').split()
        if tk == token:
            mensaje = decrypt(mensaje, AES_KEY, True)
            if mensaje == "KO":
                centralStop = True
                operativo = False
            elif mensaje == "OK":
                aux = False
                for sensor in sensores:
                    if sensores[sensor] == "KO" or sensores[sensor] == "Desconectado":
                        aux = True
                        break
                if not aux:
                    centralStop = False
                    operativo = True
            #recibimos en data[1] el destino al que tenemos que ir
            else:
                #creamos una casilla con las coordenadas del destino
                operativo2 = False
                destino = Casilla(int(mensaje.split(",")[0]), int(mensaje.split(",")[1]))
                irA(destino)
                if mensaje == "1,1":
                    estado = "Taxi ha llegado a la central. Cerrando programa"
                    time.sleep(5)                    
                    os._exit(0)
                    

def receiveServices(id):
    #Creamos el consumer de Kafka
    consumer = KafkaConsumer('service_assigned_taxi', bootstrap_servers = f'{sys.argv[3]}:{sys.argv[4]}')

    global estado, operativo,posicion,token,operativo2
    #Creamos el producer de Kafka para mandar los movimientos
    producer = KafkaProducer(bootstrap_servers = f'{sys.argv[3]}:{sys.argv[4]}')

    #Recibimos los servicios
    for message in consumer:
        #Sólo podemos procesar los mensajes dirigidos a nosotros

        tk, mensaje = message.value.decode('utf-8').split()
        if tk == token:  
            print("Mensaje recibido")          
            servicio = decrypt(mensaje, AES_KEY,False)
            #creamos producer de Kafka para notificar al cliente de la asignacion con el topic taxi_assigned
            producer.send('taxi_assigned', value = f"{servicio.getTaxi()} {servicio.getCliente()} {servicio.getDestino()}".encode('utf-8'))
            
            origen = servicio.getOrigen()
            destino = servicio.getPosDestino()
            posCliente = servicio.getPosCliente()

            Pos = origen
            estado = f"Yendo a recoger al cliente {servicio.getCliente()}"
            recogido = False
            while not recogido:
                if operativo and operativo2:
                    Pos = moverTaxi(Pos, posCliente)
                    posicion = Pos
                    if Pos.getX() == posCliente.getX() and Pos.getY() == posCliente.getY():
                        recogido = True
                    time.sleep(1)
                else:
                    estado = "Taxi detenido. Servicio cancelado"
                    producer.send('service_completed', value = f"{servicio.getCliente()} KO".encode('utf-8'))
                    #pasamos a la siguiente iteración del bucle for
                    break
            if not operativo or not operativo2:
                continue
            estado = f"Cliente {servicio.getCliente()} recogido, yendo a {servicio.getDestino()}"

            llegada = False
            while not llegada:
                if operativo and operativo2:
                    Pos = moverTaxi(Pos, destino)
                    posicion = Pos
                    if Pos.getX() == destino.getX() and Pos.getY() == destino.getY():
                        llegada = True
                    time.sleep(1)
                else:
                    estado = "Taxi detenido. Servicio cancelado"
                    producer.send('service_completed', value = f"{servicio.getCliente()} KO".encode('utf-8'))
                    #pasamos a la siguiente iteración del bucle for
                    break
            if operativo and operativo2:
                estado = f"Servicio completado. Esperando asignación"
            
def centralState():
    global centralTimeout
    while True:
        time.sleep(1)
        centralTimeout += 1


def main():
    #Comprobamos que los argumetos sean correctos
    if len(sys.argv) != 6:
        print("Error: Usage python EC_DE.py Central_IP Central_Port Bootstrap_IP Bootstrap_Port Taxi_ID")
        return -1

    ID = int(sys.argv[5])
    
    #Primero mostramos el menú con la opción de registrarnos o de directamente acceder
    if not showMenu(ID):
        print("Saliendo del programa")
    else:

        #Creamos el hilo que comunica las alertas al consumidor
        alert_thread = threading.Thread(target=sendAlerts, args = (ID, ))
        alert_thread.start()
        """
        #preguntamos si quiere conectar sensores al taxi
        print("¿Desea conectar sensores al taxi? ([S]/N)")
        respuesta = input()
        #ponemos la respuesta en minúsculas
        respuesta = respuesta.lower()
        #valor por defecto S
        if respuesta != "n": 
            respuesta = "s"
        hostname = socket.gethostname()
        local_ip = socket.gethostbyname(hostname)

        if respuesta == "s":
            #abrir una terminal con EC_S.py
            os.system(f"start cmd /k python EC_S.py {local_ip} {4999 + ID}")
        """
        #Creamos el hilo que lleva al consumidor Kafka del mapa
        map_thread = threading.Thread(target=receiveMap)
        map_thread.start()

        #Creamos el hilo que lleva al consumidor Kafka de los servicios asignados
        services_thread = threading.Thread(target=receiveServices, args=(ID, ))
        services_thread.start()

        #creamos el hilo que recibirá los comandos de la central
        command_thread = threading.Thread(target=process_commands)
        command_thread.start()

        heartbeat_thread = threading.Thread(target=sendHeartbeat)
        heartbeat_thread.start()
        
        centralState_thread = threading.Thread(target=centralState)
        centralState_thread.start()

        map_thread.join()
        #alert_thread.join()
        services_thread.join()
        command_thread.join()
        heartbeat_thread.join()
        centralState_thread.join()


# Ejecución principal
if __name__ == "__main__":
    main()
