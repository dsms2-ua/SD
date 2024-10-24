import sys

if sys.version_info >= (3, 12, 0):
    import six
    sys.modules['kafka.vendor.six.moves'] = six.moves

import threading
import time
from kafka import KafkaProducer, KafkaConsumer
import pickle
import socket
import keyboard
import os
from Clases import *

stop_threads = False

operativo = True
estado="Esperando asignación"
sensores = {} #Aquí guardamos los sensores y su estado
centralOperativa = True
lock_operativo = threading.Lock()  # Creamos el Lock


init(autoreset=True)

def sendHeartbeat():
    while not stop_threads:
        aux = False
        if len(sensores) == 0:
            operativo = False
        else:
            for sensor in sensores:
                aux = True
                if not sensores[sensor]:
                    aux = False
                    operativo = False
                    break
        if aux:
            operativo = True
            estadoTaxi = "OK"
        else:
            estadoTaxi = "KO"
        producer = KafkaProducer(bootstrap_servers=f'{sys.argv[3]}:{sys.argv[4]}')
        producer.send('taxiUpdate', value=f"{sys.argv[5]} {estadoTaxi}".encode('utf-8'))
        time.sleep(1)

def authenticateTaxi():
    #Recogemos los datos de los argumentos
    central_ip = f'{sys.argv[1]}'
    central_port = int(sys.argv[2])

    #Nos conectamos a la central por sockets
    client_socket = socket.socket()
    client_socket.connect((central_ip, central_port))

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
    
def sensoresStates():
    global sensores
    global operativo
    while not stop_threads:
        for sensor in sensores:
            if operativo and not sensores[sensor]:
                operativo = False
                estado = "Taxi parado por sensores"
            
    
def receiveMap():
    global estado
    #Creamos el consumer de Kafka
    consumer = KafkaConsumer('map', bootstrap_servers = f'{sys.argv[3]}:{sys.argv[4]}')

    #Recibimos el mapa
    for message in consumer:
        mapa = pickle.loads(message.value)
        os.system('cls')
        #Vamos a imprimir el estado de todos los sensores también
        print("Sensores         |         Estado")
        for sensor in sensores:
            print(f"   {sensor}             |           ", end="")
            if sensores[sensor] == False:
                print(f"{Fore.RED}KO{Style.RESET_ALL}")
            else:
                print(f"{Fore.GREEN}OK{Style.RESET_ALL}")
        cadena = f"\n{Back.WHITE}{Fore.BLACK}{estado}{Style.RESET_ALL}"
        print(mapa.cadenaMapaTaxi(str(sys.argv[5])) + cadena)



def handleAlerts(client_socket, producer, id):
    global operativo
    global estado

    client_socket.settimeout(1.0)

    while not stop_threads:
        data = client_socket.recv(1024).decode('utf-8')
        #Si recibimos el mensaje con solo una palabra, es de conexion
        if len(data.split()) == 1:
            sensor = len(sensores) + 1
            est = data.split()[0]
            sensores[sensor] = est
            
            client_socket.send(f"{sensor}".encode('utf-8'))
        
        #Si recibimos el mensaje con dos palabras, es de estado  
        if len(data.split()) == 2:
            sensor = int(data.split()[0])
            est = data.split()[1]        
            if est == "KO":
                sensores[sensor] = False
                if operativo:
                    operativo = False
            elif est == "OK":
                sensores[sensor] = True
                if not operativo:
                    operativo = True                   
            elif est == "STOP":
                #Borramos ese sensor del diccionario
                sensores.pop(sensor)
        time.sleep(1)

def sendAlerts(id):
    #Creamos el socket de conexión con los sensores
    server_socket = socket.socket()
    server_socket.bind(('localhost', 4999 + id)) #Para que empiece en 5000
    server_socket.listen(5)

    #Creamos el productor de Kafka para mandar las alertas
    producer = KafkaProducer(bootstrap_servers = f'{sys.argv[3]}:{sys.argv[4]}')

    while not stop_threads:
        client, addr = server_socket.accept()
        
        client_handler = threading.Thread(target=handleAlerts, args=(client, producer, id))
        client_handler.start()

def irA(destino,inicial):
    global operativo
    # Implement the logic to move the taxi to the destination
    producer = KafkaProducer(bootstrap_servers=f'{sys.argv[3]}:{sys.argv[4]}')
    #inicializamos pos a la posición del taxi
    pos = inicial
    estado = f"Yendo a {destino.getX()},{destino.getY()}"
    while pos.getX() != destino.getX() or pos.getY() != destino.getY():
        if operativo:
            pos = moverTaxi(pos, destino)
            producer.send('taxiMovements', value=f"{sys.argv[5]} {pos.getX()} {pos.getY()}".encode('utf-8'))
            time.sleep(1)
        else: 
            estado = "Taxi detenido"    
            break    
    estado = "Esperando asignación"



def process_commands():
    global operativo
    # Configurar el consumidor de Kafka
    consumer = KafkaConsumer('taxi_orders', bootstrap_servers=f'{sys.argv[3]}:{sys.argv[4]}')
    # Recibir los mensajes
    for message in consumer:
        data = message.value.decode('utf-8').split()
        if data[0] == str(sys.argv[5]):
            if data[1] == "KO":
                operativo = False
            elif data[1] == "OK":
                operativo = True
            #recibimos en data[1] el destino al que tenemos que ir
            else:
                #creamos una casilla con las coordenadas del destino
                destino = Casilla(int(data[1].split(",")[0]), int(data[1].split(",")[1]))
                posicion = Casilla(int(data[2]),int(data[3]))
                irA(destino, posicion)

def receiveServices(id):
    #Creamos el consumer de Kafka
    consumer = KafkaConsumer('service_assigned_taxi', bootstrap_servers = f'{sys.argv[3]}:{sys.argv[4]}')

    global estado
    global operativo
    #Creamos el producer de Kafka para mandar los movimientos
    producer = KafkaProducer(bootstrap_servers = f'{sys.argv[3]}:{sys.argv[4]}')

    #Recibimos los servicios
    for message in consumer:
        #Sólo podemos procesar los mensajes dirigidos a nosotros
        servicio = pickle.loads(message.value)

        if int(servicio.getTaxi()) == id:            

            #creamos producer de Kafka para notificar al cliente de la asignacion con el topic taxi_assigned
            producer.send('taxi_assigned', value = f"{id} {servicio.getCliente()} {servicio.getDestino()}".encode('utf-8'))
            
            origen = servicio.getOrigen()
            destino = servicio.getPosDestino()
            posCliente = servicio.getPosCliente()

            Pos = origen
            estado = f"Yendo a recoger al cliente {servicio.getCliente()}"
            recogido = False
            while not recogido:
                if operativo:
                    Pos = moverTaxi(Pos, posCliente)
                    producer.send('taxiMovements', value=f"{id} {Pos.getX()} {Pos.getY()}".encode('utf-8'))
                    if Pos.getX() == posCliente.getX() and Pos.getY() == posCliente.getY():
                        recogido = True
                    time.sleep(1)
                else:
                    estado = "Taxi detenido. Servicio cancelado"
                    producer.send('service_completed', value = f"{servicio.getCliente()} KO".encode('utf-8'))
                    #pasamos a la siguiente iteración del bucle for
                    break
            if not operativo:
                continue
            estado = f"Cliente {servicio.getCliente()} recogido, yendo a {servicio.getDestino()}"
            producer.send('picked_up', value = f"{id} {servicio.getCliente()} {servicio.getDestino()}".encode('utf-8'))

            llegada = False
            while not llegada:
                if operativo:
                    Pos = moverTaxi(Pos, destino)
                    producer.send('taxiMovements', value=f"{id} {Pos.getX()} {Pos.getY()}".encode('utf-8'))
                    if Pos.getX() == destino.getX() and Pos.getY() == destino.getY():
                        llegada = True
                    time.sleep(1)
                else:
                    estado = "Taxi detenido. Servicio cancelado"
                    producer.send('service_completed', value = f"{servicio.getCliente()} KO".encode('utf-8'))
                    #pasamos a la siguiente iteración del bucle for
                    break
            if operativo:
                estado = f"Servicio completado. Esperando asignación"
                producer.send('arrived', value = f"{id} {servicio.getCliente()} {servicio.getDestino()}".encode('utf-8'))

#Creamos la función que nos va controlar si se cierra la conexión del Digital Engine
def stopTaxi():
    global stop_threads
    producer = KafkaProducer(bootstrap_servers = f'{sys.argv[3]}:{sys.argv[4]}')
    while not stop_threads:
        time.sleep(0.1)
        if keyboard.is_pressed('Ctrl + C'):
            stop_threads = True
            #Comunicamos a la central y al customer que paramos el taxi
            producer.send('taxiStop', value = f"{sys.argv[5]} STOP".encode('utf-8'))


def main():
    #Comprobamos que los argumetos sean correctos
    if len(sys.argv) != 6:
        print("Error: Usage python EC_DE.py Central_IP Central_Port Bootstrap_IP Bootstrap_Port Taxi_ID")
        return -1

    #Autenticamos con sockets la existencia del taxi
    if not authenticateTaxi():
        return
    ID = int(sys.argv[5])
    
    #Creamos el hilo que lleva al consumidor Kafka del mapa
    map_thread = threading.Thread(target=receiveMap)
    map_thread.start()

    #Creamos el hilo que comunica las alertas al consumidor
    alert_thread = threading.Thread(target=sendAlerts, args = (ID, ))
    alert_thread.start()

    #Creamos el hilo que lleva al consumidor Kafka de los servicios asignados
    services_thread = threading.Thread(target=receiveServices, args=(ID, ))
    services_thread.start()
    
    #Creamos el hilo que nos sirve para identificar que paramos el servicio
    stop_thread = threading.Thread(target=stopTaxi)
    stop_thread.start()

    #creamos el hilo que recibirá los comandos de la central
    command_thread = threading.Thread(target=process_commands)
    command_thread.start()

    heartbeat_thread = threading.Thread(target=sendHeartbeat)
    heartbeat_thread.start()

    map_thread.join()
    #alert_thread.join()
    stop_thread.join()
    services_thread.join()
    command_thread.join()
    heartbeat_thread.join()


# Ejecución principal
if __name__ == "__main__":
    main()
