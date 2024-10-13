import sys

if sys.version_info >= (3, 12, 0):
    import six
    sys.modules['kafka.vendor.six.moves'] = six.moves

import threading
import time
from kafka import KafkaProducer, KafkaConsumer
import pickle
import socket
import os
from Clases import *


def authenticateTaxi():
    #Recogemos los datos de los argumentos
    central_ip = f'{sys.argv[1]}'
    central_port = int(sys.argv[2])

    #Nos conectamos a la central por sockets
    client_socket = socket.socket()
    client_socket.connect(('localhost', central_port))

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
    
def receiveMap():
    #Creamos el consumer de Kafka
    consumer = KafkaConsumer('map', bootstrap_servers = f'{sys.argv[3]}:{sys.argv[4]}')

    #Recibimos el mapa
    for message in consumer:
        mapa = pickle.loads(message.value)
        #os.system('cls')
        print(mapa.cadenaMapa())

def handleAlerts(client_socket, producer, id):
    global ID
    while True:
        data = client_socket.recv(1024).decode('utf-8')
        if data == "KO":
            #Mandamos una alerta a la central para indicar que el taxi tiene que pararse
            producer.send('taxiUpdate', value = f"{ID} KO".encode('utf-8'))
        else:
            producer.send('taxiUpdate', value = f"{ID} OK".encode('utf-8'))

def sendAlerts(id):
    #Creamos el socket de conexión con los sensores
    server_socket = socket.socket()
    server_socket.bind(('localhost', 5000))
    server_socket.listen(5)

    #Creamos el productor de Kafka para mandar las alertas
    producer = KafkaProducer(bootstrap_servers = f'{sys.argv[3]}:{sys.argv[4]}')

    while True:
        client, addr = server_socket.accept()

        client_handler = threading.Thread(target=handleAlerts, args=(client, producer, id))
        client_handler.start()
            
        time.sleep(1)

def receiveServices(id):
    #Creamos el consumer de Kafka
    consumer = KafkaConsumer('service_assigned_taxi', bootstrap_servers = f'{sys.argv[3]}:{sys.argv[4]}')

    #Creamos el producer de Kafka para mandar los movimientos
    producer = KafkaProducer(bootstrap_servers = f'{sys.argv[3]}:{sys.argv[4]}')

    #Recibimos los servicios
    for message in consumer:
        #Sólo podemos procesar los mensajes dirigidos a nosotros
        servicio = pickle.loads(message.value)

        if int(servicio.getTaxi()) == id:            
            origen = servicio.getOrigen()
            destino = servicio.getPosDestino()
            posCliente = servicio.getPosCliente()

            Pos = origen

            recogido = False
            while not recogido:
                Pos = moverTaxi(Pos, posCliente)
                producer.send('taxiMovements', value=f"{id} {Pos.getX()} {Pos.getY()}".encode('utf-8'))
                if Pos.getX() == posCliente.getX() and Pos.getY() == posCliente.getY():
                    recogido = True
                time.sleep(1)

            llegada = False
            while not llegada:
                Pos = moverTaxi(Pos, destino)
                producer.send('taxiMovements', value=f"{id} {Pos.getX()} {Pos.getY()}".encode('utf-8'))
                if Pos.getX() == destino.getX() and Pos.getY() == destino.getY():
                    llegada = True
                time.sleep(1)

def main():
    #Comprobamos que los argumetos sean correctos
    if len(sys.argv) != 6:
        print("Error: Usage python EC_DE.py Central_IP Central_Port Bootstrap_IP Bootstrap_Port Taxi_ID")

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

    map_thread.join()
    alert_thread.join()


# Ejecución principal
if __name__ == "__main__":
    main()
