import sys

if sys.version_info >= (3, 12, 0):
    import six
    sys.modules['kafka.vendor.six.moves'] = six.moves

import socket
import threading
import time
import pickle
import os
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


#Función para enviar el mapa y para mostrar la tabla
def sendMap():
    #Creamos el productor de Kafka
    producer = KafkaProducer(bootstrap_servers=f'{sys.argv[2]}:{sys.argv[3]}')
    #Añadimos el mapa

    mapa = Mapa(LOCALIZACIONES, TAXIS, CLIENTES)
    while True:
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
    #Recibir los clientes
    for message in consumer:
        #Cogemos el mensaje y creamos el objeto servicio
        id, destino = message.value.decode('utf-8').split()
        servicio = Servicio(id, destino)

        #Iteramos sobre la lista de clientes activos para obtener la posición del cliente
        for cliente in CLIENTES:
            if cliente.getId() == servicio.getCliente():
                servicio.setPosCliente(cliente.getPosicion())
                cliente.setDestino(destino)
                for loc in LOCALIZACIONES:
                    if loc == destino:
                        servicio.setPosDestino(LOCALIZACIONES[loc])

        print(f"Cliente {servicio.getCliente()} solicita un taxi")

        asignado = False
        producer = KafkaProducer(bootstrap_servers=f'{sys.argv[2]}:{sys.argv[3]}')
        #Buscamos un taxi libre
        for taxi in TAXIS:
            if not taxi.getOcupado():
                servicio.setOrigen(taxi.getCasilla())
                servicio.setTaxi(taxi.getId())

                asignado = True
                taxi.setOcupado(True)
                taxi.setCliente(servicio.getCliente())
                taxi.setOrigen(taxi.getCasilla()) #Desde donde partimos
                taxi.setPosCliente(servicio.getPosCliente()) #Desde donde parte el cliente
                taxi.setDestino(servicio.getDestino()) #A donde va el cliente

                for loc in LOCALIZACIONES:
                    if loc == destino:
                        taxi.setPosDestino(LOCALIZACIONES[loc])

                print(f"Taxi {taxi.getId()} asignado al cliente {servicio.getCliente()}")
                producer.send('service_assigned_client', value = f"{servicio.getCliente()} OK {taxi.getId()} es el taxi asignado.".encode('utf-8'))

                #Mandamos el objeto servicio
                producer.send('service_assigned_taxi', pickle.dumps(servicio))

        #Si no hay taxis disponibles, informamos al cliente
        if not asignado:
            producer.send('service_assigned', value = f"{servicio.getCliente()} KO".encode('utf-8'))

def readTaxiUpdate():
    #Crear un consumidor de Kafka
    consumer = KafkaConsumer('taxiUpdate', bootstrap_servers=f'{sys.argv[2]}:{sys.argv[3]}')
    #Recibir los clientes
    while True:
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

                if taxi.getDestino() == taxi.getCasilla():
                    #El cliente ya ha llegado y tenemos que actualizar todos los datos
                    for cliente in CLIENTES:
                        if cliente.getId() == taxi.getCliente():
                            cliente.setDestino(None)
                            break

                    taxi.setOcupado(False)
                    taxi.setRecogido(False)
                    taxi.setCliente(None)

                break

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
    
    auth_thread.join()
    map_thread.join()
    clients_thread.join()
    services_thread.join()
    taxiUpdate_thread.join()
    taxiMovement_thread.join()

# Iniciar el servidor de autenticación y el manejo de Kafka en paralelo
if __name__ == "__main__":
    main()
