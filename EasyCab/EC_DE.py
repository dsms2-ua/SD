import sys

if sys.version_info >= (3, 12, 0):
    import six
    sys.modules['kafka.vendor.six.moves'] = six.moves

import threading
import time
from kafka import KafkaProducer, KafkaConsumer
import socket
from Clases import *

'''
#Configuramos Kafka consumer para recibir el mapa de la Central



# Configurar Kafka Producer para enviar el estado del taxi
producer_conf = {
    'bootstrap.servers': "localhost:9092"
}
producer = Producer(**producer_conf)

# Configurar Kafka Consumer para recibir actualizaciones de sensores
consumer_conf = {
    'bootstrap.servers': "localhost:9092",
    'group.id': "taxi_group",
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(**consumer_conf)
consumer.subscribe(['sensor_updates'])  # Suscribirse al topic de actualizaciones de sensores

# Estado del taxi
taxi_id = "taxi_1"
estado_taxi = "ROJO"  # Inicialmente libre

def enviar_estado():
    """
    Función que envía el estado del taxi a la central cada segundo.
    """
    while True:
        # Enviar el estado actual del taxi
        producer.produce('taxi_updates', key=taxi_id, value=f'{taxi_id},{estado_taxi}')
        producer.flush()  # Asegúrate de enviar el mensaje
        print(f"Estado enviado: {taxi_id} - {estado_taxi}")
        time.sleep(1)

# Bucle principal para recibir actualizaciones de sensores y enviar el estado del taxi
def run_ec_de():
    # Iniciar el hilo para enviar el estado
    enviar_estado_thread = threading.Thread(target=enviar_estado)
    enviar_estado_thread.start()

    # Bucle para escuchar las actualizaciones de los sensores
    while True:
        msg = consumer.poll(1.0)  # Esperar 1 segundo por nuevos mensajes
        if msg is None:
            continue
        if msg.error():
            print(f"Error en Consumer: {msg.error()}")
            continue

        # Procesar actualización de los sensores
        sensor_update = msg.value().decode('utf-8')
        sensor_taxi_id, sensor_status = sensor_update.split(',')
        print(f"Actualización de sensores recibida: Taxi {sensor_taxi_id} - Estado: {sensor_status}")

        # Actualizar el estado del taxi según el sensor
        if sensor_status == "KO":
            print(f"Taxi {sensor_taxi_id} se detiene debido a una contingencia detectada.")
            estado_taxi = "ROJO"  # Cambiar estado a libre, espera a la instrucción
        elif sensor_status == "OK":
            print(f"Taxi {sensor_taxi_id} continúa en movimiento.")
            estado_taxi = "VERDE"  # Cambiar estado a ocupado
'''

def authenticateTaxi():
    #Recogemos los datos de los argumentos
    central_ip = f'{sys.argv[1]}'
    central_port = int(sys.argv[2])

    #Nos conectamos a la central por sockets
    client_socket = socket.socket()
    client_socket.connect(('localhost', 8010))

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
        mapa = message.value.decode('utf-8')
        print(mapa)


def main():
    #Comprobamos que los argumetos sean correctos
    if len(sys.argv) != 6:
        print("Error: Usage python EC_DE.py Central_IP Central_Port Bootstrap_IP Bootstrap_Port Taxi_ID")

    #Autenticamos con sockets la existencia del taxi
    if not authenticateTaxi():
        return
    
    #Creamos el hilo que lleva al consumidor Kafka del mapa
    map_thread = threading.Thread(target=receiveMap)


# Ejecución principal
if __name__ == "__main__":
    main()
