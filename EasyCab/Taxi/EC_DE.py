import threading
import time
from kafka import Producer, Consumer

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

# Ejecución principal
if __name__ == "__main__":
    run_ec_de()
