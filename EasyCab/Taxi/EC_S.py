import time
import random
from confluent_kafka import Producer

# Configurar Kafka Producer para enviar actualizaciones de sensores
producer_conf = {'bootstrap.servers': "localhost:9092"}
producer = Producer(**producer_conf)

# ID del taxi al que están vinculados los sensores
taxi_id = "taxi_1"

def enviar_actualizacion_sensor(status):
    """
    Función para enviar el estado de los sensores (OK o KO) al EC_DE.
    """
    message = f"{taxi_id},{status}"
    producer.produce('sensor_updates', key=taxi_id, value=message)
    producer.flush()
    print(f"Estado del sensor enviado: {message}")

def simular_sensores():
    """
    Simula los sensores del taxi, enviando OK regularmente y KO en situaciones específicas.
    """
    while True:
        # Simular que el sensor está funcionando correctamente (OK) en la mayoría de los casos
        status = "OK"  # Estado OK por defecto

        # Aleatoriamente se puede generar una contingencia (KO)
        if random.random() < 0.1:  # 10% de probabilidad de un evento KO
            status = "KO"

        # Enviar la actualización de sensor al EC_DE
        enviar_actualizacion_sensor(status)

        # Esperar 1 segundo antes de la próxima actualización
        time.sleep(1)

# Iniciar la simulación de los sensores
if __name__ == "__main__":
    simular_sensores()
