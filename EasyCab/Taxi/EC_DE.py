from confluent_kafka import Consumer

# Configurar Kafka Consumer para recibir actualizaciones de sensores
consumer_conf = {
    'bootstrap.servers': "localhost:9092",
    'group.id': "taxi_group",
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(**consumer_conf)
consumer.subscribe(['sensor_updates'])

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
    taxi_id, sensor_status = sensor_update.split(',')
    print(f"Actualización de sensores recibida: Taxi {taxi_id} - Estado: {sensor_status}")

    # Si el estado es KO, el taxi se detiene
    if sensor_status == "KO":
        print(f"Taxi {taxi_id} se detiene debido a una contingencia detectada.")
        # Aquí podrías generar un mensaje a la central para reportar la situación, etc.
    elif sensor_status == "OK":
        print(f"Taxi {taxi_id} continúa en movimiento.")
