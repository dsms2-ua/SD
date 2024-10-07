from confluent_kafka import Producer

# Configurar Kafka Producer para enviar solicitudes a la central
producer_conf = {'bootstrap.servers': "localhost:9092"}
producer = Producer(**producer_conf)

# Enviar una solicitud de servicio de taxi
cliente_id = "cliente_1"
destino_x, destino_y = 5, 7
solicitud = f"SOLICITUD,{destino_x},{destino_y}"
producer.produce('service_requests', key=cliente_id, value=solicitud)
producer.flush()

print(f"Solicitud enviada: {solicitud}")
