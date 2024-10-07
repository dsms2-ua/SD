import socket
import threading
from confluent_kafka import Producer, Consumer

# Diccionario para almacenar los taxis autenticados
authenticated_taxis = {}

# Kafka Producer para enviar mensajes a taxis
producer_conf = {'bootstrap.servers': "localhost:9092"}
producer = Producer(**producer_conf)

# Función para manejar la autenticación de taxis a través de sockets
def handle_authentication(client_socket, addr):
    print(f"Conexión de autenticación desde {addr}")

    try:
        # Recibir el mensaje de autenticación del taxi
        auth_message = client_socket.recv(1024).decode('utf-8')
        print(f"Mensaje de autenticación recibido: {auth_message}")

        # Parsear el mensaje: Ejemplo: 'AUTH,taxi_id'
        if auth_message.startswith("AUTH"):
            _, taxi_id = auth_message.split(',')
            authenticated_taxis[taxi_id] = addr  # Registrar taxi autenticado
            print(f"Taxi {taxi_id} autenticado correctamente.")
            client_socket.send(f"Taxi {taxi_id} autenticado".encode('utf-8'))

        # Cerrar la conexión después de autenticar
        client_socket.close()
    except Exception as e:
        print(f"Error en autenticación de {addr}: {e}")
        client_socket.close()

# Configuración del servidor de autenticación de taxis (usando sockets)
def start_auth_server():
    server_ip = '0.0.0.0'  # Acepta conexiones desde cualquier IP
    server_port = 9999
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((server_ip, server_port))
    server.listen(5)  # Aceptar hasta 5 conexiones en cola
    print(f"Servidor de autenticación escuchando en {server_ip}:{server_port}")

    # Bucle para aceptar conexiones de taxis
    while True:
        client_socket, addr = server.accept()
        # Iniciar un hilo para manejar cada taxi que se conecta para autenticarse
        client_handler = threading.Thread(target=handle_authentication, args=(client_socket, addr))
        client_handler.start()

# Iniciar el servidor de autenticación
if __name__ == "__main__":
    start_auth_server()


#from confluent_kafka import Consumer, Producer

# Configurar Kafka Producer para enviar instrucciones a los taxis
producer_conf = {'bootstrap.servers': "localhost:9092"}
producer = Producer(**producer_conf)

# Configurar Kafka Consumer para recibir solicitudes de los clientes
consumer_conf = {
    'bootstrap.servers': "localhost:9092",
    'group.id': "central_group",
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(**consumer_conf)
consumer.subscribe(['service_requests'])  # Suscribirse al topic de solicitudes de clientes

# Diccionario para manejar los taxis disponibles
taxis_disponibles = {'taxi_1': 'ROJO', 'taxi_2': 'ROJO'}  # ROJO significa que están libres

# Bucle para escuchar las solicitudes de los clientes
while True:
    msg = consumer.poll(1.0)  # Esperar 1 segundo por nuevos mensajes
    if msg is None:
        continue
    if msg.error():
        print(f"Error en Consumer: {msg.error()}")
        continue

    # Procesar solicitud de cliente
    solicitud = msg.value().decode('utf-8')
    print(f"Solicitud recibida: {solicitud}")

    # Buscar un taxi disponible
    taxi_asignado = None
    for taxi_id, estado in taxis_disponibles.items():
        if estado == 'ROJO':  # Taxi libre
            taxi_asignado = taxi_id
            taxis_disponibles[taxi_id] = 'VERDE'  # Marcar como ocupado
            break

    # Enviar respuesta al cliente y asignar taxi
    if taxi_asignado:
        print(f"Asignando {taxi_asignado} a la solicitud {solicitud}")
        producer.produce('taxi_instructions', key=taxi_asignado, value=f'IR A {solicitud}')
        producer.flush()
    else:
        print("No hay taxis disponibles en este momento")


# Configurar Kafka Consumer para recibir actualizaciones de los taxis
consumer.subscribe(['taxi_updates'])

# Bucle para escuchar las actualizaciones de los taxis
while True:
    msg = consumer.poll(1.0)  # Esperar 1 segundo por nuevos mensajes
    if msg is None:
        continue
    if msg.error():
        print(f"Error en Consumer: {msg.error()}")
        continue

    # Procesar actualización del taxi
    actualizacion = msg.value().decode('utf-8')
    taxi_id, estado = actualizacion.split(',')
    print(f"Actualización recibida de {taxi_id}: {estado}")

    # Actualizar el estado del taxi
    taxis_disponibles[taxi_id] = estado
    print(f"Estado del taxi {taxi_id} actualizado a {estado}")
