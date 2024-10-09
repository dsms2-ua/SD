import socket
import time
import threading

# Función para enviar mensajes al Digital Engine (EC_DE)
def enviar_estado(client_socket, taxi_id):
    while True:
        # Por defecto, enviar "OK" cada segundo
        client_socket.send(f"TAXI_{taxi_id},OK".encode('utf-8'))
        print(f"TAXI_{taxi_id}: OK enviado")
        time.sleep(1)

# Función para detectar incidencias con el teclado
def detectar_incidencias(client_socket, taxi_id):
    while True:
        input("Presiona ENTER para simular una incidencia (KO)")
        client_socket.send(f"TAXI_{taxi_id},KO".encode('utf-8'))
        print(f"TAXI_{taxi_id}: KO enviado (Incidencia)")

# Función principal para iniciar EC_S
def iniciar_ec_s(ip_ec_de, puerto_ec_de, taxi_id):
    # Crear un socket TCP/IP
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    try:
        # Conectar con el Digital Engine (EC_DE)
        client_socket.connect((ip_ec_de, puerto_ec_de))
        print(f"Conectado a EC_DE en {ip_ec_de}:{puerto_ec_de}")

        # Iniciar hilo para enviar el estado periódicamente
        hilo_estado = threading.Thread(target=enviar_estado, args=(client_socket, taxi_id))
        hilo_estado.start()

        # Iniciar hilo para detectar y enviar incidencias
        hilo_incidencias = threading.Thread(target=detectar_incidencias, args=(client_socket, taxi_id))
        hilo_incidencias.start()

        # Esperar a que ambos hilos terminen (aunque en realidad se ejecutarán indefinidamente)
        hilo_estado.join()
        hilo_incidencias.join()

    except Exception as e:
        print(f"Error en la conexión con EC_DE: {e}")
    finally:
        client_socket.close()

# Ejecución principal
if __name__ == "__main__":
    # Parámetros de entrada: IP y puerto del EC_DE y el ID del taxi
    ip_ec_de = "127.0.0.1"  # IP del EC_DE
    puerto_ec_de = 8080  # Puerto del EC_DE
    taxi_id = "1"  # ID del taxi

    # Iniciar EC_S
    iniciar_ec_s(ip_ec_de, puerto_ec_de, taxi_id)
