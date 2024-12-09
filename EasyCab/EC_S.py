import socket
import time
import threading
import sys
from Clases import *

OK = True
id = 0

def sendOk(socket_server):
    global OK, id

    while True:  # Bucle infinito para intentar reconexiones
        try:
            # Crear y conectar el socket
            socket_server = socket.socket()
            socket_server.connect((sys.argv[1], int(sys.argv[2])))
            print("Conexión establecida con el servidor.")

            # Enviar mensaje de inicialización
            socket_server.send(create_message("SENSOR"))
            response = socket_server.recv(1024)

            # Recibir y verificar ID del servidor
            decoded_response = verify_message(response)
            if decoded_response:
                id = decoded_response
                print(f"ID asignado por el servidor: {id}")
            else:
                print("Error al recibir ID. Cerrando conexión e intentando nuevamente.")
                socket_server.close()
                time.sleep(5)
                continue  # Reintentar desde el inicio del bucle

            # Comenzar el envío periódico de mensajes OK/KO
            while True:
                try:
                    # Determinar el estado del sensor y enviar
                    status = "OK" if OK else "KO"
                    message = create_message(f"{id}#{status}")
                    socket_server.send(message)

                    # Verificar ACK/NACK del servidor
                    ack_response = socket_server.recv(1024)
                    if ack_response == NACK:
                        print("Error: El mensaje no fue recibido correctamente por el taxi.")

                    time.sleep(1)  # Esperar antes del próximo envío
                except Exception as e:
                    print(f"Desconexión detectada")
                    break  # Salir del bucle interno y volver a intentar la conexión
        except Exception as e:
            print(f"Error al intentar conectar con el servidor")
            time.sleep(5)  # Esperar antes de reintentar la conexión

def sendAlert():
    global OK
    #Cuando nos conectamos por primera vez, se nos asigna un ID y luego lo utilizamos para mandar el mensaje
    
    #Si presionamos cualquier tecla se envía un mensaje
    while True:
        input("Presiona cualquier tecla para parar el taxi: ")
        OK = False

        #Para volver a iniciar el coche presionamos otra tecla
        input("Presiona cualquier tecla para iniciar el taxi: ")
        OK = True

def main():
    #Comprobamos los argumentos
    if len(sys.argv) != 3:
        print("Error: Usage python EC_S.py EC_DE_IP EC_DE_Port")
        sys.exit(1)

    # Recoger los argumentos
    ip_ec_de = sys.argv[1]
    puerto_ec_de = int(sys.argv[2])

    #Creamos el socket de conexión y conectamos con el taxi
    server_socket = socket.socket()
    #Conectamos con el Digital Engine del taxi
    server_socket.connect((ip_ec_de, puerto_ec_de))

    #Creamos los hilos
    ok_thread = threading.Thread(target=sendOk, args=(server_socket,))
    ok_thread.start()

    messages_thread = threading.Thread(target=sendAlert)
    messages_thread.start()

    ok_thread.join()
    messages_thread.join()

# Ejecución principal
if __name__ == "__main__":
    main()
