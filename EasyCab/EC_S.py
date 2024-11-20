import socket
import time
import threading
import sys
from Clases import *

OK = True
id = 0

def sendOk(socket_server):
    global OK, id
    socket_server.send(create_message("SENSOR"))  # Identificación inicial
    response = socket_server.recv(1024)

    # Recibe ID del servidor
    decoded_response = verify_message(response)
    if decoded_response:
        id = decoded_response
    else:
        print("Error: No se pudo asignar el ID")
        return

    # Envía OK/KO cada segundo
    while True:
        try:
            status = "OK" if OK else "KO"
            message = create_message(f"{id}#{status}")
            socket_server.send(message)
            
            # Verifica ACK/NACK
            ack_response = socket_server.recv(1024)
            if ack_response == NACK:
                print("Error: Mensaje no fue recibido correctamente")
            
            time.sleep(1)
        except Exception as e:
            print(f"IMPOSIBLE CONECTARSE CON EL TAXI {id}, intentando reconexión...")
            # Intenta reconectarse cada 5 segundos hasta que lo logre
            while True:
                try:
                    time.sleep(5)
                    socket_server = socket.socket()
                    socket_server.connect((sys.argv[1], int(sys.argv[2])))
                    print("Reconexión exitosa.")
                    input("Presiona cualquier tecla para parar el taxi: ")
                    break  # Sale del bucle de reconexión al lograr conectarse
                except Exception as recon_error:
                    print("Error en la reconexión, intentando de nuevo...")

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
