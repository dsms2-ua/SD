import socket
import time
import threading
import sys

OK = True

def sendOk(socket_server):
    global OK
    #Cada segundo mandamos un OK al Digital Engine
    while True:
        if OK:
            socket_server.send("OK".encode('utf-8'))
        else:
            socket_server.send("KO".encode('utf-8'))
        time.sleep(1)

def sendAlert():
    global OK
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
