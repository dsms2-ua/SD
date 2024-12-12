import pickle
from colorama import init, Fore, Back, Style
import random
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives import padding
from cryptography.hazmat.backends import default_backend
import os
import base64
from colorama import Fore, Back, Style

#Clase que contendrá el control de temperatura
class CTC:
    def __init__(self, ciudad, temperatura, estado):
        self.ciudad = ciudad
        self.temperatura = temperatura
        self.estado = estado
        
    def setCiudad(self, ciudad):
        self.ciudad = ciudad
        
    def setTemperatura(self, temperatura):
        self.temperatura = temperatura
    
    def setEstado(self, estado):
        self.estado = estado
        
    def getCiudad(self):
        return self.ciudad
    
    def getTemperatura(self):
        return self.temperatura
    
    def getEstado(self):
        return self.estado
    
    def cadenaCTC(self):  
        if self.ciudad != "Error":
            cadena =  f"    \nCTC => Ciudad: {self.ciudad} - Temperatura: {self.temperatura} - Estado: {self.estado} \n"
        else:
            cadena = f"   \nLa conexión con el módulo CTC no ha podido establecerse. Se volverá a intentar dentro de 10 segundos \n"
            
        return cadena

class Mapa:
    def __init__(self,posiciones,taxis,clientes):
        self.ancho = 20
        self.alto = 20
        self.map = [[' ' for i in range(self.ancho)] for j in range(self.alto)]
        self.posiciones = posiciones
        self.taxis = taxis
        self.clientes = clientes
        #Inicializamos colorama para poder usar colores
        init(autoreset=True)

    from colorama import Fore, Back, Style

    # Metodo para generar el mapa del taxista
    def cadenaMapaTaxi(self,idTaxi):
        mapa_str = ""
        print("ID TAXI: ",idTaxi)
        # Índices de las columnas en la parte superior
        mapa_str += "   "  # Espacio para el índice de las filas    
        for col in range(1, self.ancho + 1):
            mapa_str += f"{Back.LIGHTBLACK_EX}{col:2} {Style.RESET_ALL} "
        mapa_str += "\n"

        for i in range(1, self.alto + 1):
            # Índice de fila en el lado izquierdo
            mapa_str += f"{Back.LIGHTBLACK_EX}{i:2}{Style.RESET_ALL}|"

            for j in range(1, self.ancho + 1):
                isPos = False
                isTaxi = False
                isCliente = False

                # Comprobamos si hay una posicion
                for pos in self.posiciones:
                    if self.posiciones[pos].getX() == j and self.posiciones[pos].getY() == i:
                        mapa_str += Back.BLUE + " " + Fore.BLACK + pos + " " + Style.RESET_ALL
                        isPos = True
                        break

                if not isPos:
                    for taxi in self.taxis:
                        if taxi.getX() == j and taxi.getY() == i and str(taxi.getId()) == idTaxi and taxi.getVisible():
                            isTaxi = True
                            # Cambiamos el color del fondo segun el estado del taxi
                            if not taxi.getEstado():
                                mapa_str += Back.RED + " " + Fore.BLACK + str(taxi.getId()) + "!" + Style.RESET_ALL
                            elif not taxi.getOcupado():
                                mapa_str += Back.RED + " " + Fore.BLACK + str(taxi.getId()) + " " + Style.RESET_ALL
                            elif taxi.getOcupado() and not taxi.getRecogido() and taxi.getCliente() is not None:
                                mapa_str += Back.GREEN + " " + Fore.BLACK + str(taxi.getId()) + " " + Style.RESET_ALL
                            elif taxi.getOcupado() and taxi.getRecogido() and taxi.getCliente() is not None:
                                mapa_str += Back.GREEN + " " + Fore.BLACK + str(taxi.getId()) + taxi.getCliente() + Style.RESET_ALL
                            else:
                                mapa_str += Back.RED + " " + Fore.BLACK + str(taxi.getId()) + " " + Style.RESET_ALL
                            break  
                          
                if not isPos and not isTaxi:
                    for cliente in self.clientes:
                        for taxi in self.taxis:
                            if str(taxi.getId())==idTaxi and taxi.getCliente() is not None:
                                if cliente.getPosicion().getX() == j and cliente.getPosicion().getY() == i and cliente.getId() == taxi.getCliente():
                                    mapa_str += Back.YELLOW + " " + Fore.BLACK + cliente.getId() + " " + Style.RESET_ALL
                                    isCliente = True
                                    break
                
                # Si no hay ningun elemento, añadimos un espacio con fondo blanco
                if not isPos and not isTaxi and not isCliente:
                    mapa_str += Back.WHITE + " . " + Style.RESET_ALL

                # Añadir separador vertical entre las celdas
                mapa_str += "|"

            # Salto de línea al final de cada fila con separador horizontal
            mapa_str += "\n"

        return mapa_str

    # Metodo para generar el mapa del cliente, en el que solo aparece su taxi asignado y las posiciones
    def cadenaMapaCustomer(self,idCustomer):
        mapa_str = ""

        # Índices de las columnas en la parte superior
        mapa_str += "   "  # Espacio para el índice de las filas    
        for col in range(1, self.ancho + 1):
            mapa_str += f"{Back.LIGHTBLACK_EX}{col:2} {Style.RESET_ALL} "
        mapa_str += "\n"

        for i in range(1, self.alto + 1):
            # Índice de fila en el lado izquierdo
            mapa_str += f"{Back.LIGHTBLACK_EX}{i:2}{Style.RESET_ALL}|"

            for j in range(1, self.ancho + 1):
                isPos = False
                isTaxi = False
                isCliente = False

                # Comprobamos si hay una posicion
                for pos in self.posiciones:
                    if self.posiciones[pos].getX() == j and self.posiciones[pos].getY() == i:
                        mapa_str += Back.BLUE + " " + Fore.BLACK + pos + " " + Style.RESET_ALL
                        isPos = True
                        break

                if not isPos:
                    for taxi in self.taxis:
                        if(taxi.getCliente() is not None):
                            if taxi.getX() == j and taxi.getY() == i and taxi.getCliente() == idCustomer and taxi.getVisible():
                                isTaxi = True
                                # Cambiamos el color del fondo segun el estado del taxi
                                if not taxi.getEstado():
                                    mapa_str += Back.RED + " " + Fore.BLACK + str(taxi.getId()) + "!" + Style.RESET_ALL
                                elif not taxi.getOcupado():
                                    mapa_str += Back.RED + " " + Fore.BLACK + str(taxi.getId()) + " " + Style.RESET_ALL
                                elif taxi.getOcupado() and not taxi.getRecogido():
                                    mapa_str += Back.GREEN + " " + Fore.BLACK + str(taxi.getId()) + " " + Style.RESET_ALL
                                elif taxi.getOcupado() and taxi.getRecogido():
                                    mapa_str += Back.GREEN + " " + Fore.BLACK + str(taxi.getId()) + taxi.getCliente() + Style.RESET_ALL
                                break  
                             
                if not isPos and not isTaxi:
                    for cliente in self.clientes:
                        if cliente.getPosicion().getX() == j and cliente.getPosicion().getY() == i and str(cliente.getId()) == idCustomer:
                            mapa_str += Back.YELLOW + " " + Fore.BLACK + cliente.getId() + " " + Style.RESET_ALL
                            isCliente = True
                            break
                
                # Si no hay ningun elemento, añadimos un espacio con fondo blanco
                if not isPos and not isTaxi and not isCliente:
                    mapa_str += Back.WHITE + " . " + Style.RESET_ALL

                # Añadir separador vertical entre las celdas
                mapa_str += "|"

            # Salto de línea al final de cada fila con separador horizontal
            mapa_str += "\n"

        return mapa_str


    def cadenaMapa(self):
        mapa_str = ""

        # Índices de las columnas en la parte superior
        mapa_str += "   "  # Espacio para el índice de las filas
        for col in range(1, self.ancho + 1):
            mapa_str += f"{Back.LIGHTBLACK_EX}{col:2} {Style.RESET_ALL} "  # Coloca el índice de columna (numeros) en la parte superior
        mapa_str += "\n"
        #mapa_str += "\n" + "   " + "---" * self.ancho + "\n"  # Separador horizontal debajo del índice de columnas

        for i in range(1, self.alto + 1):
            # Índice de fila en el lado izquierdo
            mapa_str += f"{Back.LIGHTBLACK_EX}{i:2}{Style.RESET_ALL}|"  # Índice de fila con separador vertical

            for j in range(1, self.ancho + 1):
                isPos = False
                isTaxi = False
                isCliente = False

                # Comprobamos si hay una posicion
                for pos in self.posiciones:
                    if self.posiciones[pos].getX() == j and self.posiciones[pos].getY() == i:
                        mapa_str += Back.BLUE + " " + Fore.BLACK + pos + " " + Style.RESET_ALL
                        isPos = True
                        break
                if not isPos:
                    # Comprobamos si hay un taxi
                    for taxi in self.taxis:
                        if taxi.getX() == j and taxi.getY() == i and taxi.getVisible():
                            isTaxi = True
                            if not taxi.getEstado():
                                mapa_str += Back.RED + " " + Fore.BLACK + str(taxi.getId()) + "!" + Style.RESET_ALL
                            elif not taxi.getOcupado():
                                mapa_str += Back.RED + " " + Fore.BLACK + str(taxi.getId()) + " " + Style.RESET_ALL
                            elif not taxi.getEstado():
                                mapa_str += Back.RED + " " + Fore.BLACK + str(taxi.getId()) + "!" + Style.RESET_ALL
                            elif taxi.getOcupado() and not taxi.getRecogido() and taxi.getCliente() is not None:
                                mapa_str += Back.GREEN + " " + Fore.BLACK + str(taxi.getId()) + " " + Style.RESET_ALL
                            elif taxi.getOcupado() and taxi.getRecogido() and taxi.getCliente() is not None:
                                mapa_str += Back.GREEN + " " + Fore.BLACK + str(taxi.getId()) + taxi.getCliente() + Style.RESET_ALL
                            else:
                                mapa_str += Back.RED + " " + Fore.BLACK + str(taxi.getId()) + " " + Style.RESET_ALL
                            break  

                # Comprobamos si hay un cliente, siempre que no haya un taxi
                if not isTaxi and not isPos:
                    for cliente in self.clientes:
                        if cliente.getPosicion().getX() == j and cliente.getPosicion().getY() == i and cliente.getVisible():
                            mapa_str += Back.YELLOW + " " + Fore.BLACK + cliente.getId() + " " + Style.RESET_ALL
                            isCliente = True
                            break

                # Si no hay ningun elemento, añadimos un espacio con fondo blanco
                if not isPos and not isTaxi and not isCliente:
                    mapa_str += Back.WHITE + " . " + Style.RESET_ALL

                # Añadir separador vertical entre las celdas
                mapa_str += "|"

            # Salto de línea al final de cada fila con separador horizontal
            mapa_str += "\n"

        return mapa_str
    
    def cadenaMapaArchivo(self):
        mapa_str = ""

        # Índices de las columnas en la parte superior
        mapa_str += "___"  # Espacio para el índice de las filas
        for col in range(1, self.ancho + 1):    
            mapa_str += f"{col:2}  "  # Coloca el índice de columna (numeros) en la parte superior
        mapa_str += "\n"
        #mapa_str += "\n" + "   " + "---" * self.ancho + "\n"  # Separador horizontal debajo del índice de columnas

        for i in range(1, self.alto + 1):
            # Índice de fila en el lado izquierdo
            if i < 10:
                mapa_str += f"0{i}|"
            else:
                mapa_str += f"{i:2}|"  # Índice de fila con separador vertical

            for j in range(1, self.ancho + 1):
                isPos = False
                isTaxi = False
                isCliente = False

                # Comprobamos si hay una posicion
                for pos in self.posiciones:
                    if self.posiciones[pos].getX() == j and self.posiciones[pos].getY() == i:
                        mapa_str += " " + pos + " "
                        isPos = True
                        break
                if not isPos:
                    # Comprobamos si hay un taxi
                    for taxi in self.taxis:
                        if taxi.getX() == j and taxi.getY() == i and taxi.getVisible():
                            isTaxi = True
                            if not taxi.getEstado():
                                mapa_str += " " + str(taxi.getId()) + "!"
                            elif not taxi.getOcupado():
                                mapa_str += " " + str(taxi.getId()) + " "
                            elif not taxi.getEstado():
                                mapa_str += " " + str(taxi.getId()) + "!"
                            elif taxi.getOcupado() and not taxi.getRecogido() and taxi.getCliente() is not None:
                                mapa_str += " " + str(taxi.getId()) + " "
                            elif taxi.getOcupado() and taxi.getRecogido() and taxi.getCliente() is not None:
                                mapa_str += " " + str(taxi.getId()) + taxi.getCliente()
                            else:
                                mapa_str += " " + str(taxi.getId()) + " "
                            break  

                # Comprobamos si hay un cliente, siempre que no haya un taxi
                if not isTaxi and not isPos:
                    for cliente in self.clientes:
                        if cliente.getPosicion().getX() == j and cliente.getPosicion().getY() == i and cliente.getVisible():
                            mapa_str += " " + cliente.getId() + " "
                            isCliente = True
                            break

                # Si no hay ningun elemento, añadimos un espacio con fondo blanco
                if not isPos and not isTaxi and not isCliente:
                    mapa_str += " . "

                # Añadir separador vertical entre las celdas
                mapa_str += "|"

            # Salto de línea al final de cada fila con separador horizontal
            mapa_str += "\n"

        return mapa_str


class Casilla:
    def __init__(self, x=1, y=1):
        self.x = x
        self.y = y

    def __str__(self):
        return f"({self.x}, {self.y})"
    
    def getX(self):
        return self.x
    
    def getY(self):
        return self.y
    
    def __eq__(self, other):
        if isinstance(other, Casilla):
            return self.getX() == other.getX() and self.getY() == other.getY()
        return False
    
class Taxi:
    def __init__(self, id):
        self.id = id
        self.casilla = Casilla()
        self.estado = True #True si esta operativo, False si no
        self.ocupado = False #True si esta ocupado, False si no
        self.cliente = None #ID del cliente que vamos a dar servicio
        self.origen = None 
        self.posCliente = None #Posicion del cliente que debemos recoger
        self.destino = None #Localizacion a la que quiere ir el cliente
        self.posDestino = None
        self.recogido = False #Indica si hemos recogido al cliente o no
        self.timeout = 0
        self.visible = True
        self.token = None

    def setVisible(self, visible):
        self.visible = visible
    
    def setTimeout(self, timeout):
        self.timeout = timeout

    def setCasilla(self, casilla):
        self.casilla = casilla

    def setEstado(self, estado):
        self.estado = estado

    def setOcupado(self, ocupado):
        self.ocupado = ocupado

    def setCliente(self, cliente):
        self.cliente = cliente

    def setOrigen(self, origen):
        self.origen = origen

    def setPosCliente(self, posCliente):
        self.posCliente = posCliente

    def setDestino(self, destino):
        self.destino = destino

    def setPosDestino(self, posDestino):
        self.posDestino = posDestino

    def setRecogido(self, recogido):
        self.recogido = recogido
        
    def setToken(self, token):
        self.token = token

    def getVisible(self):
        return self.visible

    def getId(self):
        return self.id
    
    def getCasilla(self):
        return self.casilla
    
    def getX(self):
        return self.casilla.getX()
    
    def getY(self):
        return self.casilla.getY()
    
    def getEstado(self):
        return self.estado
    
    def getOcupado(self):
        return self.ocupado
    
    def getCliente(self):
        return self.cliente
    
    def getOrigen(self):
        return self.origen
    
    def getPosCliente(self):
        return self.posCliente
    
    def getDestino(self):
        return self.destino

    def getPosDestino(self):
        return self.posDestino 
    
    def getRecogido(self):
        return self.recogido
    
    def getTimeout(self):
        return self.timeout
    
    def getToken(self):
        return self.token
    
    
class Cliente():
    def __init__(self, id, locs, taxis, clientes):
        self.id = id
        self.posicion = generarAleatoria(locs, taxis, clientes)
        self.destino = None
        self.timeout = 0
        self.visible = True

    def setDestino(self, destino):
        self.destino = destino

    def setPosicion(self, posicion):
        self.posicion = posicion
    
    def setTimeout(self, timeout):
        self.timeout = timeout
        
    def setVisible(self, visible):
        self.visible = visible
    
    def getId(self):
        return self.id

    def getPosicion(self):
        return self.posicion
    
    def getDestino(self):
        return self.destino
    
    def getTimeout(self):
        return self.timeout
    
    def getVisible(self):
        return self.visible
        

def generarAleatoria(locs, taxis, clientes):
    valida = False
    while not valida:

        x = random.randint(1, 20)
        y = random.randint(1, 20)

        valida = True

        #Que no coincida con locs
        for loc in locs:
            if locs[loc].getX() == x and locs[loc].getY() == y:
                valida = False

        for taxi in taxis:
            if taxi.getX() == x and taxi.getY() == y:
                valida = False

        for cliente in clientes:
            if cliente.getPosicion().getX() == x and cliente.getPosicion().getY() == y:
                valida = False

    return Casilla(x, y)

        
class Servicio:
    def __init__(self,cliente,destino):
        self.cliente = cliente
        self.origen = None
        self.destino = destino
        self.posDestino = None
        self.posCliente = None
        self.taxi = None

    def getCliente(self):
        return self.cliente

    def getOrigen(self):
        return self.origen

    def getDestino(self):
        return self.destino
    
    def getPosDestino(self):
        return self.posDestino
    
    def getPosCliente(self):
        return self.posCliente

    def getTaxi(self):
        return self.taxi
    
    def setCliente(self,cliente):
        self.cliente = cliente

    def setTaxi(self,taxi):
        self.taxi = taxi

    def setPosDestino(self, posDestino):
        self.posDestino = posDestino

    def setOrigen(self,origen):
        self.origen = origen

    def setPosCliente(self,posCliente):
        self.posCliente = posCliente
    

def generarTabla(TAXIS, CLIENTES, LOCALIZACIONES, CTC):
    strTabla =  "___________________________________________________________________________\n"
    strTabla += "|                             ***EASYCAB***                               |\n"
    strTabla += "|-------------------------------------------------------------------------|\n"
    strTabla += "|                                 TAXIS                                   |\n"
    strTabla += "|-------------------------------------------------------------------------|\n"
    strTabla += "|      ID     |        Destino      |      Estado     |      Posicion     |\n"
    
    for taxi in TAXIS:
        #Si el esado del taxi es KO, todo el texto esta en color rojo
        #Primero imprimimos el ID
        strTabla +=  "|      "
        if not taxi.getEstado():
            strTabla += Fore.RED + str(taxi.getId()) + Style.RESET_ALL
        else:
            strTabla += str(taxi.getId())
        strTabla += "      "

        #Ahora imprimimos el destino
        strTabla += "|           "
        if not taxi.getOcupado():
            if not taxi.getEstado():
                strTabla += Fore.RED + "-" + Style.RESET_ALL
            else:
                strTabla += "-"
        elif not taxi.getRecogido():
            if not taxi.getEstado() and taxi.getCliente() is not None:
                strTabla += Fore.RED + taxi.getCliente() + Style.RESET_ALL
            elif taxi.getCliente() is not None: 
                strTabla += taxi.getCliente()
            else:
                strTabla += "-"
        else:
            if not taxi.getEstado():
                strTabla += Fore.RED + taxi.getDestino() + Style.RESET_ALL
            else:
                strTabla += taxi.getDestino()
        strTabla += "         |"

        #Ahora imprimimos el estado
        if taxi.getEstado():
            if taxi.getOcupado() and taxi.getCliente() is not None:
                strTabla += "   OK.Servicio " + taxi.getCliente() + " |"
            else:
                strTabla += "    OK.Parado" + "    |"
        elif taxi.getVisible():
            #Esto lo imprimimos en rojo
            strTabla += Fore.RED + "    KO. Parado" + Style.RESET_ALL + "   |"
        else:
            strTabla += Fore.RED + " KO. Desconectado" + Style.RESET_ALL + "|"
        #Ahora imprimimos la posicion
        pos = taxi.getCasilla()
        aux = False
        for loc in LOCALIZACIONES:
            if LOCALIZACIONES[loc] == pos:
                aux = True
                strTabla += "         " + loc + "     "
                break  
        if not aux:
            strTabla += "       " + str(taxi.getCasilla())
        
            if taxi.getCasilla().getX() < 10 and taxi.getCasilla().getY() < 10:
                strTabla += "  "
            elif taxi.getCasilla().getX() < 10 or taxi.getCasilla().getY() < 10:
                strTabla += " "
              
        strTabla += "    |"

        strTabla += "\n"
    
    strTabla += "|-------------------------------------------------------------------------|\n"
    strTabla += "|                                CLIENTES                                 |\n"
    strTabla += "|-------------------------------------------------------------------------|\n"
    strTabla += "|      ID     |        Destino      |      Estado     |      Posicion     |\n"
    for cliente in CLIENTES:        
        strTabla += "|      " + cliente.getId() + "      |" + "           "

        if cliente.getDestino() is None:
            strTabla += "-"
        else:
            strTabla += cliente.getDestino() 
        strTabla += "         |"

        #Buscamos si el cliente tiene un taxi asignado
        asignado = False
        id = None
        for taxi in TAXIS:
            if taxi.getCliente() == cliente.getId():
                id = taxi.getId()
                asignado = True
                break
        
        if cliente.getTimeout() == 5:
            strTabla += Fore. RED + "   Sin conexión" + Style.RESET_ALL + "  |" 
        elif asignado:
            strTabla += "    OK.Taxi " + str(id) + "    |"
        else:
            strTabla += "   OK. Sin Taxi  |"
            
        #Aquí ajustamos el formato
        
        #Primero imprimimos la posicion

        pos = cliente.getPosicion()
        aux = False
        for loc in LOCALIZACIONES:
            if LOCALIZACIONES[loc] == pos:
                aux = True
                strTabla += "         " + loc + "     "
                break
        
        if not aux:
            strTabla += "       " + str(cliente.getPosicion())
            
            if cliente.getPosicion().getX() < 10 and cliente.getPosicion().getY() < 10:
                strTabla += "  "
            elif cliente.getPosicion().getX() < 10 or cliente.getPosicion().getY() < 10:
                strTabla += " "
              
        strTabla += "    |"
        
        strTabla += "\n"
    
    strTabla += "|_________________________________________________________________________|\n"
    
    strTabla += CTC.cadenaCTC()

    return strTabla

def generarTablaArchivo(TAXIS, CLIENTES, LOCALIZACIONES):
    strTabla =  "___________________________________________________________________________\n"
    strTabla += "|                             ***EASYCAB***                               |\n"
    strTabla += "|-------------------------------------------------------------------------|\n"
    strTabla += "|                                 TAXIS                                   |\n"
    strTabla += "|-------------------------------------------------------------------------|\n"
    strTabla += "|      ID     |        Destino      |      Estado     |      Posicion     |\n"
    
    for taxi in TAXIS:
        #Si el esado del taxi es KO, todo el texto esta en color rojo
        #Primero imprimimos el ID
        strTabla +=  "|      "
        if not taxi.getEstado():
            strTabla += str(taxi.getId())
        else:
            strTabla += str(taxi.getId())
        strTabla += "      "

        #Ahora imprimimos el destino
        strTabla += "|           "
        if not taxi.getOcupado():
            if not taxi.getEstado():
                strTabla += "-"
            else:
                strTabla += "-"
        elif not taxi.getRecogido():
            if not taxi.getEstado() and taxi.getCliente() is not None:
                strTabla += taxi.getCliente()
            elif taxi.getCliente() is not None: 
                strTabla += taxi.getCliente()
        else:
            if not taxi.getEstado():
                strTabla += taxi.getDestino()
            else:
                strTabla += taxi.getDestino()
        strTabla += "         |"

        #Ahora imprimimos el estado
        if taxi.getEstado():
            if taxi.getOcupado() and taxi.getCliente() is not None:
                strTabla += "   OK.Servicio " + taxi.getCliente() + " |"
            else:
                strTabla += "    OK.Parado" + "    |"
        elif taxi.getVisible():
            #Esto lo imprimimos en rojo
            strTabla += Fore.RED + "    KO. Parado" + "   |"
        else:
            strTabla += Fore.RED + "KO. Desconectado" + " |"
        #Ahora imprimimos la posicion
        pos = taxi.getCasilla()
        aux = False
        for loc in LOCALIZACIONES:
            if LOCALIZACIONES[loc] == pos:
                aux = True
                strTabla += "         " + loc + "     "
                break  
        if not aux:
            strTabla += "       " + str(taxi.getCasilla())
        
            if taxi.getCasilla().getX() < 10 and taxi.getCasilla().getY() < 10:
                strTabla += "  "
            elif taxi.getCasilla().getX() < 10 or taxi.getCasilla().getY() < 10:
                strTabla += " "
              
        strTabla += "    |"

        strTabla += "\n"
    
    strTabla += "|-------------------------------------------------------------------------|\n"
    strTabla += "|                                CLIENTES                                 |\n"
    strTabla += "|-------------------------------------------------------------------------|\n"
    strTabla += "|      ID     |        Destino      |      Estado     |      Posicion     |\n"
    for cliente in CLIENTES:
        strTabla += "|      " + cliente.getId() + "      |" + "           "

        if cliente.getDestino() is None:
            strTabla += "-"
        else:
            strTabla += cliente.getDestino() 
        strTabla += "         |"

        #Buscamos si el cliente tiene un taxi asignado
        asignado = False
        id = None
        for taxi in TAXIS:
            if taxi.getCliente() == cliente.getId():
                id = taxi.getId()
                asignado = True
                break
        
        if asignado:
            strTabla += "    OK.Taxi " + str(id) + "    |"
        else:
            strTabla += "   OK. Sin Taxi  |"
            
        #Aquí ajustamos el formato
        
        #Primero imprimimos la posicion

        pos = cliente.getPosicion()
        aux = False
        for loc in LOCALIZACIONES:
            if LOCALIZACIONES[loc] == pos:
                aux = True
                strTabla += "         " + loc + "     "
                break
        
        if not aux:
            strTabla += "       " + str(cliente.getPosicion())
            
            if cliente.getPosicion().getX() < 10 and cliente.getPosicion().getY() < 10:
                strTabla += "  "
            elif cliente.getPosicion().getX() < 10 or cliente.getPosicion().getY() < 10:
                strTabla += " "
              
        strTabla += "    |"
        
        strTabla += "\n"
    
    strTabla += "|_________________________________________________________________________|\n"

    return strTabla

def distanciaMasCorta(actual, objetivo):
    #Tenemos en cuenta que el mapa es esferico
    tam = 20
    dist = (objetivo - actual) % tam

    if dist > tam / 2:
        dist = dist - tam
    return dist

def moverTaxi(actual, objetivo):
    actualX = actual.getX()
    actualY = actual.getY()

    objetivoX = objetivo.getX()
    objetivoY = objetivo.getY()

    #Calculamos la distancia mas corta en los dos ejes
    distX = distanciaMasCorta(actualX, objetivoX)
    distY = distanciaMasCorta(actualY, objetivoY)

    #Determinamos la direccion del movimiento en X
    if distX > 0:
        nuevoX = actualX + 1
        if nuevoX > 20:
            nuevoX = 1
    elif distX < 0:
        nuevoX = actualX - 1
        if nuevoX < 1:
            nuevoX = 20
    else:
        nuevoX = actualX

    # Hacemos lo mismo con el eje Y
    if distY > 0:
        nuevoY = actualY + 1
        if nuevoY > 20:
            nuevoY = 1
    elif distY < 0:
        nuevoY = actualY - 1
        if nuevoY < 1:
            nuevoY = 20
    else:
        nuevoY = actualY

    return Casilla(nuevoX, nuevoY)

def imprimirErrorCentral():
    #Imprimimos un mensaje en grande donde informamos del error de la central
    print("##########################################################")
    print("##                                                      ##")
    print("##                                                      ##")
    print("##                                                      ##")
    print("##           Error al conectar con la central           ##")
    print("##                                                      ##")
    print("##                                                      ##")
    print("##                                                      ##")
    print("##########################################################")

# Constantes para el protocolo
STX = b'\x02'
ETX = b'\x03'
ACK = b'\x06'
NACK = b'\x15'


# Funcion para calcular el LRC de un mensaje
def calculate_lrc(data: bytes) -> bytes:
    lrc = 0
    for byte in data:
        lrc ^= byte
    return bytes([lrc])

# Empaqueta el mensaje en el formato <STX><DATA><ETX><LRC>
def create_message(data: str) -> bytes:
    data_bytes = data.encode('utf-8')
    lrc = calculate_lrc(data_bytes)
    return STX + data_bytes + ETX + lrc

# Verifica el mensaje recibido usando <LRC> y retorna el contenido si es valido
def verify_message(message: bytes) -> str:
    if message[0] != STX[0] or message[-2] != ETX[0]:
        return None  # Mensaje mal empaquetado

    data = message[1:-2]
    lrc_received = message[-1]
    lrc_calculated = calculate_lrc(data)[0]

    if lrc_received == lrc_calculated:
        return data.decode('utf-8')
    else:
        return None  # LRC incorrecto
    
"""Metodos AES"""
# Función para generar una clave AES de 256 bits
def generate_aes_key():
    return os.urandom(32)

# Función para cifrar un mensaje con AES
def encrypt(message: str, key: bytes,isString: bool) -> str:
    # Generamos un IV aleatorio de 16 bytes
    iv = os.urandom(16)
    
    # Creamos el cifrador AES en modo CBC
    cipher = Cipher(algorithms.AES(key), modes.CBC(iv), backend=default_backend())
    encryptor = cipher.encryptor()
    if isString:
        message = message.encode()
    # Aseguramos que el mensaje tenga un tamaño múltiplo de 16 (bloque AES)
    padder = padding.PKCS7(128).padder()  # PKCS7 padding para AES
    padded_message = padder.update(message) + padder.finalize()
    
    # Ciframos el mensaje
    encrypted_message = encryptor.update(padded_message) + encryptor.finalize()
    
    # Devuelve el IV y el mensaje cifrado juntos, codificados en base64
    return base64.b64encode(iv + encrypted_message).decode('utf-8')
                            
# Función para descifrar un mensaje con AES
def decrypt(encrypted_message: str, key: bytes, isString: bool) -> str:
    # Decodificamos el mensaje cifrado de base64
    
    encrypted_data = base64.b64decode(encrypted_message)
    
    # El IV está al principio del mensaje cifrado, y el mensaje cifrado sigue después
    iv = encrypted_data[:16]
    encrypted_message = encrypted_data[16:]
    
    # Creamos el descifrador AES en modo CBC
    cipher = Cipher(algorithms.AES(key), modes.CBC(iv), backend=default_backend())
    decryptor = cipher.decryptor()
    
    # Desciframos el mensaje
    decrypted_message = decryptor.update(encrypted_message) + decryptor.finalize()
    
    # Deshacemos el padding
    unpadder = padding.PKCS7(128).unpadder()
    original_message = unpadder.update(decrypted_message) + unpadder.finalize()
    if isString:
        return original_message.decode('utf-8')
    return pickle.loads(original_message)

def extract_taxi_id_and_message(received_message):
    parts = received_message.split(' ', 1)
    taxi_id = int(parts[0])  # El taxi_id es la primera parte
    encrypted_message = bytes.fromhex(parts[1])  # Convertimos de hexadecimal a bytes
    return taxi_id, encrypted_message