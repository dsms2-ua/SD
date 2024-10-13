from colorama import init, Fore, Back, Style
import random

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

    # Método para generar el mapa del cliente, en el que solo aparece su taxi asignado y las posiciones
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

                # Comprobamos si hay una posición
                for pos in self.posiciones:
                    if self.posiciones[pos].getX() == j and self.posiciones[pos].getY() == i:
                        mapa_str += Back.BLUE + " " + Fore.BLACK + pos + " " + Style.RESET_ALL
                        isPos = True
                        break

                if not isPos:
                    for taxi in self.taxis:
                        if(taxi.getCliente() is not None):
                            if taxi.getX() == j and taxi.getY() == i and taxi.getCliente() == idCustomer:
                                isTaxi = True
                                # Cambiamos el color del fondo según el estado del taxi
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
                
                # Si no hay ningún elemento, añadimos un espacio con fondo blanco
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
            mapa_str += f"{Back.LIGHTBLACK_EX}{col:2} {Style.RESET_ALL} "  # Coloca el índice de columna (números) en la parte superior
        mapa_str += "\n"
        #mapa_str += "\n" + "   " + "---" * self.ancho + "\n"  # Separador horizontal debajo del índice de columnas

        for i in range(1, self.alto + 1):
            # Índice de fila en el lado izquierdo
            mapa_str += f"{Back.LIGHTBLACK_EX}{i:2}{Style.RESET_ALL}|"  # Índice de fila con separador vertical

            for j in range(1, self.ancho + 1):
                isPos = False
                isTaxi = False
                isCliente = False

                # Comprobamos si hay una posición
                for pos in self.posiciones:
                    if self.posiciones[pos].getX() == j and self.posiciones[pos].getY() == i:
                        mapa_str += Back.BLUE + " " + Fore.BLACK + pos + " " + Style.RESET_ALL
                        isPos = True
                        break
                if not isPos:
                    # Comprobamos si hay un taxi
                    for taxi in self.taxis:
                        if taxi.getX() == j and taxi.getY() == i:
                            isTaxi = True
                            if not taxi.getEstado():
                                mapa_str += Back.RED + " " + Fore.BLACK + str(taxi.getId()) + "!" + Style.RESET_ALL
                            elif not taxi.getOcupado():
                                mapa_str += Back.RED + " " + Fore.BLACK + str(taxi.getId()) + " " + Style.RESET_ALL
                            elif not taxi.getEstado():
                                mapa_str += Back.RED + " " + Fore.BLACK + str(taxi.getId()) + "!" + Style.RESET_ALL
                            elif taxi.getOcupado() and not taxi.getRecogido():
                                mapa_str += Back.GREEN + " " + Fore.BLACK + str(taxi.getId()) + " " + Style.RESET_ALL
                            elif taxi.getOcupado() and taxi.getRecogido():
                                mapa_str += Back.GREEN + " " + Fore.BLACK + str(taxi.getId()) + taxi.getCliente() + Style.RESET_ALL
                            break  

                # Comprobamos si hay un cliente, siempre que no haya un taxi
                if not isTaxi and not isPos:
                    for cliente in self.clientes:
                        if cliente.getPosicion().getX() == j and cliente.getPosicion().getY() == i:
                            mapa_str += Back.YELLOW + " " + Fore.BLACK + cliente.getId() + " " + Style.RESET_ALL
                            isCliente = True
                            break

                # Si no hay ningún elemento, añadimos un espacio con fondo blanco
                if not isPos and not isTaxi and not isCliente:
                    mapa_str += Back.WHITE + " . " + Style.RESET_ALL

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
        self.estado = True #True si está operativo, False si no
        self.ocupado = False #True si está ocupado, False si no
        self.cliente = None #ID del cliente que vamos a dar servicio
        self.origen = None 
        self.posCliente = None #Posición del cliente que debemos recoger
        self.destino = None #Localización a la que quiere ir el cliente
        self.posDestino = None
        self.recogido = False #Indica si hemos recogido al cliente o no

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
    
    
class Cliente():
    def __init__(self, id, locs, taxis, clientes):
        self.id = id
        self.posicion = generarAleatoria(locs, taxis, clientes)
        self.destino = None

    def setDestino(self, destino):
        self.destino = destino

    def setPosicion(self, posicion):
        self.posicion = posicion
    
    def getId(self):
        return self.id

    def getPosicion(self):
        return self.posicion
    
    def getDestino(self):
        return self.destino
        

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
    

def generarTabla(TAXIS, CLIENTES):
    strTabla =  "_______________________________________________________\n"
    strTabla += "|                    ***EASYCAB***                    |\n"
    strTabla += "|-----------------------------------------------------|\n"
    strTabla += "|                       TAXIS                         |\n"
    strTabla += "|-----------------------------------------------------|\n"
    strTabla += "|      ID     |        Destino      |      Estado     |\n"
    
    for taxi in TAXIS:
        #Si el esado del taxi es KO, todo el texto está en color rojo
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
            if not taxi.getEstado():
                strTabla += Fore.RED + taxi.getCliente() + Style.RESET_ALL
            else: 
                strTabla += taxi.getCliente()
        else:
            if not taxi.getEstado():
                strTabla += Fore.RED + taxi.getDestino() + Style.RESET_ALL
            else:
                strTabla += taxi.getDestino()
        strTabla += "         |"

        #Ahora imprimimos el estado
        if taxi.getEstado():
            if taxi.getOcupado():
                strTabla += "   OK.Servicio " + taxi.getCliente() + " |"
            else:
                strTabla += "    OK.Parado" + "    |"
        else:
            #Esto lo imprimimos en rojo
            strTabla += Fore.RED + "    KO. Parado" + Style.RESET_ALL + "   |"

        strTabla += "\n"
    
    strTabla += "|-----------------------------------------------------|\n"
    strTabla += "|                     CLIENTES                        |\n"
    strTabla += "|-----------------------------------------------------|\n"
    strTabla += "|      ID     |        Destino      |      Estado     |\n"
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
        
        strTabla += "\n"
    
    strTabla += "|_____________________________________________________|\n"

    return strTabla

def distanciaMasCorta(actual, objetivo):
    #Tenemos en cuenta que el mapa es esférico
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

    #Calculamos la distancia más corta en los dos ejes
    distX = distanciaMasCorta(actualX, objetivoX)
    distY = distanciaMasCorta(actualY, objetivoY)

    #Determinamos la dirección del movimiento en X
    if distX > 0:
        nuevoX = (actualX + 1) % 20
    elif distX < 0:
        nuevoX = (actualX - 1) % 20
    else:
        nuevoX = actualX

    #Hacemos lo mismo con el eje Y
    if distY > 0:
        nuevoY = (actualY + 1) % 20
    elif distY < 0:
        nuevoY = (actualY - 1) % 20
    else:
        nuevoY = actualY

    return Casilla(nuevoX, nuevoY)