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
                
                for taxi in self.taxis:
                    if(taxi.getCliente() is not None):
                        if taxi.getX() == j and taxi.getY() == i and taxi.getCliente() == idCustomer:
                            isTaxi = True
                            # Cambiamos el color del fondo según el estado del taxi
                            if taxi.getEstado() == False:
                                mapa_str += Back.RED + " " + Fore.BLACK + str(taxi.getId()) + "!" + Style.RESET_ALL
                            elif taxi.getDestino() is None:
                                mapa_str += Back.RED + " " + Fore.BLACK + str(taxi.getId()) + " " + Style.RESET_ALL
                            else:
                                mapa_str += Back.GREEN + " " + Fore.BLACK + str(taxi.getId()) + taxi.getDestino() + Style.RESET_ALL
                            break    

                for cliente in self.clientes:
                    print(cliente.getId() + " " + idCustomer)
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

                # Comprobamos si hay un taxi
                for taxi in self.taxis:
                    if taxi.getX() == j and taxi.getY() == i:
                        isTaxi = True
                        # Cambiamos el color del fondo según el estado del taxi
                        if taxi.getEstado() == False:
                            mapa_str += Back.RED + " " + Fore.BLACK + str(taxi.getId()) + "!" + Style.RESET_ALL
                        elif taxi.getDestino() is None:
                            mapa_str += Back.RED + " " + Fore.BLACK + str(taxi.getId()) + " " + Style.RESET_ALL
                        else:
                            mapa_str += Back.GREEN + " " + Fore.BLACK + str(taxi.getId()) + taxi.getDestino() + Style.RESET_ALL
                        break

                # Comprobamos si hay un cliente
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
    
class Taxi:
    def __init__(self, id):
        self.id = id
        self.casilla = Casilla()
        self.destino = None
        self.estado = True
        self.cliente = None

    def setCasilla(self, casilla):
        self.casilla = casilla

    def setEstado(self, estado):
        self.estado = estado

    def setDestino(self, destino):
        self.destino = destino

    def setCliente(self, cliente):
        self.cliente = cliente

    def getId(self):
        return self.id
    
    def getDestino(self):
        return self.destino

    def getX(self):
        return self.casilla.getX()
    
    def getY(self):
        return self.casilla.getY()
    
    def getEstado(self):
        return self.estado
    
    def getCliente(self):
        return self.cliente
    
class Cliente():
    def __init__(self, id, locs, taxis, clientes):
        self.id = id
        self.posicion = generarAleatoria(locs, taxis, clientes)
    
    def getId(self):
        return self.id

    def getPosicion(self):
        return self.posicion

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
        self.taxi = None

    def getCliente(self):
        return self.cliente

    def getOrigen(self):
        return self.origen

    def getDestino(self):
        return self.destino

    def getTaxi(self):
        return self.taxi

    def setTaxi(self,taxi):
        self.taxi = taxi

    def setOrigen(self,origen):
        self.origen = origen
    

