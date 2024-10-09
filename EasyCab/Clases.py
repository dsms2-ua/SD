from colorama import init, Fore, Back, Style

class Mapa:
    def __init__(self):
        self.ancho = 20
        self.alto = 20
        self.map = [[' ' for i in range(self.ancho)] for j in range(self.alto)]
        #Inicializamos colorama para poder usar colores
        init(autoreset=True)

    from colorama import Fore, Back, Style

    def cadenaMapa(self, posiciones, taxis, clientes):
        mapa_str = ""

        # Índices de las columnas en la parte superior
        mapa_str += "   "  # Espacio para el índice de las filas
        for col in range(self.ancho):
            mapa_str += f"{col:2} "  # Coloca el índice de columna (números) en la parte superior
        mapa_str += "\n" + "   " + "---" * self.ancho + "\n"  # Separador horizontal debajo del índice de columnas

        for i in range(self.alto):
            # Índice de fila en el lado izquierdo
            mapa_str += f"{i:2} |"  # Índice de fila con separador vertical

            for j in range(self.ancho):
                isPos = False
                isTaxi = False
                isCliente = False

                # Comprobamos si hay una posición
                for pos in posiciones:
                    if posiciones[pos].getX() == i and posiciones[pos].getY() == j:
                        mapa_str += Back.WHITE + Fore.BLUE + pos + Style.RESET_ALL
                        isPos = True
                        break

                # Comprobamos si hay un taxi
                for taxi in taxis:
                    if taxi.getX() == i and taxi.getY() == j:
                        isTaxi = True
                        # Cambiamos el color del fondo según el estado del taxi
                        if taxi.getEstado() == False:
                            mapa_str += Back.WHITE + Fore.RED + taxi.getId() + "!" + Style.RESET_ALL
                        elif taxi.getDestino() is None:
                            mapa_str += Back.WHITE + Fore.RED + taxi.getId() + Style.RESET_ALL
                        else:
                            mapa_str += Back.WHITE + Fore.GREEN + taxi.getId() + taxi.getDestino() + Style.RESET_ALL
                        break

                # Comprobamos si hay un cliente
                for cliente in clientes:
                    if cliente.getX() == i and cliente.getY() == j:
                        mapa_str += Back.WHITE + Fore.BLACK + cliente.getId() + Style.RESET_ALL
                        isCliente = True
                        break

                # Si no hay ningún elemento, añadimos un espacio con fondo blanco
                if not isPos and not isTaxi and not isCliente:
                    mapa_str += Back.WHITE + "  " + Style.RESET_ALL

                # Añadir separador vertical entre las celdas
                mapa_str += "|"

            # Salto de línea al final de cada fila con separador horizontal
            mapa_str += "\n" + "   " + "---" * self.ancho + "\n"

        return mapa_str


class Casilla:
    def __init__(self, x=1, y=1):
        self.x = 1
        self.y = 1

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

    def setCasilla(self, casilla):
        self.casilla = casilla

    def setEstado(self, estado):
        self.estado = estado

    def setDestino(self, destino):
        self.destino = destino

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
    
class Cliente():
    def __init__(self, id):
        self.id = id
        self.posicion = None
    
    def getId(self):
        return self.id