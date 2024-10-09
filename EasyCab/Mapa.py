from colorama import init, Fore, Back, Style

class Mapa:
    def __init__(self):
        self.ancho = 20
        self.alto = 20
        self.map = [[' ' for i in range(self.ancho)] for j in range(self.alto)]
        #Inicializamos colorama para poder usar colores
        init(autoreset=True)

    def cadenaMapa(self, posiciones, taxis, clientes):
        mapa_str = ""
        for i in range(self.alto):
            for j in range(self.ancho):
                #Comprobamos si hay una posición
                for pos in posiciones:
                    if posiciones[pos].getX() == i and posiciones[pos].getY() == j:
                        mapa_str += Fore.BLUE + pos + Fore.RESET

                #Comprobamos si hay un taxi
                for taxi in taxis:
                    if taxi.getX() == i and taxi.getY() == j:
                        #Ahora comprobamos el estado y añadimos según el color
                        if taxi.getEstado() == False:
                            mapa_str += Fore.RED + taxi.getId() + "!" + Fore.RESET

                        elif taxi.getDestino() == None:
                            mapa_str += Fore.RED + taxi.getId() + Fore.RESET

                        elif taxi.getDestine != None:
                            mapa_str += Fore.GREEN + taxi.getId() + taxi.getDestino() + Fore.RESET

                #Por último comprobamos si hay un cliente
                for cliente in clientes:
                    if cliente.getX() == i and cliente.getY() == j:
                        mapa_str += Fore.YELLOW + cliente.getId() + Fore.RESET
                
            mapa_str += "\n"
        return mapa_str