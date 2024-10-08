from colorama import init, Fore, Back, Style

class Mapa:
    def __init__(self):
        self.ancho = 20
        self.alto = 20
        self.map = [[' ' for i in range(self.ancho)] for j in range(self.alto)]
        #Inicializamos colorama para poder usar colores
        init(autoreset=True)

    def __str__(self):
        mapa_str = ""
        for i in range(self.alto):
            for j in range(self.ancho):
                print(self.map[i][j], end=' ')
            print()
        return mapa_str