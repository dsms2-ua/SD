from Central import Casilla

class Taxi:
    def __init__(self, id):
        self.id = id
        self.estado = "LIBRE"
        self.casilla = Casilla()
        self.destino = None

    def setCasilla(self, casilla):
        self.casilla = casilla

    def setEstado(self, estado):
        self.estado = estado

    def setDestino(self, destino):
        self.destino = destino
