from Central import Casilla

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
